"""
ECB intressimäärad ja sektorite aktsiatootlus — Airflow pipeline

Andmevoog:
    dbt_seed >> [laadi_ecb_maarad, laadi_indeksite_hinnad] >> dbt_run >> dbt_test

Miks see järjekord?
    1. dbt_seed käivitub kõigepealt — loob/uuendab marts.dim_indeksid tabeli,
       millest laadi_indeksite_hinnad loeb aktiivsed tickerid dünaamiliselt.
    2. Mõlemad andmete tõmbamise taskid töötavad paralleelselt.
    3. dbt_run transformeerib toorandmed staging → intermediate → marts kihtidesse.
    4. dbt_test kontrollib andmekvaliteedi reegleid.

Idempotentsus:
    — Iga käivitus saab unikaalse run_id (UUID).
    — Staging tabelid on append-only: sama kuupäev võib esineda mitme run_id-ga.
    — Deduplitseerimine toimub dbt mudelites (võetakse MAX(run_id) per kirje).
    — Kui DAG ebaõnnestub ja käivitatakse uuesti, ei teki duplikaate mart tasandil.

Inkrementaalsus (yfinance):
    — Esimesel käivitusel tõmmatakse YFINANCE_INITIAL_DAYS päeva ajalugu (5 aastat).
    — Järgmistel käivitustel tõmmatakse alates viimasest olemasolevast kuupäevast
      miinus YFINANCE_OVERLAP_DAYS (kattuvus, et äärmuslikud juhtumid ei kaoks).

ECB API:
    — Tõmmatakse alati kogu ajalugu (andmemaht on väike, ~30-40 rida).
    — ECB SDMX REST API, avalik, ei nõua autentimist.
"""

import ssl
import uuid
import time
from datetime import datetime, timedelta, timezone

import requests
import urllib3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from requests.adapters import HTTPAdapter

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# Konfiguratsioon
# =============================================================================

# ECB SDMX REST API — hoiustamise püsivõimaluse intressimäär (Deposit Facility Rate)
# detail=dataonly tagastab ainult TIME_PERIOD ja OBS_VALUE veerud (puhas CSV)
ECB_API_URL = (
    "https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.DFR.LEV"
    "?format=csvdata&detail=dataonly"
)

# yfinance seaded
YFINANCE_INITIAL_DAYS = 5 * 365    # Esimene käivitus: 5 aastat ajalugu
YFINANCE_OVERLAP_DAYS = 5          # Kattuvus inkrementaalsel tõmbamisel (päevi)

# Airflow PostgreSQL ühenduse ID (määratud compose.yml-s AIRFLOW_CONN_ANALYTICS_DB kaudu)
POSTGRES_CONN_ID = "analytics_db"

# dbt käsud (käivitatakse BashOperatoriga Airflow konteineris)
DBT_DIR = "/opt/airflow/dbt_project"
DBT_SEED_CMD = f"cd {DBT_DIR} && dbt seed --profiles-dir ."
DBT_RUN_CMD  = f"cd {DBT_DIR} && dbt run --profiles-dir ."
DBT_TEST_CMD = f"cd {DBT_DIR} && dbt test --profiles-dir ."


# =============================================================================
# TLS adapter (Python 3.12 + OpenSSL 3.x + Docker MTU ühilduvusprobleem)
# HOIATUS: Lülitab TLS kontrollimise välja — sobib ainult avalike API-de jaoks
#          õppekeskkonnas. Ärge kasutage tundlike andmete korral.
# =============================================================================
class _LaxHTTPSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0)
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def _http_session() -> requests.Session:
    """
    Loob HTTP sessiooni, mis:
    1. Ignoreerib SSL/MTU vigu (kriitiline Docker for Mac/Linux puhul).
    2. Imiteerib tavalist veebibrauserit, et vältida Yahoo/API bot-blokke (kriitiline kõigil OS-idel).
    """
    s = requests.Session()
    s.mount("https://", _LaxHTTPSAdapter())
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return s


# =============================================================================
# Abifunktsioon: pipeline_runs kirje loomine ja uuendamine
# =============================================================================
def _start_run(hook: PostgresHook, source_name: str) -> str:
    """Loob pipeline_runs kirje olekuga 'running', tagastab run_id."""
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    hook.run(
        """
        INSERT INTO staging.pipeline_runs (run_id, fetched_at, source_name, status)
        VALUES (%s, %s, %s, 'running')
        """,
        parameters=(run_id, now, source_name),
    )
    return run_id


def _finish_run(hook: PostgresHook, run_id: str, success: bool, message: str = None):
    """Uuendab pipeline_runs kirjet lõppolekuga."""
    status = "success" if success else "failed"
    hook.run(
        "UPDATE staging.pipeline_runs SET status = %s, message = %s WHERE run_id = %s",
        parameters=(status, message, run_id),
    )


# =============================================================================
# Task 1 (paralleelne): ECB hoiustamise püsivõimaluse intressimäärad
# =============================================================================
def laadi_ecb_maarad(**context):
    """
    Tõmbab ECB SDMX REST API-st kogu intressimäärade ajaloo ja laadib
    staging.ecb_rates_raw tabelisse.

    Allikas: data-api.ecb.europa.eu (avalik, ei nõua API võtit)
    Formaat: CSV (detail=dataonly → ainult TIME_PERIOD ja OBS_VALUE)
    Strateegia: Alati kogu ajalugu (andmemaht väike, ~30-40 rida)
    """
    import io
    import pandas as pd

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_id = _start_run(hook, "ecb")
    now = datetime.now(timezone.utc)

    try:
        # --- API päring ---
        resp = _http_session().get(ECB_API_URL, timeout=30)
        resp.raise_for_status()

        # --- CSV parsimine ---
        # detail=dataonly tagastab ainult TIME_PERIOD ja OBS_VALUE
        df = pd.read_csv(io.StringIO(resp.text))

        # Veeru nimed võivad API versiooni muutudes varieeruda — otsime paindlikult
        time_col = next(c for c in df.columns if "TIME_PERIOD" in c.upper())
        obs_col  = next(c for c in df.columns if "OBS_VALUE"   in c.upper())

        df = df[[time_col, obs_col]].dropna()
        df.columns = ["date_ref", "rate_pct"]
        df["date_ref"] = pd.to_datetime(df["date_ref"]).dt.date

        if df.empty:
            raise ValueError("ECB API tagastas tühja vastuse — kontrolli URL-i")

        # --- Andmebaasi kirjutamine ---
        rows = [
            (run_id, now, str(row.date_ref), float(row.rate_pct), ECB_API_URL)
            for row in df.itertuples()
        ]
        hook.insert_rows(
            table="staging.ecb_rates_raw",
            rows=rows,
            target_fields=["run_id", "fetched_at", "date_ref", "rate_pct", "source_url"],
        )

        _finish_run(hook, run_id, success=True)
        print(f"ECB: laaditud {len(rows)} intressimäära kirjet (run_id={run_id})")


    except Exception as exc:
        _finish_run(hook, run_id, success=False, message=str(exc))
        raise


# =============================================================================
# Task 2 (paralleelne): Indeksite hinnaandmed Yahoo Finance'ist
# =============================================================================
def laadi_indeksite_hinnad(**context):
    import pandas as pd
    import yfinance as yf

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Aktiivsete tickeride laadimine dbt seed tabelist ---
    aktiivsed = hook.get_records(
        """
        SELECT ticker
        FROM   marts.dim_sectors
        WHERE  valid_to = '9999-01-01'
        ORDER  BY ticker
        """
    )
    tickers = [row[0] for row in aktiivsed]

    if not tickers:
        raise ValueError(
            "marts.dim_sectors on tühi või puudub. "
            "Veendu, et dbt_seed on edukalt käivitunud."
        )

    print(f"Tõmban hinnaandmeid {len(tickers)} tickeri kohta: {tickers}")

    # --- Inkrementaalne vahemik: mis on juba olemas? ---
    olemasolev = hook.get_records(
        """
        SELECT ticker, MAX(price_date)
        FROM   staging.index_prices_raw
        GROUP  BY ticker
        """
    )
    max_kuupaev = {row[0]: row[1] for row in olemasolev}

    run_id = _start_run(hook, "yfinance")
    now = datetime.now(timezone.utc)

    # curl_cffi on installitud _PIP_ADDITIONAL_REQUIREMENTS kaudu.
    # yfinance 0.2.37+ tuvastab curl_cffi automaatselt ja kasutab Chrome TLS-kätlust (ei pea käsitsi edastama)
    # Prior plain request (http_session) does a distinct Python handshake (TLS - transport layer security) 
    # curl_cffi instead uses actual Chrome's TLS implementation, so the handshake is genuine and FY cant make difference on protocol level
    # # impersonate='chrome120' means "pretend to be Chrome" 

    # curl_cffi
    try:
        import curl_cffi  # noqa: F401
        print(f"curl_cffi {curl_cffi.__version__} on saadaval — yfinance kasutab Chrome TLS automaatselt")
    except ImportError:
        print("HOIATUS: curl_cffi ei ole installitud — Yahoo võib blokeerida päringuid")

    MAX_TICKER_ATTEMPTS = 3

    try:
        ticker_errors = []
        total_rows_written = 0

        for ticker in tickers:
            if ticker in max_kuupaev and max_kuupaev[ticker]:
                start = max_kuupaev[ticker] - timedelta(days=YFINANCE_OVERLAP_DAYS)
            else:
                start = datetime.today().date() - timedelta(days=YFINANCE_INITIAL_DAYS)

            end = datetime.today().date()
            source_url = f"yfinance ticker={ticker} start={start} end={end} auto_adjust=True"

            # Korduskatsed eksponentsiaalse ooteajaga (5s, 10s, 20s)
            df = None
            for attempt in range(MAX_TICKER_ATTEMPTS):
                try:
                    yf_ticker = yf.Ticker(ticker)
                    df = yf_ticker.history(
                        start=str(start),
                        end=str(end),
                        interval="1d",
                        auto_adjust=True,
                        actions=False,
                    )
                    break
                except Exception as exc:
                    if attempt < MAX_TICKER_ATTEMPTS - 1:
                        delay = 4 * (2 ** attempt)
                        print(
                            f"HOIATUS: {ticker} katse {attempt + 1}/{MAX_TICKER_ATTEMPTS} "
                            f"ebaõnnestus: {exc}. Ootan {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        print(f"VIGA: {ticker} ebaõnnestus kõigis {MAX_TICKER_ATTEMPTS} katses: {exc}")
                        ticker_errors.append(ticker)

            if df is None:
                continue

            if df.empty:
                print(f"HOIATUS: {ticker} — tühi vastus vahemikus {start}–{end}")
                time.sleep(4)
                continue

            df = df.rename(columns={
                "Open": "open_price",
                "High": "high_price",
                "Low": "low_price",
                "Close": "close_price",
                "Volume": "volume",
            })
            df.index = pd.to_datetime(df.index).date

            ticker_read = []
            for price_date, row in df.iterrows():
                if pd.isna(row.get("close_price")):
                    continue
                ticker_read.append((
                    run_id,
                    now,
                    ticker,
                    str(price_date),
                    float(row["open_price"]) if pd.notna(row.get("open_price")) else None,
                    float(row["high_price"]) if pd.notna(row.get("high_price")) else None,
                    float(row["low_price"]) if pd.notna(row.get("low_price")) else None,
                    float(row["close_price"]),
                    int(row["volume"]) if pd.notna(row.get("volume")) else None,
                    source_url,
                ))

            if ticker_read:
                # Kirjutame kohe pärast igat tickerit — kui järgmine ticker blokeeritakse,
                # on see ticker juba andmebaasis ja järgmine katse ei pea seda uuesti tõmbama.
                hook.insert_rows(
                    table="staging.index_prices_raw",
                    rows=ticker_read,
                    target_fields=[
                        "run_id", "fetched_at", "ticker", "price_date",
                        "open_price", "high_price", "low_price", "close_price",
                        "volume", "source_url",
                    ],
                )
                total_rows_written += len(ticker_read)
                print(f"yfinance: {ticker} — kirjutatud {len(ticker_read)} rida")

            # Pausis tickerite vahel vähendab Yahoo rate-limit riski
            time.sleep(4)

        if ticker_errors:
            raise ValueError(
                f"Järgmised tickerid ebaõnnestusid pärast {MAX_TICKER_ATTEMPTS} katset: "
                f"{ticker_errors}. IP võib olla rate-limited."
            )

        if total_rows_written == 0:
            raise ValueError(
                "YFinance tagastas 0 rida kõigi tickerite jaoks — IP on tõenäoliselt rate-limited."
            )

        _finish_run(hook, run_id, success=True)
        print(f"yfinance: kokku {total_rows_written} rida kirjutatud (run_id={run_id})")

    except Exception as exc:
        _finish_run(hook, run_id, success=False, message=str(exc))
        raise


# =============================================================================
# DAG definitsioon
# =============================================================================
with DAG(
    dag_id="ecb_pipeline",
    description=(
        "ECB intressimäärad + STOXX Europe 600 sektori ETF hinnad → "
        "staging → dbt → marts"
    ),
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ecb", "yfinance", "intressimaarad", "aktsiatootlus"],
) as dag:

    # -------------------------------------------------------------------------
    # dbt seed: laadib seeds/indeksid.csv → marts.dim_indeksid
    # Peab käivituma enne andmete tõmbamist, et tickerid oleks DB-s saadaval
    # -------------------------------------------------------------------------
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=DBT_SEED_CMD,
    )

    # -------------------------------------------------------------------------
    # Paralleelsed andmete tõmbamise taskid
    # Mõlemad käivituvad korraga pärast dbt_seed'i
    # -------------------------------------------------------------------------
    ecb_task = PythonOperator(
        task_id="laadi_ecb_maarad",
        python_callable=laadi_ecb_maarad,
    )

    yfinance_task = PythonOperator(
        task_id="laadi_indeksite_hinnad",
        python_callable=laadi_indeksite_hinnad,
        retries=3,
        retry_delay=timedelta(minutes=10),
    )

    # -------------------------------------------------------------------------
    # dbt run: staging vaated + intermediate vaated + marts tabelid
    # -------------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_RUN_CMD,
    )

    # -------------------------------------------------------------------------
    # dbt test: andmekvaliteedi kontrollid (not_null, unique, accepted_values jms)
    # -------------------------------------------------------------------------
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_TEST_CMD,
    )

    # -------------------------------------------------------------------------
    # Sõltuvused:
    #   dbt_seed → [ecb_task, yfinance_task] → dbt_run → dbt_test
    # -------------------------------------------------------------------------
    dbt_seed >> [ecb_task, yfinance_task] >> dbt_run >> dbt_test