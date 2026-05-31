# ECB intressimäärad ja sektorite aktsiatootlus — Airflow + dbt + Superset

Projekt uurib, kuidas Euroopa Keskpanga (EKP) hoiustamise püsivõimaluse intressimäär on seotud STOXX Europe 600 sektori indeksfondide tootlustega. Andmevoog töötab täisautomaatselt: Airflow orkestreerib andmete tõmbamise ja dbt transformatsioonid, Superset kuvab tulemused.

## Äriküsimus

Kuidas reageerivad Euroopa aktsiaturu sektorid EKP intressimäära muutustele ja kui suur on see reaktsioon?

**Mõõdikud:**
1. Kirjeldav aegrida — sektori sulgemishinnad ja intressimäär samal teljel vaadeldaval perioodil
2. Keskmised 30-päeva tootlused pärast iga EKP intressiotsust (tõus vs. langus)
3. Beetakoefitsiendid (OLS regressioon) — üks arv sektori kohta: kui palju muutub sektori 30-päeva tootlus 1 baaspunkti suuruse intressimuutuse korral

## Andmestik

| Allikas | Tüüp | Ajas muutuv? | Roll |
|---------|------|--------------|------|
| ECB Data Portal | Avalik REST API, XML | Jah, intressiotsuste korral (~8× aastas) | EKP hoiustamise püsivõimaluse intressimäära ajalugu |
| Yahoo Finance | `yfinance` Pythoni pakett | Jah, iga kauplemispäev | 19 STOXX Europe 600 sektori ETF-i päevahinnad (XETRA) |
| `seeds/dim_sectors.csv` | Staatiline dbt seeme | Ei, muutub ainult projekti muutmisel | ETF-i tickerid koos kehtivusperioodiga |

## Stack

| Komponent | Tööriist |
|-----------|----------|
| Orkestreerimine | Apache Airflow 3.x |
| Transformatsioon | dbt Core 1.10 |
| Andmehoidla | PostgreSQL (pgDuckDB) |
| Näidikulaud | Apache Superset 6.x |
| Infrastruktuur | Docker Compose |

Kõik komponendid käivituvad Dockeris — dbt ega Airflow'i ei pea kohalikult paigaldama.

## Andmevoog lühidalt

1. **Seed andmete laadimine** — `dbt seed` laadib `dim_sectors.csv` → `marts.dim_sectors` (19 aktiivset tickerit).
2. **Sissevõtt (paralleelselt)** — Airflow tõmbab ECB intressimäärad REST API-st ja Yahoo Finance'ist ETF-ide päevahinnad.
3. **Laadimine** — Andmed kirjutatakse staging toorandmete tabelitesse (`append-only`; iga käivitus saab unikaalse `run_id`).
4. **Transformatsioon** — `dbt run` ehitab staging vaated → intermediate vaated → marts tabelid.
5. **Testimine** — `dbt test` kontrollib andmekvaliteeti; ebaõnnestumine märgib Airflow töövoo punaseks.
6. **Näidikulaud** — Superset loeb `marts.*` tabeleid ja näitab kolme ärimõõdikut.

## Andmevoog

```
ECB Data Portal
    ↓ (Airflow PythonOperator, @daily)
staging.ecb_rates_raw              ← toorandmed
    ↓ (dbt staging vaade)
staging.stg_ecb_rates              ← puhastatud, deduplitseeritud

Yahoo Finance (yfinance)
    ↓ (Airflow PythonOperator, @daily)
staging.index_prices_raw           ← toorandmed
    ↓ (dbt staging vaade)
staging.stg_index_prices           ← puhastatud, deduplitseeritud

    ↓ (dbt intermediate vaated)
intermediate.int_sector_returns    ← päevane protsendine tootlus (LAG-funktsioon)
intermediate.int_aligned_ecb_rates ← intressimäär iga kauplemispäeva kohta (forward-fill)

    ↓ (dbt marts tabelid)
marts.mart_prices_and_rates        ← mõõdik 1: kirjeldav aegrida
marts.mart_post_event_returns      ← mõõdik 2: 30-päeva tootlused EKP otsuse järel
marts.mart_sector_betas            ← mõõdik 3: OLS beetakoefitsient sektori kohta
    ↓
Superset näidikulaud
```

## Projekti struktuur

```
.
├── compose.yml                        ← kõik teenused ühes failis
├── .env.example                       ← kopeeri .env-iks ja täida saladused
├── Dockerfile.superset
├── init/
│   └── 01_create_schemas.sql          ← loob skeemid ja toorandmete tabelid (käivitub üks kord)
├── airflow/
│   └── dags/
│       └── ecb_pipeline.py            ← Airflow DAG (seemestamine → tõmbamine → dbt)
├── dbt_project/
│   ├── dbt_project.yml                ← dbt projekti seadistus (kiht → skeem)
│   ├── profiles.yml                   ← andmebaasi ühendus (loeb keskkonnamuutujad)
│   ├── seeds/
│   │   └── dim_sectors.csv            ← 19 sektori ETF-i koos kehtivusperioodiga
│   ├── macros/
│   │   └── generate_schema_name.sql   ← dbt kasutab täpseid skeemide nimesid (mitte prefiksiga)
│   └── models/
│       ├── staging/
│       │   ├── sources.yml            ← toorandmete tabelid dbt allikatena
│       │   ├── stg_ecb_rates.sql      ← ECB toorandmete deduplitseerimine
│       │   ├── stg_index_prices.sql   ← hinna toorandmete deduplitseerimine
│       │   └── schema.yml             ← staging mudelite testid
│       ├── intermediate/
│       │   ├── int_sector_returns.sql    ← päevane tootlus LAG() kaudu
│       │   ├── int_aligned_ecb_rates.sql ← intressimäär forward-filliga iga kauplemispäevale
│       │   └── schema.yml
│       └── marts/
│           ├── mart_prices_and_rates.sql    ← mõõdik 1: kirjeldav aegrida
│           ├── mart_post_event_returns.sql  ← mõõdik 2: tootlused EKP otsuse järel
│           ├── mart_sector_betas.sql        ← mõõdik 3: OLS beeta sektori kohta
│           └── schema.yml
├── superset/
│   ├── superset_config.py
│   └── dashboard_export.zip           ← valmis näidikulaud (imporditav Superseti kaudu)
└── docs/
    ├── arhitektuur.md
    ├── getting-started.md
    └── project-overview.md
```

## Käivitamine

```bash
# 1. Kopeeri keskkonnamuutujad
cp .env.example .env

# 2. Ava `.env` tekstiredaktoris ja **genereeri turvaline SECRET_KEY Superseti jaoks** — ilma selleta Superset ei käivitu:
python3 -c "import secrets; print(secrets.token_hex(32))"

# 3. Asenda `.env` failis `CHANGE_ME_generate_with_python_secrets` genereeritud väärtusega.

# 4. Käivita kõik teenused
docker compose up -d --build

# 5. Ava Airflow UI ja käivita DAG esimest korda käsitsi
#    http://localhost:8080  (kasutaja/parool: airflow / airflow)
#    → ecb_pipeline → lülita DAG sisse → vajuta ▶ Trigger DAG
# Esimene käivitus võtab **2–4 minutit** (yfinance tõmbab 5 aasta × 19 sektori hinnaandmed). Rohelised kastid tähendavad edu; punaste puhul klõpsa taskil → „Logs".
# NB! Yahoo Finance laadi_indeksite_hinnad task võib saada Rate Limit Error vea - Airflow proovib ise hiljem uuesti, tuleks varuda kannatust :)

# 6. Ava Superset
#    http://localhost:8088  (kasutaja/parool: vt .env SUPERSET_ADMIN_USER/PASSWORD)
#    Loo ühendus analytics-db andmebaasiga:"
#       Settings > Database Connections > + Database"
#       SQLAlchemy URI: postgresql+psycopg2://praktikum:praktikum@analytics-db:5432/praktikum
#    Lae üles superset/dashboard_export.zip fail, parool: {POSTGRES_PASSWORD}
#    Kui teed muudatusi dashboardis, tuleks see uuesti exportida -> kirjutada üle vana .zip fail -> commitida reposse.
```

## Saladused ja konfiguratsioon

Kõik paroolid ja võtmed on `.env` failis. Reposse läheb ainult `.env.example`. Päris `.env` on `.gitignore`-s.

| Muutuja | Tähendus |
|---------|----------|
| `POSTGRES_PASSWORD` | Analüütika andmebaasi parool |
| `AIRFLOW_USER` / `AIRFLOW_PASSWORD` | Airflow UI sisselogimine |
| `SUPERSET_SECRET_KEY` | Superseti sessiooniküpsiste krüptovõti — **genereeri uus**, ära jäta vaikeväärtust |
| `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` | Superseti halduskasutaja |
| `SUPERSET_DB_PASSWORD` | Superseti metaandmebaasi parool |
| `AIRFLOW_UID` | Airflow konteineri kasutaja UID — Linuxis sea `$(id -u)` |

## Andmekvaliteedi testid

`dbt test` käivitub automaatselt iga DAG-käivituse lõpus. Testide ebaõnnestumine märgib Airflow töövoo punaseks.

Kõigi mudelite `schema.yml` failides on konfigureeritud `not_null` ja `unique` testid põhivõtmete ning kriitiliste veergude jaoks (`date_ref`, `rate_pct`, `ticker`, `price_date`, `close_price`).

**Planeeritud laiendused** (vastutaja: Kerttu):
- Kombineeritud unikaalsuse testid — nt `(event_date, ticker)` tabelis `mart_post_event_returns`
- Väärtuste vahemiku testid — nt intressimäär vahemikus −1 kuni 15 protsenti
- Mudelivahelised seosete testid (`relationships`)

## dbt käsud (käsitsi käivitamiseks)

```bash
# Ava Airflow konteineri shell (dbt on sealt kättesaadav):
docker compose exec ecb-airflow-apiserver bash

# dbt projekti kaustas:
cd /opt/airflow/dbt_project

dbt seed --profiles-dir .            # laadib dim_sectors.csv
dbt run --profiles-dir .             # käivitab kõik mudelid
dbt test --profiles-dir .            # käivitab testid
dbt docs generate --profiles-dir .   # genereerib dokumentatsiooni
```

## Superset seadistus

Kui DAG on vähemalt korra edukalt läbi jooksnud:

### 1. Lisa andmebaasi ühendus

**Settings → Database Connections → + Database → PostgreSQL**

Sisesta ühendusstring:

```
postgresql://praktikum:praktikum@ecb-analytics-db:5432/praktikum
```

> Host on `ecb-analytics-db` (konteineri nimi), mitte `localhost` — Superset ja andmebaas on samas Dockeri võrgus.

Vajuta **Test Connection** (peaks andma „Connection looks good!") → **Connect**.

### 2. Impordi näidikulaud

Valmis näidikulaud on reposis (`superset/dashboard_export.zip`).

**Settings → Import dashboards → Upload ZIP file** → vali `superset/dashboard_export.zip` → **Import**.

### 3. Päringud SQL Laboris

Andmeid saab uurida ka **SQL Lab → SQL Editor**:

```sql
-- Mõõdik 3: sektorite beetad kahanevalt
SELECT * FROM marts.mart_sector_betas ORDER BY beta DESC;

-- Mõõdik 2: tootlused EKP otsuste järel
SELECT * FROM marts.mart_post_event_returns LIMIT 50;

-- Mõõdik 1: kirjeldav aegrida ühe sektori kohta
SELECT * FROM marts.mart_prices_and_rates WHERE ticker = 'EXV1.DE' LIMIT 50;
```

## Tõrkeotsing

**Airflow näitab „Broken DAG" viga**
DAG-failis on Pythoni süntaksiviga või ebaõnnestunud import. Vaata:
```bash
docker compose logs ecb-airflow-dag-processor
```

**`laadi_indeksite_hinnad` ebaõnnestub veateatega „marts.dim_sectors does not exist"**
`dbt_seed` task enne seda ebaõnnestus. Klõpsa Airflow'is `dbt_seed` → „Logs".

**`laadi_indeksite_hinnad` ebaõnnestub korduvalt**
Yahoo Finance piirab päringute sagedust. Task kordab automaatselt kuni 3 korda (10-minutilise pausiga). Kui kõik korduskatsed ebaõnnestuvad, on konkreetsed tickerid loetletud Airflow logis.

**Superset näitab „Connection refused" andmebaasi testimisel**
Veendu, et kasutasid hosti nimena `ecb-analytics-db`, mitte `localhost`.

**Pordid 8080, 8088 või 55432 on hõivatud**
Muuda `.env` failis vastavaid pordiväärtusi:
```
AIRFLOW_PORT_HOST=8081
SUPERSET_PORT_HOST=8089
DB_PORT_HOST=55433
```

**Esimene `docker compose up` võtab kaua aega**
Normaalne — pip paigaldab dbt, yfinance ja pandas konteinerite sisse. Juhtub ainult esmakordsel ehitamisel.

## Tööjaotus

| Liige | Roll | Vastutusala |
|-------|------|-------------|
| **Argo** | Andmeallika omanik | Airflow DAG (`airflow/dags/ecb_pipeline.py`), API ühendused, pipeline'i tööshoiu |
| **Kristjan** | Transformatsioonide omanik | dbt mudelid (`dbt_project/models/`), SQL loogika kirjutamine ja hooldus |
| **Kerttu** | Kvaliteedi omanik | dbt testid (`schema.yml` failid), ebaõnnestunud testikäivituste analüüs |
| **Liis** | Näidikulaua omanik | Superseti näidikulaud, andmebaasi ühendus, graafikud |

## Kokkuvõte, puudused ja võimalikud edasiarendused

**Mis töötab:**
- Pipeline töötab otsast lõpuni: Airflow → staging → dbt → Superset
- Andmete tõmbamine on inkrementaalne — iga järgnev käivitus tõmbab ainult uued kauplemispäevad
- dbt testid kontrollivad andmekvaliteeti automaatselt iga käivituse lõpus
- Airflow kordab ebaõnnestunud yfinance taskid automaatselt (kuni 3 korda, 10-minutilise pausiga)

**Teadlikud piirangud:**
- Superset näidikulaud tuleb esimesel korral käsitsi importida (ZIP-fail on repos olemas)
- `staging.ecb_rates_raw` ja `staging.index_prices_raw` kasvavad piiramatult — vanade käivituste andmeid ei kustutata
- dbt testide katvus on praegu minimaalne; väärtuste vahemiku ja mudelivahelised testid on lisamisel

**Võimalikud edasiarendused:**
- Laiendada analüüsi teiste EKP intressimääradega (nt refinantseerimisoperatsioonide määr)
- Automatiseerida Superseti näidikulaua importimine `docker compose up` käivitamisel
- Lisada `staging.pipeline_runs` põhjal Airflow sensor, mis kontrollib eelmise käivituse edukust enne uue alustamist
- Kaaluda Yahoo Finance asendamist stabiilsema andmeallikaga (nt Stooq `pandas_datareader` kaudu), kui rate-limiting osutub jätkuvaks probleemiks
