-- =============================================================================
-- 01_create_schemas.sql
--
-- Loob skeemid ja toorandmete tabelid, kuhu Airflow DAG andmeid kirjutab.
-- dbt loob oma mudelid (staging vaated, intermediate vaated, marts tabelid) ise.
--
-- See skript käivitub automaatselt AINULT ühel korral, kui analytics-db
-- konteiner käivitatakse esimest korda (tühi andmemaht).
-- Hilisemad muudatused tuleb rakendada migratsiooniskriptiga käsitsi.
--
-- Skeemide ülesanded:
--   staging     — Airflow kirjutab toorandmed siia (append-only raw tabelid)
--                 dbt loob siia ka puhastatud staging vaated
--   intermediate — dbt vahearvutused (vaated, mitte tabelid)
--   marts        — dbt lõpptulemused visualiseerimiseks (tabelid)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- -----------------------------------------------------------------------------
-- Pipeline käivituste jälgimine
-- Iga Airflow DAG käivitus saab unikaalse run_id.
-- Staging tabelid viitavad sellele — nii on võimalik andmete päritolu jälgida.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id      uuid         PRIMARY KEY,
    fetched_at  timestamptz  NOT NULL,
    source_name text         NOT NULL,  -- 'ecb' | 'yfinance'
    status      text         NOT NULL,  -- 'running' | 'success' | 'failed'
    message     text                    -- veateade, kui status = 'failed'
);

-- -----------------------------------------------------------------------------
-- ECB hoiustamise püsivõimaluse intressimäärad (toorandmed)
--
-- Allikas: ECB Data Portal REST API (SDMX), avalik, ei nõua autentimist
-- URL: https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.DFR.LEV
--       ?format=csvdata&detail=dataonly
--
-- Lisamisstrateegia: append-only (iga käivitus lisab kogu ajaloo uue run_id-ga)
-- Deduplitseerimine toimub dbt staging vaates (MAX(run_id) per date_ref)
--
-- Veerud:
--   run_id     — viide pipeline_runs tabelile
--   fetched_at — millal andmed tõmbati
--   date_ref   — intressimäära kehtimise alguskuupäev (ECB otsuse kuupäev)
--   rate_pct   — intressimäär protsentides (nt 4.00 = 4%)
--   source_url — täpne URL, millelt andmed tõmbati (auditeerimiseks)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.ecb_rates_raw (
    run_id      uuid          NOT NULL REFERENCES staging.pipeline_runs(run_id),
    fetched_at  timestamptz   NOT NULL,
    date_ref    date          NOT NULL,
    rate_pct    numeric(8, 4) NOT NULL,
    source_url  text          NOT NULL,
    PRIMARY KEY (run_id, date_ref)
);

-- -----------------------------------------------------------------------------
-- Indeksite hinnaandmed Yahoo Finance'ist (toorandmed)
--
-- Allikas: yfinance Python pakett (Yahoo Finance)
-- Tõmmatavad tickerid: märgitud dbt seeds failis (dbt_project/seeds/indeksid.csv)
--   ja talletatavad marts.dim_indeksid tabelis
--
-- Lisamisstrateegia: inkrementaalne + append-only
--   — Esimesel käivitusel tõmmatakse kogu ajalugu (kuni 10 aastat)
--   — Järgmistel käivitustel tõmmatakse ainult uued kuupäevad (alates viimasest
--     olemasolevast kuupäevast, 5 päeva kattuvusega vigade vältimiseks)
--   — Sama ticker + price_date võib esineda mitme run_id-ga (append-only)
--   — Deduplitseerimine toimub dbt staging vaates (MAX(run_id) per ticker, price_date)
--
-- Veerud:
--   run_id      — viide pipeline_runs tabelile
--   fetched_at  — millal andmed tõmbati
--   ticker      — ETF sümboli kood (nt 'EXV1.DE')
--   price_date  — kauplemispäev
--   open_price  — avamishind
--   high_price  — päeva kõrgeima hind
--   low_price   — päeva madalaim hind
--   close_price — sulgemishind (peamine mõõdik tootluse arvutamiseks)
--   volume      — kaubeldud maht (ühikutes)
--   source_url  — yfinance API päringus kasutatud parameetrid (auditeerimiseks)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.index_prices_raw (
    run_id       uuid           NOT NULL REFERENCES staging.pipeline_runs(run_id),
    fetched_at   timestamptz    NOT NULL,
    ticker       text           NOT NULL,
    price_date   date           NOT NULL,
    open_price   numeric(12, 4),
    high_price   numeric(12, 4),
    low_price    numeric(12, 4),
    close_price  numeric(12, 4) NOT NULL,
    volume       bigint,
    source_url   text           NOT NULL,
    PRIMARY KEY (run_id, ticker, price_date)
);
