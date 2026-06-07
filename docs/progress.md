# Edenemisraport

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused: analytics-db, Airflow, Superset (sh Superseti automaatne dashboard'i import).
- [x] Andmeid saadakse mõlemast allikast: ECB SDMX REST API (intressimäärad) ja Yahoo Finance / yfinance (sektori ETF-ide hinnad).
- [x] Airflow DAG (`ecb_pipeline`, `@daily`) on töös: `dbt_deps → dbt_seed → [laadi_ecb_maarad ∥ laadi_indeksite_hinnad] → dbt_run → dbt_test`.
- [x] Andmed laetakse `staging` kihti: `staging.ecb_rates_raw` ja `staging.index_prices_raw` (append-only, idempotentne; iga käivitus saab `run_id`).
- [x] dbt staging mudelid (`stg_ecb_rates`, `stg_index_prices`) deduplitseerivad toorandmed.
- [x] dbt intermediate mudelid (`int_sector_returns`, `int_aligned_ecb_rates`) arvutavad päevatootlused ja forward-fillitud intressimäära.
- [x] dbt mart mudelid (`mart_prices_and_rates`, `mart_post_event_returns`, `mart_sector_betas`) katavad kõik kolm ärimõõdikut.
- [x] Sektori dimensioonitabel (`marts.dim_sectors`) kasutab `valid_from`/`valid_to` struktuuri.
- [x] dbt testid läbivad: `not_null`, `unique`, `accepted_range`, `accepted_values`, kombineeritud unikaalsus (`unique_combination_of_columns`) ja mudelivaheline `relationships`.
- [x] Superseti näidikulaud kõigi kolme mõõdikuga imporditakse automaatselt Dockeri käivitamisel (`superset-import` teenus) - käsitsi ZIP-i üleslaadimist pole vaja.
- [x] Kogu andmevoog on reaalselt käivitatud ja korduvalt testitud otsast lõpuni.

## Järgmised sammud

Projekt on lõpetatud - kõik planeeritud sammud on valmis. Võimalikud edasiarendused on koondatud README peatükki „Kokkuvõte, puudused ja võimalikud edasiarendused".

## Mis takistab

- Esimene `docker compose up --build` võtab ~10 minutit (Docker laeb pilte ja pip installib dbt/yfinance/pandas).
- Esimene DAG-i käivitus võtab 3–5 minutit, kuna yfinance laeb mitme aasta päevahinnad. Yahoo Finance võib anda rate-limit vea - Airflow proovib automaatselt uuesti (kuni 3 korda, 10-minutilise pausiga).
- Kui pordid 8080 või 8088 on hõivatud, muuda `.env` failis `AIRFLOW_PORT_HOST` / `SUPERSET_PORT_HOST` väärtusi.

## Kontrollpunkt

```bash
# 1. Käivita stack (esimene kord võtab ~10 minutit - Docker laeb pilte ja installib pakke)
docker compose up -d --build

# 2. Kontrolli, et kõik teenused töötavad
docker compose ps
# Oodatav: kõik teenused 'running' või 'healthy'
# ecb-airflow-init, ecb-superset-init ja ecb-superset-import võivad olla 'exited (0)' - see on normaalne

# 3. Kontrolli, et andmed jõudsid staging tabelisse
docker compose exec analytics-db psql -U praktikum -d praktikum -c \
  "SELECT source_name, status, fetched_at FROM staging.pipeline_runs ORDER BY fetched_at DESC LIMIT 5;"

# 4. Kontrolli mart tabeleid
docker compose exec analytics-db psql -U praktikum -d praktikum -c \
  "SELECT ticker, sector_name, beta FROM marts.mart_sector_betas ORDER BY beta DESC;"
```

Oodatav tulemus: kõik teenused `healthy`/`running`, Airflow DAG viimase käivituse olek `success`, dbt testid `passed`. `pipeline_runs` tabelis on kaks rida olekuga `success` (üks ECB, üks yfinance) ja `mart_sector_betas` tabelis 19 sektori beetakordajad. Superseti näidikulaud on kohe nähtav aadressil http://localhost:8088.
