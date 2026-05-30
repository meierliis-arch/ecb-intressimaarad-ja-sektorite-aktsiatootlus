# Edenemisraport


# NB! Vajab üle vaatamist!!!

## Mis on valmis

- [x] Docker Compose käivitab kõik teenused (analytics-db, Airflow, Superset)
- [x] Andmeid saadakse allikast kätte — ECB SDMX REST API (intressimäärad) ja Yahoo Finance / yfinance (sektori ETF-ide hinnad)
- [x] Airflow DAG (`ecb_pipeline`) on kirjutatud: `dbt_seed → [laadi_ecb_maarad ∥ laadi_indeksite_hinnad] → dbt_run → dbt_test`
- [x] Andmed laetakse `staging` kihti: `staging.ecb_rates_raw` ja `staging.index_prices_raw` (append-only, idempotentne)
- [x] dbt staging mudelid (`stg_ecb_rates`, `stg_index_prices`) deduplitseerivad toorandmed
- [x] dbt intermediate mudelid (`int_sector_returns`, `int_aligned_ecb_rates`) on kirjutatud
- [x] dbt mart mudelid on kirjutatud — kolm analüütilist tabelit vastavalt arhitektuuridokumendi kolmele mõõdikule
- [x] dbt alaptestid (not_null, unique) on `schema.yml` failides defineeritud
- [x] Sektori dimensioonitabel (`dim_sectors`) kasutab `valid_from`/`valid_to` struktuuri
- [ ] Kogu andmevoog on reaalselt käivitatud ja verifitseeritud (testimisel)
- [ ] Vähemalt üks näidikulaud on Supersetis nähtaval (vaja Supersetis käsitsi luua)

## Juhis dockeri käivitamiseks

1. **Käivita stack** (`docker compose up -d --build`)
  - Kontrolli konteinerite tervist (`docker compose ps`) -> peaks olema healthy
  
2. **Triggerida DAG Airflowi UI-st**
  - Ava Airflow: http://localhost:8080 
    - kasutaja: airflow 
    - parool .env failis (by default: airflow)
  - Lülita sisse pipeline 'ecb-pipeline' -> käivitub koheselt (+ automaatselt iga päev) -> oota lõpuni
    - võib võtta 3-5 minutit, kuna laeb 3-5 aasta Yahoo andmeid
  - vt 'Kontrollpunkt' allpool andmete eduka salvestamise tagamiseks

3. **Tutvu Superseti visuaalidega**
  - Ava Superset: http://localhost:8088 
    - kasutaja: admin
    - parool .env failis (by default: admin)
- Luua Supersetis analytics-db andmebaasi ühendus ja vähemalt üks visuaal (nt ECB intressimäär ajas)
- Vaadata üle dbt testi tulemused, lisada puuduvad kvaliteedikontrollid (Kerttu)
- Täiendada Superset dashboard kõigi kolme mõõdikuga (Liis)

## Mis takistab
# TO BE ANNOUNCED
- Kui port 8080 või 8088 on hõivatud, muuda `.env` failis `AIRFLOW_PORT_HOST` ja `SUPERSET_PORT_HOST`

## Kontrollpunkt

```bash
# 1. Käivita stack (esimene kord võtab ~10 minutit — Docker laeb pilte ja installib pakke)
docker compose up -d --build

# 2. Kontrolli, et kõik teenused töötavad
docker compose ps
# Oodatav: kõik teenused 'running' või 'healthy'
# ecb-airflow-init ja ecb-superset-init võivad olla 'exited (0)' — see on normaalne

# 4. Kontrolli, et andmed jõudsid staging tabelisse
docker compose exec analytics-db psql -U praktikum -d praktikum -c \
  "SELECT source_name, status, fetched_at FROM staging.pipeline_runs ORDER BY fetched_at DESC LIMIT 5;"

# 5. Kontrolli mart tabeleid
docker compose exec analytics-db psql -U praktikum -d praktikum -c \
  "SELECT ticker, sector_name, beta FROM marts.mart_sector_betas ORDER BY beta DESC;"
```

Oodatav tulemus: `pipeline_runs` tabelis on kaks rida olekuga `success` (üks ECB, üks yfinance), `mart_sector_betas` tabelis on 19 sektori beetakordajad.
