# Arhitektuur

## Äriküsimus

Kuidas on Euroopa Keskpanga hoiustamise püsivõimaluse intressimäär seotud Euroopa erinevate sektorite indeksfondide tootlustega?

## Mõõdikud

1. Sektorite keskmised 30 päeva tootlused peale intressimäära tõusu ja langust
2. Sektorite ja EKP hoiustamise püsivõimaluse intressimäära muutuste rolling korrelatsioonid
3. Sektorite intressitundlikkuse beetad


## Andmeallikad

| Allikas | Tüüp | Ajas muutuv?                | Roll |
|---------|------|-----------------------------|------|
| Yahoo Finance | `yfinance` pythoni pakett | Jah, igal kauplemispäeval   | Sektorite indeksfondide ajalooline hinnainfo kauplemispäevadel |
| ECB Data Portal | API/CSV | Jah, intressiotsuste korral | ECB hoiustamise püsivõimaluse intressimäära muutuste ajalugu |
| `seeds/indeksid.csv` | seed | Ei, staatiline              | STOXX Europe 600 erinevate sektorite indeksfondide sümbolid yfinance päringuteks|

## Andmevoog

```mermaid
flowchart LR
    seeds[seeds/indeksid.csv] -->|dbt seed| dim[(marts.dim_indeksid)]
    yf[Yahoo Finance] -->|Airflow PythonOperator| raw_p[(staging.index_price_info)]
    ecb[ECB Data Portal] -->|Airflow PythonOperator| raw_r[(staging.ecb_deposit_rates)]
    raw_p -->|dbt staging| stg_p[staging.stg_index_prices]
    raw_r -->|dbt staging| stg_r[staging.stg_ecb_rates]
    stg_p -->|dbt intermediate| int_ret[(intermediate.sector_returns)]
    stg_r -->|dbt intermediate| int_al[(intermediate.aligned_ecb_rates)]
    int_ret -->|dbt marts| mart_post30[(marts.post_event_returns)]
    int_ret -->|dbt marts| mart_corr[(marts.rolling_correlation)]
    int_ret -->|dbt marts| mart_beta[(marts.sector_betas)]
    int_al --> mart_post30
    int_al --> mart_corr
    int_al --> mart_beta
    mart_post30 --> superset[Superset näidikulaud]
    mart_corr --> superset
    mart_beta --> superset
    airflow[Airflow scheduler] -->|"@daily"| yf
    airflow -->|"@daily"| ecb
    airflow -->|BashOperator| dbt[dbt run + dbt test]
```

## Andmebaasi kihid

| Kiht | Materiaalsus              | Roll                                                                                                           |
|------|---------------------------|----------------------------------------------------------------------------------------------------------------|
| `staging` | Tabel (raw) + Vaade (dbt) | API toorandmed ja puhastatud vaated (veerunimede, andmetüüpide korrastamine; JSON extraction; duplikaadid jne) |
| `intermediate` | Vaade (dbt)               | Vahearvutused ja analüütilised ettevalmistused.                                                                |
| `marts` | Tabel (dbt)               | Hoiab lõplikke analüütilisi mõõdikuid ja visualiseerimiseks valmis tabeleid.                                   |

Iga Airflow käivitus saab uue `run_id`. Staging kihis säilib kogu ajalugu (append-only); mart tabelid arvutatakse iga kord üle viimase ehk `MAX(run_id)` - mart tabelis sisaldub iga päeva kohta ainult üks rida.

## Tööjaotus

| Roll | Vastutus | Täitja   |
|------|----------|----------|
| Andmeallika omanik | Hoiab Airflow DAG'i töökorras, kontrollib API vastust | Argo     |
| Transformatsioonide omanik | Kirjutab ja hooldab dbt mudeleid | Kristjan |
| Kvaliteedi omanik | Kirjutab testid ja vaatab läbi ebaõnnestunud kontrollid | Kerttu   |
| Näidikulaua omanik | Ehitab näidikulaua ja seob selle äriküsimusega | Liis     |

## Riskid

| Risk | Mõju                         | Maandus                                                                                 |
|------|------------------------------|-----------------------------------------------------------------------------------------|
| API ei vasta | Airflow task ebaõnnestub     | Airflow logib vea; käivitamine kordub järgmisel tunnil automaatselt.                    |
| Indeksfondi sümbol muutub või eemaldatakse | Sektoril puuduvad uued andmed | Kontrollitakse hinnainfo tabelis andmelünkade olemasolu.                                |
| Scheduler ei käivitu | Andmed ei uuene              | Scheduler logib ebaõnnestunud käivitused ning andmevoogu saab käsitsi uuesti käivitada. |

## Privaatsus ja turve

Projekt kasutab ainult avalikke indeksfondide hinnaandmeid Yahoo Finance'ist ja Euroopa Keskpanga intressimäärasid ECB andmeportaalist. Isikuandmeid ei koguta. Andmebaasi kasutajanimi ja parool tulevad `.env` failist.
