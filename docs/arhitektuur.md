# Arhitektuur

## Äriküsimus

Kuidas on Euroopa Keskpanga(EKP) hoiustamise püsivõimaluse intressimäär seotud Euroopa erinevate sektorite indeksfondide tootlustega?

## Mõõdikud

1. Kirjeldav graafik kus on välja toodud indeksite hinnad ning intressimäärade tase vaadeldaval perioodil.
2. Sektorite keskmised 30 päeva tootlused peale intressimäära tõusu ja langust - Arvutame iga EKP intressimuutuse järel kui mitu protsenti indeks järgneva 30 päeva jooksul liigub.
3. Sektorite intressitundlikkuse beetad - Lineaarse regressiooniga hindame, kui palju liigub sektor 1 baaspunkti suuruse intressimuutuse korral.


## Andmeallikad

| Allikas | Tüüp | Ajas muutuv?                | Roll |
|---------|------|-----------------------------|------|
| Yahoo Finance | `yfinance` pythoni pakett | Jah, igal kauplemispäeval   | Sektorite indeksfondide ajalooline hinnainfo kauplemispäevadel |
| ECB Data Portal | API/XML | Jah, intressiotsuste korral | ECB hoiustamise püsivõimaluse intressimäära muutuste ajalugu |
| `seeds/dim_sectors.csv` | seed | Ei, staatiline              | STOXX Europe 600 erinevate sektorite indeksfondide sümbolid yfinance päringuteks|

## Andmevoog

```mermaid
flowchart LR
    seeds[seeds/dim_sectors.csv] -->|dbt seed| dim[(marts.dim_sectors)]
    yf[Yahoo Finance] -->|Airflow PythonOperator| raw_p[(staging.index_prices_raw)]
    ecb[ECB Data Portal] -->|Airflow PythonOperator| raw_r[(staging.ecb_rates_raw)]
    raw_p -->|dbt staging| stg_p[staging.stg_index_prices]
    raw_r -->|dbt staging| stg_r[staging.stg_ecb_rates]
    stg_p -->|dbt intermediate| int_ret[intermediate.int_sector_returns]
    stg_p --> int_al[intermediate.int_aligned_ecb_rates]
    stg_r -->|dbt intermediate| int_al
    stg_p -->|dbt marts| mart_pr[(marts.mart_prices_and_rates)]
    int_al --> mart_pr
    dim --> mart_pr
    int_ret -->|dbt marts| mart_post30[(marts.mart_post_event_returns)]
    stg_r --> mart_post30
    dim --> mart_post30
    mart_post30 -->|dbt marts| mart_beta[(marts.mart_sector_betas)]
    mart_pr --> superset[Superset näidikulaud]
    mart_post30 --> superset
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
