# Arhitektuur

## Äriküsimus

Kuidas on Euroopa Keskpanga hoiustamise püsivõimaluse intressimäär seotud Euroopa erinevate sektorite indeksfondide tootlustega?

## Mõõdikud

1. Sektorite keskmised 30 päeva tootlused peale intressimäära tõusu ja langust
2. Sektorite ja EKP hoiustamise püsivõimaluse intressimäära muutuste rolling korrelatsioonid
3. Sektorite intressitundlikkuse beetad


## Andmeallikad

| Allikas | Tüüp | Ajas muutuv? | Roll |
|---------|------|--------------|------|
| Yahoo Finance | `yfinance` pythoni pakett | Jah, iga kauplemispäeva | Sektorite indeksfondide ajalooline hinnainfo kauplemispäevadel |
| ECB Data Portal | API/CSV | Jah, intressiotsuste korral | ECB hoiustamise püsivõimaluse intressimäära muutuste ajalugu |
| `seeds/indeksid.csv` | seed | Ei, staatiline | STOXX Europe 600 erinevate sektorite indeksfondide sümbolid yfinance päringuteks|

## Andmevoog

```mermaid
flowchart LR

    seeds[seeds/indeksid.csv]
    ecb[ECB Data Portal]
    yf[Yahoo Finance]

    scheduler[Scheduler daily]

    stg_prices[(staging.index_price_info)]
    stg_rates[(staging.ecb_deposit_rates)]

    int_returns[(intermediate.sector_returns)]
    int_aligned_rates[(intermediate.aligned_ecb_rates)]

    mart_post30[(mart.post_event_returns)]
    mart_corr[(mart.rolling_correlation)]
    mart_beta[(mart.sector_betas)]

    returns[Indeksfondide tootluste arvutus]
    align[ECB määrade joondamine]
    post30[30 päeva tootluste arvutus]
    rollingcorr[Rolling correlation arvutus]
    beta[Beetade arvutus]

    dashboard[Superset näidikulaud]
    quality[Andmekvaliteedi testid]

    scheduler --> yf
    scheduler --> ecb

    seeds --> yf

    yf --> stg_prices
    ecb --> stg_rates

    stg_prices --> returns
    stg_prices --> align
    stg_rates --> align

    returns --> int_returns
    align --> int_aligned_rates

    int_returns --> post30
    int_aligned_rates --> post30

    int_returns --> rollingcorr
    int_aligned_rates --> rollingcorr

    int_returns --> beta
    int_aligned_rates --> beta

    post30 --> mart_post30
    rollingcorr --> mart_corr
    beta --> mart_beta

    mart_post30 --> dashboard
    mart_corr --> dashboard
    mart_beta --> dashboard

    mart_post30 --> quality
    mart_corr --> quality
    mart_beta --> quality
```

## Andmebaasi kihid

| Kiht | Roll |
|------|------|
| `staging` | Hoiab lähteandmeid analüüsi jaoks. |
| `intermediate` | Vahearvutused ja analüütilised ettevalmistused. |
| `mart` | Hoiab lõplikke analüütilisi mõõdikuid ja visualiseerimiseks valmis tabeleid. |

## Tööjaotus

| Roll | Vastutus | Täitja |
|------|----------|--------|
| Andmeallika omanik | Kirjutab sissevõtu loogika, hoiab API-t töös | [Nimi] |
| Transformatsioonide omanik | Kirjutab mart kihi mudelid ja mõõdikute arvutuse | [Nimi] |
| Kvaliteedi omanik | Kirjutab testid ja vaatab läbi ebaõnnestunud kontrollid | [Nimi] |
| Näidikulaua omanik | Ehitab näidikulaua ja seob selle äriküsimusega | [Nimi] |

## Riskid

| Risk | Mõju | Maandus |
|------|------|---------|
| API ei vasta | Andmed ei uuene | Skript annab veateate, mille korral saab andmevoo käsitsi uuesti käivitada. |
| Indeksfondi sümbol muutub või eemaldatakse | Sektoril puuduvad uued andmed | Kontrollitakse hinnainfo tabelis andmelünkade olemasolu. |
| Scheduler ei käivitu | Andmed ei uuene | Scheduler logib ebaõnnestunud käivitused ning andmevoogu saab käsitsi uuesti käivitada. |

## Privaatsus ja turve

Projekt kasutab ainult avalikke indeksfondide hinnaandmeid yahoo financeist ja Euroopa Keskpanga intressimäärasid ECB andmeportaalist. Isikuandmeid ei koguta. Andmebaasi kasutajanimi ja parool tulevad `.env` failist.
