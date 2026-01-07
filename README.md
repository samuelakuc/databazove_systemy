ELT proces datasetu YES ENERGY

#1. ÚVOD A POPIS ZDROJOVÝCH DÁT

V tomto projekte analyzujeme dáta o elektrickej energii, jej cenách, zmenách cien v čase a vplyv obnoviteľnej energie na produkciu aj cenu elektrickej energie v regióne ERCOT. Analýza prebieha na základe dát z datasetu Yes Energy.

Cieľom je analyzovať a porozumieť:
1. Presnosti predikcie spotreby elektrickej energie (graf1, graf 3, graf4)
2. Stúpaniu a klesaniu cien elektrickej energie v čase (graf2)
3. Vplyvu používania obnoviteľných zdrojov, ich výhody a nevýhody (graf 5, graf6)

Dáta z datasetu Yes Energy boli analyzované a extrahované nasledujúcich tabuliek:
dart_loads_sample (údaje o skutočnej spotrebe elektrickej energie)
dart_prices_sample (cenové údaje pre jednotlivé územné uzly a oblasti)
ds_object_list_sample (informácie o všetkých objektoch - uzly, oblasti, load zones...)
iso_market_times_sample (časové údaje)
ts_forecast_sample (údaje predikcií výroby elektrickej energie)
sts_gen_sample (údaje reálnej výroby elektrickej energie obnoviteľných zdrojov)

---

##1.1 Dátová architektúra
Surové dáta z datasetu Yes Energy sú usporiadané v relačnom modeli, zobrazenom v nasledujúcom ERD Diagrame
![Staging ERD](staging_diagram.png)

---

#2. DIMENZIONÁLNY MODEL

Star schema obsahuje 1 tabuľku faktov fact_power_prices a 4 dimenzie:
1. dim_datetime - dimenzia času, obsahuje údaje o čase záznamu, na faktovú tabuľku je napojená skrz datetime_id, SCD0
2. dim_load_zone - obsahuje údaje o load zónach a geografických oblastiach na predikciu výroby elektricej energie, zaznamenáva či je údaj platný a aktuálny. Na faktovú tabuľku je napojená skrz load_zone_id, SCD2
3. dim_price_nodes - informácie o uzloch na určovanie cien elektrickej energie, napojená na faktovú tabuľku skrz price_nodes_id, SCD1
4. dim_generation_type - údaje o type produkovanej elektrickej energie (solar,wind,...), napojené na faktovú tabuľku skrz generation_type_id, SCD0

FACT TABLE
PK - id
FK - dim_datetime_id, dim_nodes_id, dim_load_zone_id, dim_generation_type_id
Hlavné metriky - dalmp, rtlmp, ich stddev hodnoty, load_forecast, net_load_forecast, rt_load, údaje o predikcií a realnej produkcií obnoviteľných zdrojov, net_load_rt
![Star Schema](img/star_schema.png)
