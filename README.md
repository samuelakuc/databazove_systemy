ELT proces datasetu YES ENERGY

1. ÚVOD A POPIS ZDROJOVÝCH DÁT

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

1.1 Dátová architektúra
Surové dáta z datasetu Yes Energy sú usporiadané v relačnom modeli, zobrazenom v nasledujúcom ERD Diagrame
Staging diagram 
![Staging ERD](staging_diagram.png)
