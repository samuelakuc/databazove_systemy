use database badger_db;
use warehouse badger_wh;
use schema staging;

show databases like '%yes%';
show schemas in database yes_energy__sample_data;
SHOW TABLES IN SCHEMA YES_ENERGY__SAMPLE_DATA.YES_ENERGY_SAMPLE;

select * from yes_energy__sample_data.yes_energy_sample.dart_loads_sample limit 10;
select * from yes_energy__sample_data.yes_energy_sample.dart_prices_sample limit 10;
select * from yes_energy__sample_data.yes_energy_sample.ds_object_list_sample limit 10;
select * from yes_energy__sample_data.yes_energy_sample.iso_market_times_sample limit 10;
select * from yes_energy__sample_data.yes_energy_sample.ts_forecast_sample limit 10;
select * from yes_energy__sample_data.yes_energy_sample.ts_gen_sample limit 100;

EXTRACT(staging tabulky)

create or replace table stg_prices as
select objectid, datetime, dalmp, rtlmp, iso
from yes_energy__sample_data.yes_energy_sample.dart_prices_sample
where iso = 'E';

select * from stg_prices limit 50;

create or replace table stg_nodes AS
SELECT objectid, 
objectname, 
objecttype,
zone
from yes_energy__sample_data.yes_energy_sample.ds_object_list_sample;

select * from stg_nodes limit 50;


create or replace table stg_times AS
SELECT datetime, 
marketday, 
onpeak,
offpeak,
wepeak,
wdpeak
from yes_energy__sample_data.yes_energy_sample.iso_market_times_sample
where iso = 'ERCOT';

select * from stg_times limit 50;

create or replace table stg_load_forecast AS
SELECT objectid, 
datatypeid, 
datetime,
value,
publishdate
from yes_energy__sample_data.yes_energy_sample.ts_forecast_sample
where objectid in(10000712973)
AND datatypeid in(19060,9285, 662);

select * from stg_load_forecast limit 50;

select objectid, objectname 
from yes_energy__sample_data.yes_energy_sample.ds_object_list_sample
where objecttype = 'load_zone';

select * from yes_energy__sample_data.yes_energy_sample.v_data_catalog_sample limit 50;

select distinct datatypeid, datatype_name,
from yes_energy__sample_data.yes_energy_sample.v_data_catalog_sample
where datatype_name ILIKE '%load%'
   or datatype_name ILIKE '%wind%'
   or datatype_name ILIKE '%solar%'
order by datatype_name;

select distinct datatypeid, objectid
from yes_energy__sample_data.yes_energy_sample.ts_forecast_sample
order by datatypeid;

select datatypeid, seriesname, description
from yes_energy__sample_data.yes_energy_sample.v_data_catalog_sample
where seriesname ilike '%forecast%';

create or replace table stg_real_load AS
SELECT objectid, 
datetime,
rtload as load_gen
from yes_energy__sample_data.yes_energy_sample.dart_loads_sample
where objectid = (10000712973);

select * from stg_real_load limit 50;

create or replace table stg_real_wind AS
SELECT objectid, 
trunc(dateadd('second', 3599, datetime), 'hour') as datetime,
avg(value) as wind_gen
from yes_energy__sample_data.yes_energy_sample.ts_gen_sample
where datatypeid = 16
and objectid = 10000712973
group by objectid, trunc(dateadd('second', 3599, datetime), 'hour');

select * from stg_real_wind limit 50;

create or replace table stg_real_solar AS
SELECT objectid, 
trunc(dateadd('second', 3599, datetime), 'hour') as datetime,
avg(value) as solar_gen
from yes_energy__sample_data.yes_energy_sample.ts_gen_sample
where datatypeid = 650
and objectid = 10000712973
group by objectid, trunc(dateadd('second', 3599, datetime), 'hour');

select * from stg_real_solar limit 50;

