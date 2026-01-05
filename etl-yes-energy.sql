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

LOAD(dim tabulky a ich naplnenie)

create or replace table dim_datetime(
id int autoincrement primary key,
datetime datetime,
marketday date,
hour int,
day int,
month int,
year int,
onpeak boolean,
offpeak boolean,
wepeak boolean,
wdpeak boolean
);

insert into dim_datetime(
datetime,
marketday,
hour,
day,
month,
year,
onpeak,
offpeak,
wepeak,
wdpeak
)
select distinct
datetime,
marketday,
extract(hour FROM datetime) as hour,
extract(day FROM datetime) as day,
extract(month FROM datetime) as month,
extract(year FROM datetime) as year,
onpeak,
offpeak,
wepeak,
wdpeak
FROM stg_times;

select * from dim_datetime order by id limit 10;

create or replace table dim_price_nodes(
id int autoincrement primary key,
objectid int,
objectname varchar,
zone varchar
);

merge into dim_price_nodes tgt
using(
select distinct
objectid,
objectname,
zone
from stg_nodes
where objecttype = 'price_node'
) src
on tgt.objectid = src.objectid

when matched then
update set
tgt.objectname = src.objectname,
tgt.zone = src.zone

when not matched then
insert(
objectid,
objectname,
zone
)
values(
src.objectid,
src.objectname,
src.zone
);

select * from dim_price_nodes limit 10;

create or replace table dim_load_zone(
id int autoincrement primary key,
objectid int,
objectname varchar,
zone varchar,
valid_from date,
valid_to date,
is_current boolean
);

merge into dim_load_zone tgt
using(
select distinct
objectid,
objectname,
zone
from stg_nodes
where objecttype = 'load_zone'
) src 
on tgt.objectid = src.objectid
and tgt.is_current = true 

when matched and(
tgt.objectname != src.objectname
or tgt.zone != src.zone)
then
update set 
tgt.valid_to = current_timestamp(),
tgt.is_current = false;


insert into dim_load_zone(
id,
objectid,
objectname,
zone,
valid_from,
valid_to,
is_current
)
select 
row_number() over(order by src.objectid) as id,
src.objectid,
src.objectname,
src.zone,
current_timestamp(),
null,
true
from(
select distinct 
objectid,
objectname,
zone
from stg_nodes
where objecttype = 'load_zone'
) src
left join dim_load_zone tgt
on src.objectid = tgt.objectid and tgt.is_current = true
where tgt.objectid is null
or tgt.objectname != src.objectname
or tgt.zone != src.zone;


select * from dim_load_zone limit 10;

create or replace table dim_generation_type(
id int autoincrement primary key,
generation_type varchar
);

insert into dim_generation_type(generation_type)
values
('Wind'),
('Solar'),
('Load');

select * from dim_generation_type;

