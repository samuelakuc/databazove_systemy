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
