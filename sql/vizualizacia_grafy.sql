//graf1 - Predikcia spotreby elektrickej energie vs realna spotreba
select
dt.marketday,
avg(fp.load_forecast) as avg_forecast,
avg(fp.rt_load) as avg_real_load
from fact_power_prices fp
join dim_datetime dt on fp.datetime_id = dt.id
where fp.generation_type_id = 135690
group by dt.marketday
order by dt.marketday
limit 200;

//graf2 - Priemerna denna a mesacna volatilita (kolisanie cien v case)
select 
dt.marketday,
avg(fp.daily_dalmp_stddev) as daily_dalmp_stddev,
avg(fp.monthly_dalmp_stddev) as monthly_dalmp_stddev
from fact_power_prices fp
join dim_datetime dt on fp.datetime_id = dt.id
group by marketday
order by marketday
limit 200;


//graf3 - Perrcentualna chybovost predikcie produkcie vetrom
select 
dt.datetime,
wf.value as forecast,
wg.wind_gen as actual,
abs(wg.wind_gen - wf.value) / nullif(wf.value,0)*100 as perc_error
from dim_datetime dt
join stg_load_forecast wf on wf.datetime = dt.datetime and wf.datatypeid = 9285
join stg_real_wind wg on wg.datetime = dt.datetime
order by dt.datetime
limit 800;

//graf4 - Percentualna chybovost predikcie produkcie solarne
select 
dt.datetime,
sf.value as forecast,
sg.solar_gen as actual,
abs(sg.solar_gen - sf.value) / nullif(sf.value,0)*100 as perc_error
from dim_datetime dt
join stg_load_forecast sf on sf.datetime = dt.datetime and sf.datatypeid = 662
join stg_real_solar sg on sg.datetime = dt.datetime
order by dt.datetime
limit 800;

//graf5 - Podiel obnovitelnych zdrojov na celkovej produkcii elektrickej energie
select
dt.datetime,
sum(coalesce(wg.wind_gen_real,0) + coalesce(sg.solar_gen_real,0)) / sum(f.rt_load)*100 as renewables_share_perc,
from dim_datetime dt
left join fact_power_prices f on f.datetime_id = dt.id and f.generation_type_id = 135690
left join fact_power_prices wg on wg.datetime_id = dt.id and wg.generation_type_id = 135688
left join fact_power_prices sg on sg.datetime_id = dt.id and sg.generation_type_id = 135689
group by dt.datetime
order by dt.datetime
limit 800;

//graf6 - Vplyv produkcie obnovitelnymi zdrojmi na volatilitu
select
dt.marketday,
avg(f.daily_dalmp_stddev) as daily_price_volatility,
sum(coalesce(wg.wind_gen_real,0) + coalesce(sg.solar_gen_real,0))
/ nullif(sum(f.rt_load),0)*100 as renewables_share_perc
from fact_power_prices f
join dim_datetime dt on f.datetime_id = dt.id
left join fact_power_prices wg on wg.datetime_id = dt.id and wg.generation_type_id = 135688
left join fact_power_prices sg on sg.datetime_id = dt.id and sg.generation_type_id = 135689
where f.generation_type_id = 135690
group by dt.marketday
order by dt.marketday
limit 200;
