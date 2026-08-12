select
    cast(period_start as date)              as intensity_date,
    region_id,
    region_name,

    count(*)                                as period_count,
    round(avg(intensity_forecast), 2)       as avg_intensity,
    min(intensity_forecast)                 as min_intensity,
    max(intensity_forecast)                 as max_intensity,
    round(avg(renewable_perc), 2)           as avg_renewable_perc,

    max(_loaded_at)                         as _loaded_at

from {{ ref('int_region_halfhourly') }}
group by 1, 2, 3