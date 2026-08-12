with national as (

    select
        cast(period_start as date)                                as intensity_date,
        count(*)                                                  as period_count,
        round(avg(intensity_forecast), 2)                         as avg_national_forecast,
        round(avg(intensity_actual), 2)                           as avg_national_actual,
        round(avg(abs(intensity_forecast - intensity_actual)), 2) as avg_forecast_error,
        max(_loaded_at)                                           as _loaded_at

    from {{ ref('stg_national_intensity') }}
    group by 1

),

regional as (

    -- Unweighted mean across DNO regions, for comparison against the settled
    -- national series. int_region_halfhourly is already filtered to
    -- region_type = 'dno'. Sourcing from stg_regional_intensity instead would
    -- pull in England/Scotland/Wales/GB, which are aggregates of the same
    -- regions, and inflate region_count from 14 to 18.
    select
        cast(period_start as date)          as intensity_date,
        count(distinct region_id)           as region_count,
        round(avg(intensity_forecast), 2)   as avg_regional_forecast

    from {{ ref('int_region_halfhourly') }}
    group by 1

)

select
    n.intensity_date,
    n.period_count,
    n.avg_national_forecast,
    n.avg_national_actual,
    n.avg_forecast_error,
    r.region_count,
    r.avg_regional_forecast,
    n._loaded_at

from national as n
inner join regional as r
    on n.intensity_date = r.intensity_date