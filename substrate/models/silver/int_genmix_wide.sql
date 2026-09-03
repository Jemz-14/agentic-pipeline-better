with pivoted as (

    select
        period_start,
        region_id,

        max(case when fuel = 'biomass' then perc end) as biomass_perc,
        max(case when fuel = 'coal'    then perc end) as coal_perc,
        max(case when fuel = 'gas'     then perc end) as gas_perc,
        max(case when fuel = 'hydro'   then perc end) as hydro_perc,
        max(case when fuel = 'imports' then perc end) as imports_perc,
        max(case when fuel = 'nuclear' then perc end) as nuclear_perc,
        max(case when fuel = 'other'   then perc end) as other_perc,
        max(case when fuel = 'solar'   then perc end) as solar_perc,
        max(case when fuel = 'wind'    then perc end) as wind_perc,

        count(*)           as fuel_count,
        sum(perc)          as total_perc,
        max(_loaded_at)    as _loaded_at

    from {{ ref('stg_regional_genmix') }}
    group by period_start, region_id

)

select
    *,
    solar_perc + wind_perc + hydro_perc as renewable_perc
from pivoted