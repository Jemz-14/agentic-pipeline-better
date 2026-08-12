select
    i.period_start,
    i.period_end,
    i.region_id,
    r.region_name,
    r.dno_region,

    i.intensity_forecast,
    i.intensity_index,

    g.biomass_perc,
    g.coal_perc,
    g.gas_perc,
    g.hydro_perc,
    g.imports_perc,
    g.nuclear_perc,
    g.other_perc,
    g.solar_perc,
    g.wind_perc,
    g.renewable_perc,
    g.total_perc,

    i._loaded_at

from {{ ref('stg_regional_intensity') }} as i

inner join {{ ref('stg_regions') }} as r
    on i.region_id = r.region_id

inner join {{ ref('int_genmix_wide') }} as g
    on i.period_start = g.period_start
    and i.region_id = g.region_id

-- Regions 15-18 (England, Scotland, Wales, GB) are aggregates of regions 1-14
-- sitting at the same grain key. Without this filter, any national roll-up
-- double-counts every half-hour.
where r.region_type = 'dno'