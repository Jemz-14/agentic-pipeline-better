-- Guards the region_type filter. If it is ever removed, aggregate regions
-- leak in and every downstream total roughly doubles.
select
    h.region_id,
    r.region_type
from {{ ref('int_region_halfhourly') }} as h
inner join {{ ref('stg_regions') }} as r
    on h.region_id = r.region_id
where r.region_type != 'dno'