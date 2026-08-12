select
    regionid    as region_id,
    shortname   as region_name,
    dnoregion   as dno_region,
    region_type
from {{ ref('regions') }}