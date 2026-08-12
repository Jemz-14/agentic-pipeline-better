select
    period_start,
    regionid as region_id,
    fuel,
    perc,
    _loaded_at
from {{ source('carbon', 'regional_genmix') }}