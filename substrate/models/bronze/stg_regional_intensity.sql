select
    period_start,
    period_end,
    regionid as region_id,
    intensity_forecast,
    intensity_index,
    _loaded_at
from {{ source('carbon', 'regional_intensity') }}