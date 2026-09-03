select
    period_start,
    period_end,
    intensity_forecast,
    intensity_actual,
    intensity_index,
    _loaded_at
from {{ source('carbon', 'national_intensity') }}