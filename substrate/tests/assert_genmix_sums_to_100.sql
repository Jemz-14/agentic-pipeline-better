-- The API rounds fuel percentages to 1dp, so real sums land in 99.8-100.2.
-- Tolerance is 0.5; anything wider means rows were added or dropped.
select
    period_start,
    region_id,
    total_perc
from {{ ref('int_genmix_wide') }}
where abs(total_perc - 100) > 0.5