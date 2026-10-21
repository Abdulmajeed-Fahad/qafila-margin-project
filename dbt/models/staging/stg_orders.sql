-- One row per order. created_at is UTC text; Riyadh is always UTC+3 (no daylight saving),
-- so the Riyadh order date is the UTC timestamp plus 3 hours, cut down to a date.
select
    order_id,
    store_id,
    status,
    cast(created_at::timestamp + interval 3 hour as date) as order_date
from {{ source('raw', 'orders') }}
