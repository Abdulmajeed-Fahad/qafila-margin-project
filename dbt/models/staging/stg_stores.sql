-- One row per store, with the city used in the margin report.
select store_id, city
from {{ source('raw', 'stores') }}
