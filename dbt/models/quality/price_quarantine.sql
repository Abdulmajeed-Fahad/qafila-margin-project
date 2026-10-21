-- One row per rejected supplier row: the file it came from, and why we didn't use it.
-- (Rows stay here even after a later file corrects them: they were rejected when we read the file.)
select
    source_file,
    sku,
    valid_from,
    raw_cost,
    reject_reason as reason
from {{ ref('stg_supplier_prices') }}
where reject_reason is not null
