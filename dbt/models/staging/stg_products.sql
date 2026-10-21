-- One row per product. The SKU is trimmed so it can be compared with the supplier files.
select
    product_id,
    {{ clean_sku('sku') }} as sku,
    category
from {{ source('raw', 'products') }}
