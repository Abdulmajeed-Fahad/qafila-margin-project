-- One row per supplier price row, cleaned but NOT filtered: bad rows stay here with a reject_reason,
-- and the quarantine model and the cost history both read from this one place.
--   * the cost sits in unit_cost (weeks 36-38) or cost_sar (week 39 on)
--   * SKU is trimmed; valid_from becomes a date
--   * reject_reason: unknown_sku first, otherwise invalid_cost, otherwise empty (the row is usable)
with prices as (
    select
        source_file,
        file_row,
        {{ clean_sku('sku') }}                      as sku,
        try_cast(valid_from as date)                 as valid_from,
        coalesce(unit_cost, cost_sar)                as raw_cost
    from {{ source('raw', 'supplier_prices') }}
),

parsed as (
    select *, {{ parse_cost('raw_cost') }} as unit_cost_sar from prices
)

select
    source_file,
    file_row,
    sku,
    valid_from,
    raw_cost,
    unit_cost_sar,
    case
        when sku not in (select sku from {{ ref('stg_products') }})        then 'unknown_sku'
        when unit_cost_sar is null or not isfinite(unit_cost_sar) or unit_cost_sar <= 0 then 'invalid_cost'
    end as reject_reason
from parsed
