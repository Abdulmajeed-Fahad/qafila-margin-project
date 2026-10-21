-- SCD2 history of supplier costs: one row per SKU per cost version.
--   valid_from : first day the cost applies         valid_to : first day it no longer applies (empty = current)
--
-- A weekly file only lists the products quoted that week. We don't need to fill the gaps: a version simply
-- lasts until the next *change*, so a product missing from a week keeps its previous cost.
with usable as (                        -- 1. only rows that passed the quarantine rules
    select * from {{ ref('stg_supplier_prices') }}
    where reject_reason is null and valid_from is not null
),

one_per_day as (                        -- 2. same SKU + valid_from in several files: the file name that sorts last wins
    select sku, valid_from, round(unit_cost_sar, 2) as unit_cost_sar
    from usable
    qualify row_number() over (partition by sku, valid_from order by source_file desc, file_row desc) = 1
),

with_previous as (                      -- 3. look at the cost of the SKU's previous quote
    select *, lag(unit_cost_sar) over (partition by sku order by valid_from) as previous_cost
    from one_per_day
),

versions as (                           -- 4. a new version starts only when the cost changes
    select sku, unit_cost_sar, valid_from
    from with_previous
    where previous_cost is null or unit_cost_sar <> previous_cost
)

select
    sku,
    unit_cost_sar,
    valid_from,
    lead(valid_from) over (partition by sku order by valid_from)       as valid_to,        -- the next version's start (exclusive)
    lead(valid_from) over (partition by sku order by valid_from) is null as is_current
from versions
