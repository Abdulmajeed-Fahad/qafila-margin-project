-- Each version must end exactly where the next one starts (no gaps, no overlaps),
-- and a new version may only start when the cost really changed.
with ordered as (
    select
        sku, unit_cost_sar, valid_from, valid_to,
        lead(valid_from)    over (partition by sku order by valid_from) as next_from,
        lag(unit_cost_sar)  over (partition by sku order by valid_from) as previous_cost
    from {{ ref('product_cost_history') }}
)
select * from ordered
where (next_from is not null and valid_to is distinct from next_from)
   or unit_cost_sar = previous_cost
