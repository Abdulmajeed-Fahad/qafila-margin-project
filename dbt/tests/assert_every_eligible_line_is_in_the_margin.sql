-- Reconciliation: the margin table must count exactly the order lines the rules make eligible (nothing lost, nothing doubled).
with expected as (
    select count(*) as lines
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o on o.order_id = i.order_id
    where o.status = 'delivered' and i.quantity > 0 and o.order_date >= date '2026-08-31'
),
actual as (select coalesce(sum(lines), 0) as lines from {{ ref('weekly_margin') }})
select expected.lines as expected_lines, actual.lines as actual_lines
from expected, actual
where expected.lines <> actual.lines
