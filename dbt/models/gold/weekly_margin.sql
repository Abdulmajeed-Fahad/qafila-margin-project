-- Weekly gross margin: one row per ISO week x city x category that has at least one counted line.
--
-- Counted line = delivered order, quantity > 0, order date (Riyadh) on or after 2026-08-31 (first supplier week).
-- Each line is costed with the cost version that was valid ON ITS ORDER DATE (point-in-time join):
--     valid_from <= order_date < valid_to      (valid_to is empty for the current version)
-- A line with no valid version is "uncosted": it counts in lines and revenue, but not in the cost columns.
with lines as (
    select
        o.order_date,
        week(o.order_date)                                 as iso_week,
        s.city,
        p.category,
        p.sku,
        i.quantity,
        round(i.quantity * i.unit_price - i.discount, 2)   as line_revenue
    from {{ ref('stg_order_items') }} i
    join {{ ref('stg_orders') }} o         on o.order_id = i.order_id
    left join {{ ref('stg_products') }} p  on p.product_id = i.product_id
    left join {{ ref('stg_stores') }} s    on s.store_id = o.store_id
    where o.status = 'delivered'
      and i.quantity > 0
      and o.order_date >= date '2026-08-31'
),

costed as (
    select
        l.*,
        h.unit_cost_sar,
        l.quantity * h.unit_cost_sar                       as line_cost
    from lines l
    left join {{ ref('product_cost_history') }} h
           on h.sku = l.sku
          and l.order_date >= h.valid_from
          and (h.valid_to is null or l.order_date < h.valid_to)
),

summed as (
    select
        iso_week,
        city,
        category,
        count(*)                                                       as lines,
        round(sum(line_revenue), 2)                                    as revenue_sar,
        round(coalesce(sum(line_revenue) filter (where unit_cost_sar is not null), 0), 2) as costed_revenue_sar,
        round(coalesce(sum(line_cost), 0), 2)                          as cost_sar,
        count(*) filter (where unit_cost_sar is null)                  as uncosted_lines
    from costed
    group by iso_week, city, category
)

select
    iso_week::integer                                      as iso_week,
    city,
    category,
    lines::integer                                         as lines,
    revenue_sar,
    costed_revenue_sar,
    cost_sar,
    round(costed_revenue_sar - cost_sar, 2)                as margin_sar,
    uncosted_lines::integer                                as uncosted_lines
from summed
