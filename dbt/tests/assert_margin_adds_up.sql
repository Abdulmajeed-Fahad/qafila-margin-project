-- margin = costed revenue - cost, costed revenue can't exceed revenue, uncosted lines can't exceed lines.
select * from {{ ref('weekly_margin') }}
where abs(costed_revenue_sar - cost_sar - margin_sar) > 0.011
   or costed_revenue_sar > revenue_sar + 0.01
   or uncosted_lines > lines
