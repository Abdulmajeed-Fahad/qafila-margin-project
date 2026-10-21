{# Trim whitespace (spaces, tabs, non-breaking spaces) from both ends. Used for every SKU, on both sides of the join. #}
{% macro clean_sku(col) -%}
    regexp_replace({{ col }}, '^[\s\x{00A0}]+|[\s\x{00A0}]+$', '', 'g')
{%- endmacro %}

{# Read a cost typed by a human as a number: "9,49" and "٩٫٤٩" both become 9.49. Anything else becomes NULL. #}
{% macro parse_cost(col) -%}
    try_cast(
        replace(replace(translate(trim({{ col }}), '٠١٢٣٤٥٦٧٨٩٫', '0123456789.'), '٬', ''), ',', '.')
        as double)
{%- endmacro %}
