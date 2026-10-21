{# Generic test: every value must be greater than zero. Returns the offending rows. #}
{% test positive(model, column_name) %}
select * from {{ model }} where {{ column_name }} is null or {{ column_name }} <= 0
{% endtest %}
