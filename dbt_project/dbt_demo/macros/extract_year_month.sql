-- This macro, extract_year_month, takes a timestamp_column as input and returns two columns: year and month

{% macro extract_year_month(timestamp_column) %}
    EXTRACT(YEAR FROM {{ timestamp_column }}) AS year,
    EXTRACT(MONTH FROM {{ timestamp_column }}) AS month
{% endmacro %}

-- It is used in the models int_marketing_costs.sql and marketing_sales_report.sql
-- It is used to replace the multiple extract() function calls
-- This is created so that it can be used accross other multiple models.
-- It makes the sql code easy to understand
-- If you need to change how you extract the year and month, you only need to modify the macro, not every model that uses it.