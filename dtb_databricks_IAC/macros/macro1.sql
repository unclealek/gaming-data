{% macro macro1() %}
    {{ config(materialized='table', severity='warn') }}
{% endmacro %}
