{% macro fix_double_at(column_name) %}
    replace({{ column_name }}, '@@', '@')
{% endmacro %}
