{% macro clean_email_chars(column_name) %}
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace({{ column_name }}, '[#%]', ''),
            '#example', '@example'),
        '_com', '.com'),
    'example.com$', '@example.com')
{% endmacro %}
