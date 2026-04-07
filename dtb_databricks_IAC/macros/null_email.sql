{% macro null_email(email_col, first_name_col, last_name_col) %}
    case
        when {{ email_col }} is null
            then concat({{ first_name_col }}, '.', {{ last_name_col }}, '@example.com')
        else {{ email_col }}
    end
{% endmacro %}

