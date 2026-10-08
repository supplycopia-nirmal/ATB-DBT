{% macro safe_cast(column_name, target_type) %}
    try_cast({{ column_name }} as {{ target_type }})
{% endmacro %}
