{% macro clean_string(column_name) %}
    case 
        when {{ column_name }} is null or trim(cast({{ column_name }} as varchar)) in ('', 'NULL', 'null', 'N/A', 'NA', 'none', 'NONE') 
        then null 
        else regexp_replace(trim(cast({{ column_name }} as varchar)), '\s+', ' ', 'g')
    end
{% endmacro %}

{% macro clean_upper(column_name) %}
    case 
        when {{ column_name }} is null or trim(cast({{ column_name }} as varchar)) in ('', 'NULL', 'null', 'N/A', 'NA', 'none', 'NONE') 
        then null 
        else upper(regexp_replace(trim(cast({{ column_name }} as varchar)), '\s+', ' ', 'g'))
    end
{% endmacro %}
