{% macro deduplicate(relation, partition_by, order_by) %}
    select * from (
        select 
            *,
            row_number() over (
                partition by {{ partition_by }}
                order by {{ order_by }}
            ) as _rn
        from {{ relation }}
    )
    where _rn = 1
{% endmacro %}
