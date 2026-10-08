{% macro append_metadata(source_file_var, business_keys) %}
    '{{ var(source_file_var) }}' as _source_file,
    row_number() over () as _source_row_number,
    md5(cast(concat_ws('||', {% for col in business_keys %} coalesce(cast({{ col }} as varchar), '') {% if not loop.last %}, {% endif %}{% endfor %}) as varchar)) as _row_hash,
    current_timestamp as _ingested_at
{% endmacro %}
