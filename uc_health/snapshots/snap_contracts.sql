{% snapshot snap_contracts %}

{{
    config(
      target_schema='main',
      unique_key='contract_number || \'_\' || item_id',
      strategy='check',
      check_cols=['contract_price', 'contract_ea_price', 'contract_end_date', 'pricing_tier'],
    )
}}

select * from {{ ref('stg_contracts_v4') }}

{% endsnapshot %}
