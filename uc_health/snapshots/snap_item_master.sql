{% snapshot snap_item_master %}

{{
    config(
      target_schema='main',
      unique_key='item_id',
      strategy='check',
      check_cols=['contract_price', 'contract_number', 'is_active', 'unspsc_code'],
    )
}}

select * from {{ ref('stg_item_master_v4') }}

{% endsnapshot %}
