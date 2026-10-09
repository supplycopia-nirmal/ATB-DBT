{{ config(materialized='table') }}

with po as (
    select * from {{ ref('stg_po_v4') }}
),

im as (
    select * from {{ ref('int_item_master_enriched_v4') }}
),

con as (
    select * from {{ ref('stg_contracts_v4') }}
),

inv as (
    select * from {{ ref('stg_invoice_v4') }}
),

inv_agg as (
    select
        po_number,
        po_line_no,
        count(distinct invoice_number) as invoice_count,
        sum(invoice_qty) as total_invoiced_qty,
        sum(invoice_total_value) as total_invoiced_amount,
        min(invoice_paid_date) as first_invoice_paid_date,
        max(invoice_paid_date) as last_invoice_paid_date
    from inv
    group by po_number, po_line_no
),

-- Match PO to contract by item_id & vendor_name with active date check
po_con as (
    select
        p.*,
        c.contract_number as matched_contract_number,
        c.contract_price as matched_contract_price,
        c.contract_ea_price as matched_contract_ea_price,
        c.contract_uom as matched_contract_uom,
        c.contract_start_date as matched_contract_start_date,
        c.contract_end_date as matched_contract_end_date,
        c.contract_category,
        row_number() over (
            partition by p.po_number, p.po_line_no
            order by c.contract_end_date desc nulls last
        ) as _rn
    from po p
    left join con c
        on p.item_id = c.item_id
       and (p.po_date between c.contract_start_date and coalesce(c.contract_end_date, cast('2099-12-31' as timestamp)))
),

joined as (
    select
        pc.*,
        im.custom_category as product_class,
        im.product_subclass,
        im.final_unspsc_description as unspsc_description,
        im.unspsc_code as im_unspsc,
        im.data_quality_score,
        ia.invoice_count,
        ia.total_invoiced_qty,
        ia.total_invoiced_amount,
        ia.first_invoice_paid_date,
        ia.last_invoice_paid_date,
        case when pc.matched_contract_number is not null then true else false end as is_contract_matched,
        {{ price_variance2('pc.unit_price', 'pc.matched_contract_price', 'pc.matched_contract_ea_price', "case when pc.uom = pc.matched_contract_uom then 'Y' else 'N' end", 'pc.uom_conv_factor', 'pc.quantity') }} as price_variance2
    from po_con pc
    left join im on pc.item_id = im.item_id
    left join inv_agg ia on pc.po_number = ia.po_number and pc.po_line_no = ia.po_line_no
    where pc._rn = 1
),

finalized as (
    select
        j.*,
        case when j.price_variance2 > 0 then j.price_variance2 else 0.0 end as overpayment_amount,
        case 
            when j.is_contract_matched and j.price_variance2 > 0 then j.price_variance2
            when not j.is_contract_matched then coalesce(j.total_value, 0.0) * 0.15
            else 0.0
        end as savings_opportunity,
        case 
            when j.is_contract_matched and abs(coalesce(j.price_variance2, 0.0)) <= {{ var('price_variance_threshold') }} * coalesce(j.total_value, 1.0) then true
            else false
        end as is_contract_compliant,

        -- 17 Explicit Dashboard Parity Columns
        j.matched_contract_price as contract_price,
        j.matched_contract_start_date as contract_start,
        j.matched_contract_end_date as contract_end,
        j.matched_contract_uom as contract_uom,
        case when j.uom = j.matched_contract_uom then 'Y' else 'N' end as contract_uom_matches_po_uom,
        case when j.is_contract_matched then 'On contract' else 'Off contract' end as contract_status,
        case when j.is_contract_matched then 'Y' else 'N' end as has_current_contract,
        current_date as current_contract_as_of,
        j.matched_contract_number as current_contract_number,
        j.matched_contract_price as current_contract_price,
        j.matched_contract_uom as current_contract_uom,
        j.matched_contract_start_date as current_contract_start,
        j.matched_contract_end_date as current_contract_end,
        case when j.uom = j.matched_contract_uom then 'Y' else 'N' end as current_contract_uom_matches_po_uom
    from joined j
)

select * from finalized
