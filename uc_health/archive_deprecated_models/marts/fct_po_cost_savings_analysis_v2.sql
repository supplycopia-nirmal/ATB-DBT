{{ config(materialized='table') }}

with purchase_orders as (
    select *, row_number() over() as po_row_id 
    from {{ ref('stg_purchase_orders') }}
    where item_id is not null
),
invoices as (
    select *, row_number() over() as __inv_id
    from (
        select *, row_number() over(partition by item_id, "Invoice_uom", date_trunc('month', coalesce(try_cast(invoice_raised_date as date), try_cast(po_date as date))) order by coalesce(try_cast(invoice_raised_date as date), try_cast(po_date as date)) desc) as rn
        from {{ ref('stg_invoice') }}
        where item_id is not null
    ) where rn = 1
),
item_master as ( select *, row_number() over() as __im_id from {{ ref('int_item_master_profiled') }} ),
contracts as ( select *, row_number() over() as __con_id from {{ ref('stg_contracts') }} ),

mapped_contracts as (
    select po.po_row_id, con.__con_id
    from purchase_orders po
    inner join contracts con on po.item_id = con.item_id
    where (try_cast(po.po_date as date) >= try_cast(con.contract_start_date as date) and try_cast(po.po_date as date) <= coalesce(try_cast(con.contract_end_date as date), '2099-12-31')) AND po.uom = con.contract_uom
    QUALIFY row_number() over (
        partition by po.po_row_id
        order by abs(date_diff('day', try_cast(con.contract_start_date as date), try_cast(po.po_date as date))) asc
    ) = 1
),

mapped_item_master as (
    select po.po_row_id, im.__im_id
    from purchase_orders po
    inner join item_master im on po.item_id = im.item_id
    where (try_cast(po.po_date as date) >= try_cast(im.contract_start_date as date) and try_cast(po.po_date as date) <= coalesce(try_cast(im.contract_end_date as date), '2099-12-31')) AND po.uom = im.contract_uom
    QUALIFY row_number() over (
        partition by po.po_row_id
        order by abs(date_diff('day', try_cast(im.contract_start_date as date), try_cast(po.po_date as date))) asc
    ) = 1
),

mapped_inv as (
    select po.po_row_id, inv.__inv_id
    from purchase_orders po
    inner join invoices inv on po.item_id = inv.item_id
    where (coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) >= try_cast(po.po_date as date) - interval 180 day and coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)) <= try_cast(po.po_date as date) + interval 180 day) AND po.uom = inv."Invoice_uom"
    QUALIFY row_number() over (
        partition by po.po_row_id
        order by abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(po.po_date as date))) asc
    ) = 1
)

select
    po.* EXCLUDE (po_row_id),
    -- ITEM MASTER
    im."packaging_string" as im_packaging_string,
    im."manufacture_id" as im_manufacture_id,
    im."brand_name" as im_brand_name,
    im."latex" as im_latex,
    im."ndc" as im_ndc,
    im."contract_qoe" as im_contract_qoe,
    im."hcpcs" as im_hcpcs,
    im."item_description" as im_item_description,
    im."mfr_part_number" as im_mfr_part_number,
    im."mfr_name" as im_mfr_name,
    im."vendor_name" as im_vendor_name,
    im."vendor_part_number" as im_vendor_part_number,
    im."vendor_code" as im_vendor_code,
    im."contract_number" as im_contract_number,
    im."contract_description" as im_contract_description,
    im."contract_start_date" as im_contract_start_date,
    im."contract_end_date" as im_contract_end_date,
    im."unspsc_code" as im_unspsc_code,
    im."unspsc_description" as im_unspsc_description,
    im."is_active" as im_is_active,
    im."is_missing_vendor_code" as im_is_missing_vendor_code,
    im."is_missing_mfr_name" as im_is_missing_mfr_name,
    im."custom_category" as im_custom_category,
    im."data_quality_score" as im_data_quality_score,
    -- INVOICES
    inv."Invoice_uom_conv_factor" as inv_Invoice_uom_conv_factor,
    inv."po_date" as inv_po_date,
    inv."facility_entity_code" as inv_facility_entity_code,
    inv."po_uom_conv_factor" as inv_po_uom_conv_factor,
    inv."po_qty" as inv_po_qty,
    inv."po_total_value" as inv_po_total_value,
    inv."invoice_number" as inv_invoice_number,
    inv."invoice_line_number" as inv_invoice_line_number,
    inv."invoice_paid_date" as inv_invoice_paid_date,
    inv."invoice_raised_date" as inv_invoice_raised_date,
    inv."invoice_payable_date" as inv_invoice_payable_date,
    inv."invoice_qty" as inv_invoice_qty,
    inv."invoice_total_value" as inv_invoice_total_value,
    inv."po_number" as inv_po_number,
    inv."po_line_no" as inv_po_line_no,
    inv."facility_name" as inv_facility_name,
    inv."item_description" as inv_item_description,
    -- CONTRACTS
    con."contract_number" as con_contract_number,
    con."contract_description" as con_contract_description,
    con."contract_start_date" as con_contract_start_date,
    con."contract_end_date" as con_contract_end_date,
    con."contract_qoe" as con_contract_qoe,
    con."item_description" as con_item_description,
    con."manufacturer_part_number" as con_manufacturer_part_number,
    con."manufacture_name" as con_manufacture_name,
    con."vendor_name" as con_vendor_name,
    con."contract_category" as con_contract_category,
    con."list_price" as con_list_price,
    con."pricing_tier" as con_pricing_tier,
    con."tier_requirements" as con_tier_requirements,

    inv.Invoice_uom as inv_invoice_uom,
    inv.invoice_unit_price as inv_invoice_unit_price,
    inv.po_uom as inv_po_uom,
    inv.po_unit_price as inv_po_unit_price,

    im.contract_uom as im_contract_uom,
    im.contract_price as im_contract_price,
    im.im_unit_contract_price as im_im_unit_contract_price,

    con.contract_uom as con_contract_uom,
    con.contract_price as con_contract_price,
    con.contract_ea_price as con_contract_ea_price,

    ROUND(
        case
            when try_cast(con.contract_price as double) > 0 then
                (try_cast(po.unit_price as double) - coalesce(try_cast(case when po.uom = con.contract_uom then con.contract_price else con.contract_ea_price end as double), 0)) * try_cast(po.quantity as double)
            when try_cast(im.contract_price as double) > 0 then
                (try_cast(po.unit_price as double) - coalesce(try_cast(case when po.uom = im.contract_uom then im.contract_price else im.im_unit_contract_price end as double), 0)) * try_cast(po.quantity as double)
            else 0
        end,
    2) as price_variance

from purchase_orders po
left join mapped_contracts m_con on po.po_row_id = m_con.po_row_id
left join contracts con on m_con.__con_id = con.__con_id
left join mapped_item_master m_im on po.po_row_id = m_im.po_row_id
left join item_master im on m_im.__im_id = im.__im_id
left join mapped_inv m_inv on po.po_row_id = m_inv.po_row_id
left join invoices inv on m_inv.__inv_id = inv.__inv_id
