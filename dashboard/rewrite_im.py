with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    content = f.read()

im_pattern = """    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by
            case when c.item_uom = im.contract_uom then 1 else 2 end,
            try_cast(im.contract_start_date as date) desc
    ) = 1
),

mapped_po"""

im_replacement = """    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by
            case when try_cast(c.admit_date_time as date) >= try_cast(im.contract_start_date as date) and try_cast(c.admit_date_time as date) <= coalesce(try_cast(im.contract_end_date as date), '2099-12-31') then 1 else 2 end,
            case when c.item_uom = im.contract_uom then 1 else 2 end,
            try_cast(im.contract_start_date as date) desc
    ) = 1
),

mapped_po"""

content = content.replace(im_pattern, im_replacement)

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(content)
