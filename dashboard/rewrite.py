import re

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    content = f.read()

# Fix Contracts
contracts_pattern = r"""(inner join contracts con on c\.item_number = con\.item_id\n\s+QUALIFY row_number\(\) over \(\n\s+partition by c\.consumption_row_id\n\s+order by \n\s+case when try_cast\(c\.admit_date_time as date\) >= try_cast\(con\.contract_start_date as date\) and try_cast\(c\.admit_date_time as date\) <= try_cast\(con\.contract_end_date as date\) then 1 else 2 end,\n\s+case when c\.item_uom = con\.contract_uom then 1 else 2 end,\n\s+try_cast\(con\.contract_start_date as date\) desc\n\s+\) = 1)"""
contracts_replacement = """inner join contracts con on c.item_number = con.item_id
    where try_cast(c.admit_date_time as date) >= try_cast(con.contract_start_date as date) 
      and try_cast(c.admit_date_time as date) <= coalesce(try_cast(con.contract_end_date as date), '2099-12-31')
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by 
            case when c.item_uom = con.contract_uom then 1 else 2 end,
            try_cast(con.contract_start_date as date) desc
    ) = 1"""
content = re.sub(contracts_pattern, contracts_replacement, content, flags=re.MULTILINE)

# Fix Item Master
im_pattern = r"""(inner join item_master im on c\.item_number = im\.item_id\n\s+QUALIFY row_number\(\) over \(\n\s+partition by c\.consumption_row_id\n\s+order by \n\s+case when try_cast\(c\.admit_date_time as date\) >= try_cast\(im\.contract_start_date as date\) and try_cast\(c\.admit_date_time as date\) <= try_cast\(im\.contract_end_date as date\) then 1 else 2 end,\n\s+case when c\.item_uom = im\.contract_uom then 1 else 2 end,\n\s+try_cast\(im\.contract_start_date as date\) desc\n\s+\) = 1)"""
im_replacement = """inner join item_master im on c.item_number = im.item_id
    where try_cast(c.admit_date_time as date) >= try_cast(im.contract_start_date as date) 
      and try_cast(c.admit_date_time as date) <= coalesce(try_cast(im.contract_end_date as date), '2099-12-31')
    QUALIFY row_number() over (
        partition by c.consumption_row_id
        order by 
            case when c.item_uom = im.contract_uom then 1 else 2 end,
            try_cast(im.contract_start_date as date) desc
    ) = 1"""
content = re.sub(im_pattern, im_replacement, content, flags=re.MULTILINE)

# Fix PO
po_pattern = r"""(inner join purchase_orders po on c\.item_number = po\.item_id\n\s+where try_cast\(po\.po_date as date\) <= try_cast\(c\.admit_date_time as date\)\n\s+QUALIFY row_number\(\) over \(partition by c\.consumption_row_id order by try_cast\(po\.po_date as date\) desc\) = 1)"""
po_replacement = """inner join purchase_orders po on c.item_number = po.item_id
    where abs(date_diff('day', try_cast(po.po_date as date), try_cast(c.admit_date_time as date))) <= 180
    QUALIFY row_number() over (
        partition by c.consumption_row_id 
        order by abs(date_diff('day', try_cast(po.po_date as date), try_cast(c.admit_date_time as date))) asc
    ) = 1"""
content = re.sub(po_pattern, po_replacement, content, flags=re.MULTILINE)

# Fix Invoices
inv_pattern = r"""(inner join invoices inv on c\.item_number = inv\.item_id\n\s+where coalesce\(try_cast\(inv\.invoice_raised_date as date\), try_cast\(inv\.po_date as date\)\) <= try_cast\(c\.admit_date_time as date\)\n\s+QUALIFY row_number\(\) over \(partition by c\.consumption_row_id order by coalesce\(try_cast\(inv\.invoice_raised_date as date\), try_cast\(inv\.po_date as date\)\) desc\) = 1)"""
inv_replacement = """inner join invoices inv on c.item_number = inv.item_id
    where abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(c.admit_date_time as date))) <= 180
    QUALIFY row_number() over (
        partition by c.consumption_row_id 
        order by abs(date_diff('day', coalesce(try_cast(inv.invoice_raised_date as date), try_cast(inv.po_date as date)), try_cast(c.admit_date_time as date))) asc
    ) = 1"""
content = re.sub(inv_pattern, inv_replacement, content, flags=re.MULTILINE)

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(content)

print("Rewrite successful.")
