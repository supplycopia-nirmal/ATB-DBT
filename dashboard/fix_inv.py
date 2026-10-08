with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'r') as f:
    content = f.read()

# We need to only replace the po_uom inside mapped_inv
start = content.find('mapped_inv as')
end = content.find('select\n    c."SURGICAL_HIERARCHY"', start)
mapped_inv_text = content[start:end]

mapped_inv_text = mapped_inv_text.replace("c.item_uom = po_uom", "c.item_uom = inv_Invoice_uom")

content = content[:start] + mapped_inv_text + content[end:]

with open('/Users/nirmalrayan/Documents/UC_Health/uc_health/models/marts/fct_cost_savings_analysis.sql', 'w') as f:
    f.write(content)
