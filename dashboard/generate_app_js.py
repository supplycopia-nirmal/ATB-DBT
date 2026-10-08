with open('app.js', 'r') as f:
    content = f.read()

old_block = """                    if (tableName === 'transformed') {
                        const tailCols = ['total_quantity', 'supply_unit_price', 'con_contract_ea_price', 'im_contract_price', 'total_acquisition_cost', 'price_variance', 'is_off_contract'];
                        const startCols = resData.columns.filter(c => !tailCols.includes(c));
                        const endCols = tailCols.filter(c => resData.columns.includes(c));
                        resData.columns = [...startCols, ...endCols];
                    }

                    const columns = resData.columns.map(col => {
                        let def = { data: col, title: col, defaultContent: "NULL" };
                        if (transformedColNames.includes(col) || tableName === 'transformed') {
                            if (transformedColNames.includes(col)) {
                                def.className = "transformed-col";
                                def.title = `✨ ${col}`;
                            }
                        }
                        return def;
                    });"""

new_block = """                    let tailCols = [];
                    if (tableName === 'transformed') {
                        tailCols = [
                            'total_quantity',
                            'consumption_uom', 'consumption_unit_price', 'consumption_contract_price',
                            'po_uom', 'po_unit_price',
                            'inv_invoice_uom', 'inv_invoice_unit_price', 'inv_po_uom', 'inv_po_unit_price',
                            'im_contract_uom', 'im_contract_price', 'im_im_unit_contract_price',
                            'con_contract_uom', 'con_contract_price', 'con_contract_ea_price',
                            'price_variance'
                        ];
                        const allTailCols = [...tailCols, 'is_off_contract'];
                        const startCols = resData.columns.filter(c => !allTailCols.includes(c));
                        const endCols = allTailCols.filter(c => resData.columns.includes(c));
                        resData.columns = [...startCols, ...endCols];
                    }

                    const columns = resData.columns.map(col => {
                        let def = { data: col, title: col, defaultContent: "NULL" };
                        if (transformedColNames.includes(col)) {
                            def.className = "transformed-col";
                            def.title = `✨ ${col}`;
                        } else if (tableName === 'transformed' && tailCols.includes(col)) {
                            def.className = "price-variance-col";
                            def.title = `💲 ${col}`;
                        }
                        return def;
                    });"""

if old_block in content:
    with open('app.js', 'w') as f:
        f.write(content.replace(old_block, new_block))
    print("Success")
else:
    print("Not found")
