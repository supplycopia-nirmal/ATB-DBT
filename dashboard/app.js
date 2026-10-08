// Utility for formatting currency
const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 0
    }).format(value);
};

// Utility for formatting numbers
const formatNumber = (value) => {
    return new Intl.NumberFormat('en-US').format(value);
};

// Main function to load and render data
async function initDashboard() {
    try {
        const cacheBuster = '?t=' + new Date().getTime();
        const response = await fetch('/api/metrics' + cacheBuster);
        const data = await response.json();

        // 1. Populate Metrics
        document.getElementById('metric-savings').innerText = formatCurrency(data.savings.total_variance);
        document.getElementById('metric-savings-po-v2').innerText = formatCurrency(data.savings.total_variance_po_v2);
        document.getElementById('metric-savings-cons-v2').innerText = formatCurrency(data.savings.total_variance_cons_v2);
        document.getElementById('metric-savings-po-v3').innerText = formatCurrency(data.savings.total_variance_po);
        document.getElementById('metric-savings-cons-v3').innerText = formatCurrency(data.savings.total_variance_cons);
        if (document.getElementById('metric-savings-po-v4')) {
            document.getElementById('metric-savings-po-v4').innerText = formatCurrency(data.savings.total_variance_po_v4 || 0);
        }
        if (document.getElementById('metric-savings-cons-v4')) {
            document.getElementById('metric-savings-cons-v4').innerText = formatCurrency(data.savings.total_variance_cons_v4 || 0);
        }
        if (document.getElementById('metric-opp-cons-v4')) {
            document.getElementById('metric-opp-cons-v4').innerText = formatCurrency(data.savings.total_savings_opp_cons_v4 || 0);
        }

        document.getElementById('metric-off-contract').innerText = formatCurrency(data.savings.off_contract_spend);
        document.getElementById('metric-dq').innerText = `${data.data_quality.average_score} / 100`;
        document.getElementById('metric-missing-vendors').innerText = formatNumber(data.data_quality.missing_vendors);

        // 2. Populate Raw Stats
        
        
        
        // Load initial raw stats and filter options
        fetch('/api/filter_options').then(r => r.json()).then(data => {
            const consSel = document.getElementById('cons-filter');
            data.cons_facilities.forEach(f => {
                const opt = document.createElement('option');
                opt.value = f; opt.innerText = f;
                consSel.appendChild(opt);
            });
            const poSel = document.getElementById('po-filter');
            data.po_facilities.forEach(f => {
                const opt = document.createElement('option');
                opt.value = f; opt.innerText = f;
                poSel.appendChild(opt);
            });
        });
        
        function updateRawStats() {
            const consVal = document.getElementById('cons-filter').value;
            const poVal = document.getElementById('po-filter').value;
            fetch(`/api/raw_stats?cons_filter=${encodeURIComponent(consVal)}&po_filter=${encodeURIComponent(poVal)}`)
                .then(r => r.json())
                .then(d => {
                    document.getElementById('raw-consumption').innerText = formatNumber(d.consumption.volume);
                    document.getElementById('raw-consumption-val').innerText = formatCurrency(d.consumption.value);
                    document.getElementById('raw-po').innerText = formatNumber(d.purchase_orders.volume);
                    document.getElementById('raw-po-val').innerText = formatCurrency(d.purchase_orders.value);
                });
        }
        updateRawStats();

        document.getElementById('cons-filter').addEventListener('change', () => {
            updateRawStats();
            if ($.fn.DataTable.isDataTable('#raw-consumption-table')) { $('#raw-consumption-table').DataTable().ajax.reload(); }
        });
        document.getElementById('po-filter').addEventListener('change', () => {
            updateRawStats();
            if ($.fn.DataTable.isDataTable('#raw-po-table')) { $('#raw-po-table').DataTable().ajax.reload(); }
        });

        document.getElementById('raw-invoices').innerText = formatNumber(data.raw_counts.invoices);
        document.getElementById('raw-item').innerText = formatNumber(data.raw_counts.item_master);
        document.getElementById('raw-contracts').innerText = formatNumber(data.raw_counts.contracts || 0);
        document.getElementById('metric-contracts').innerText = formatNumber(data.raw_counts.contracts || 0);

        // Common Chart Defaults for Glassmorphism Look
        Chart.defaults.color = '#94a3b8';
        Chart.defaults.font.family = "'Inter', sans-serif";
        const gridLinesColor = 'rgba(255, 255, 255, 0.05)';

        // 3. Render Aggregate Savings Charts
        const renderSavingsChart = async (endpoint, canvasId, label) => {
            const res = await fetch(endpoint);
            const chartData = await res.json();
            const ctx = document.getElementById(canvasId).getContext('2d');
            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: chartData.map(d => d.name || 'Unknown'),
                    datasets: [{
                        label: label,
                        data: chartData.map(d => d.savings),
                        backgroundColor: 'rgba(230, 69, 82, 0.8)',
                        borderRadius: 4,
                        hoverBackgroundColor: '#e64552'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            grid: { color: gridLinesColor, borderColor: 'transparent' },
                            ticks: {
                                callback: function(value) {
                                    return '$' + (value / 1000).toFixed(1) + 'k';
                                }
                            }
                        },
                        x: {
                            grid: { display: false },
                            ticks: {
                                maxRotation: 45,
                                minRotation: 45
                            }
                        }
                    },
                    plugins: {
                        legend: { display: false }
                    }
                }
            });
        };

        // Fire chart fetches concurrently
        renderSavingsChart('/api/savings/by-manufacturer', 'mfrSavingsChart', 'Savings ($)');
        renderSavingsChart('/api/savings/by-vendor', 'vendorSavingsChart', 'Savings ($)');
        renderSavingsChart('/api/savings/by-procedure', 'procedureSavingsChart', 'Savings ($)');

        // 4.5 Fetch and Render Data Health
        try {
            const healthRes = await fetch('/api/data-health');
            const healthData = await healthRes.json();
            
            const renderHealth = (prefix, data) => {
                const datesEl = document.getElementById(`health-${prefix}-dates`);
                const gapsEl = document.getElementById(`health-${prefix}-gaps`);
                if (data.min_date && data.max_date) {
                    datesEl.innerText = `${data.min_date} to ${data.max_date}`;
                    if (data.missing_days > 0) {
                        gapsEl.innerHTML = `<span style="color: #fb7185;">Warning: ${data.missing_days} days missing</span>`;
                    } else {
                        gapsEl.innerHTML = `<span style="color: #34d399;">Continuous (No gaps)</span>`;
                    }
                } else {
                    datesEl.innerText = "No data available";
                    gapsEl.innerText = "";
                }
            };
            
            if(healthData.consumption) renderHealth('cons', healthData.consumption);
            if(healthData.purchase_orders) renderHealth('po', healthData.purchase_orders);
            if(healthData.invoices) renderHealth('inv', healthData.invoices);
            if(healthData.contracts) renderHealth('con', healthData.contracts);
            
        } catch(e) {
            console.error("Failed to fetch data health", e);
        }

        // 5. Initialize DataTables for Raw Data Explorer
        const initDataTable = (tableId, tableName) => {
            // First fetch just 1 row to get the column headers
            fetch(`/api/data?table=${tableName}&start=0&length=1`)
                .then(res => res.json())
                .then(resData => {
                    if (!resData.columns) return;
                    
                    const transformedColNames = ['primary_drg_code', 'primary_procedure_group', 'custom_category', 'is_missing_vendor_code', 'is_missing_mfr_name', 'data_quality_score'];
                    const isSavingsTable = ['transformed', 'po_cost_savings_v2', 'cons_cost_savings_v2', 'po_cost_savings_v3', 'cons_cost_savings_v3', 'po_cost_savings_v4', 'cons_cost_savings_v4', 'gap_analysis_v4'].includes(tableName);

                    
                    let tailCols = [];
                    if (isSavingsTable) {
                        tailCols = [
                            'total_quantity',
                            'consumption_uom', 'consumption_unit_price', 'consumption_contract_price',
                            'po_uom', 'po_unit_price',
                            'inv_invoice_uom', 'inv_invoice_unit_price', 'inv_po_uom', 'inv_po_unit_price',
                            'im_contract_uom', 'im_contract_price', 'im_im_unit_contract_price',
                            'con_contract_uom', 'con_contract_price', 'con_contract_ea_price',
                            'uom', 'unit_price', 'quantity', 'contract_uom_matches_po_uom',
                            'ITEM_UOM', 'supply_unit_price', 'TOTAL_QUANTITY',
                            'current_contract_uom', 'current_contract_price', 'current_contract_ea_price',
                            'price_variance', 'price_variance2'
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
                        } else if (isSavingsTable && tailCols.includes(col)) {
                            def.className = "price-variance-col";
                            def.title = `💲 ${col}`;
                        }
                        return def;
                    });
                    
                    $(`#${tableId}`).DataTable({
                        serverSide: true,
                        processing: true,
                        ajax: {
                            url: `/api/data?table=${tableName}`,
                            data: function(d) {
                                if (tableName === 'consumption') {
                                    d.cons_filter = document.getElementById('cons-filter').value;
                                } else if (tableName === 'purchase_orders') {
                                    d.po_filter = document.getElementById('po-filter').value;
                                }
                            }
                        },
                        columns: columns,
                        scrollX: true,
                        pageLength: 10,
                        lengthMenu: [10, 25, 50, 100],
                        language: {
                            search: "_INPUT_",
                            searchPlaceholder: "Search records..."
                        }
                    });
                })
                .catch(err => console.error("Error initializing DataTable", err));
        };

        // We only initialize the visible one to save resources, others on click
        initDataTable('raw-consumption-table', 'consumption');
        
        let initializedTables = { 'table-consumption': true };

        // 6. Sub-Tab Switching Logic (Raw Data Explorer)
        const tabBtns = document.querySelectorAll('.tab-btn');
        tabBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                // Remove active from all
                document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                
                // Add active to clicked
                btn.classList.add('active');
                const targetId = btn.dataset.target;
                document.getElementById(targetId).classList.add('active');
                
                // Update download link
                const tableMap = {
                    'table-consumption': 'consumption',
                    'table-po': 'purchase_orders',
                    'table-invoices': 'invoices',
                    'table-items': 'item_master',
                    'table-contracts': 'contracts'
                };
                const tableName = tableMap[targetId];
                document.getElementById('raw-download-btn').href = `/api/download?table=${tableName}`;

                // Initialize DataTable if not already done
                if (!initializedTables[targetId]) {
                    const dtMap = {
                        'table-consumption': 'raw-consumption-table',
                        'table-po': 'raw-po-table',
                        'table-invoices': 'raw-invoices-table',
                        'table-items': 'raw-items-table',
                        'table-contracts': 'raw-contracts-table'
                    };
                    initDataTable(dtMap[targetId], tableName);
                    initializedTables[targetId] = true;
                }
            });
        });

        let missingVendorsInitialized = false;
        
        // 7. Main Tab Switching Logic
        const mainTabBtns = document.querySelectorAll('.main-tab-btn');
        mainTabBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.main-tab-btn').forEach(b => {
                    b.classList.remove('active');
                });
                document.querySelectorAll('.main-view').forEach(v => v.style.display = 'none');

                btn.classList.add('active');
                
                const targetId = btn.dataset.target;
                document.getElementById(targetId).style.display = 'block';

                // Initialize Transformed DataTable when Savings Tab is clicked for the first time
                if (targetId === 'view-savings' && !initializedTables['transformed']) {
                    initDataTable('transformed-table', 'transformed');
                    initDataTable('po-v2-table', 'po_cost_savings_v2');
                    initDataTable('cons-v2-table', 'cons_cost_savings_v2');
                    initDataTable('po-v3-table', 'po_cost_savings_v3');
                    initDataTable('cons-v3-table', 'cons_cost_savings_v3');
                    initDataTable('missing-po-v3-table', 'missing_po_v3');
                    initDataTable('missing-cons-v3-table', 'missing_cons_v3');
                    initDataTable('po-v4-table', 'po_cost_savings_v4');
                    initDataTable('cons-v4-table', 'cons_cost_savings_v4');
                    initDataTable('gap-v4-table', 'gap_analysis_v4');

                    initializedTables['transformed'] = true;
                }
                
                // Redraw charts if needed (Chart.js sometimes needs resize when unhidden)
                setTimeout(() => {
                    window.dispatchEvent(new Event('resize'));
                }, 100);
            });
        });

        // 8. Missing Vendors Click Logic
        const mvCard = document.getElementById("missing-vendors-card");
        if (mvCard) {
            mvCard.addEventListener('click', () => {
                if (!missingVendorsInitialized) {
                    // Initialize the missing vendors datatable with the filter
                    const tableName = 'item_master';
                    fetch(`/api/data?table=${tableName}&start=0&length=1`)
                        .then(res => res.json())
                        .then(resData => {
                            if (!resData.columns) return;
                            const columns = resData.columns.map(col => ({ data: col, title: col, defaultContent: "NULL" }));
                            $(`#missing-vendors-table`).DataTable({
                                serverSide: true,
                                processing: true,
                                ajax: `/api/data?table=${tableName}&missing_vendors=true`,
                                columns: columns,
                                scrollX: true,
                                pageLength: 10,
                                lengthMenu: [10, 25, 50, 100],
                                language: {
                                    search: "_INPUT_",
                                    searchPlaceholder: "Search records..."
                                }
                            });
                        })
                        .catch(err => console.error("Error initializing missing vendors table", err));
                        
                    missingVendorsInitialized = true;
                }
            });
        }
        // 9. Generate LLM Insights
        const generateBtn = document.getElementById('generate-insights-btn');
        if (generateBtn) {
            generateBtn.addEventListener('click', async () => {
                const contentDiv = document.getElementById('insights-content');
                contentDiv.style.display = 'block';
                contentDiv.innerHTML = '<div style="display: flex; align-items: center; gap: 10px;"><div class="spinner"></div><span>Ask The Bee is analyzing the data...</span></div>';
                generateBtn.disabled = true;
                generateBtn.style.opacity = '0.5';

                try {
                    const res = await fetch('/api/generate_insights', { method: 'POST' });
                    const data = await res.json();
                    
                    if (data.error) {
                        contentDiv.innerHTML = `<span style="color: var(--error-color);">Error: ${data.error}</span>`;
                    } else {
                        // Use marked to parse markdown
                        contentDiv.innerHTML = marked.parse(data.markdown);
                    }
                } catch (e) {
                    contentDiv.innerHTML = `<span style="color: var(--error-color);">Failed to fetch insights: ${e.message}</span>`;
                } finally {
                    generateBtn.disabled = false;
                    generateBtn.style.opacity = '1';
                }
            });
        }

        // 11. Volume Trend Logic
        let volumeChartInstance = null;
        const vtModal = document.getElementById("volume-trend-modal");
        const vtLoading = document.getElementById("volume-trend-loading");
        const ctxVt = document.getElementById("volumeTrendChart").getContext("2d");

        const healthCards = {
            'health-card-cons': 'consumption',
            'health-card-po': 'po',
            'health-card-inv': 'inv',
            'health-card-con': 'con'
        };

        Object.keys(healthCards).forEach(cardId => {
            const cardEl = document.getElementById(cardId);
            if (cardEl) {
                cardEl.addEventListener('click', async () => {
                    const dataset = healthCards[cardId];
                    vtModal.style.display = "block";
                    vtLoading.style.display = "block";
                    
                    if (volumeChartInstance) {
                        volumeChartInstance.destroy();
                    }
                    
                    try {
                        const response = await fetch(`/api/volume-trend?dataset=${dataset}`);
                        const data = await response.json();
                        
                        if (data.error) throw new Error(data.error);

                        const labels = data.map(d => d.date);
                        const values = data.map(d => d.count);

                        volumeChartInstance = new Chart(ctxVt, {
                            type: 'line',
                            data: {
                                labels: labels,
                                datasets: [{
                                    label: `Data Volume`,
                                    data: values,
                                    borderColor: 'rgba(54, 162, 235, 1)',
                                    backgroundColor: 'rgba(54, 162, 235, 0.2)',
                                    fill: true,
                                    tension: 0.1
                                }]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                scales: {
                                    y: { beginAtZero: true }
                                },
                                plugins: {
                                    zoom: {
                                        pan: {
                                            enabled: true,
                                            mode: 'x'
                                        },
                                        zoom: {
                                            wheel: { enabled: true },
                                            pinch: { enabled: true },
                                            mode: 'x'
                                        }
                                    }
                                }
                            }
                        });
                    } catch (error) {
                        console.error("Failed to load volume trend:", error);
                        alert("Could not load volume trend data.");
                    } finally {
                        vtLoading.style.display = "none";
                    }
                });
            }
        });

    } catch (error) {
        console.error("Failed to load dashboard data:", error);
        alert(`Could not load data: ${error.message}. Please refresh the page or clear your cache.`);
    }
}

// Initialize on load
document.addEventListener('DOMContentLoaded', initDashboard);

    // Robust Savings Math Modal Logic
    $(document).on('click', '.savings-info-icon', function() {
        $('#savings-math-modal').fadeIn(200);
    });
    $(document).on('click', '#close-savings-math', function() {
        $('#savings-math-modal').fadeOut(200);
    });
    $(window).on('click', function(event) {
        if ($(event.target).is('#savings-math-modal')) {
            $('#savings-math-modal').fadeOut(200);
        }
    });
