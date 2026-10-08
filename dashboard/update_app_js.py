with open('app.js', 'r') as f:
    content = f.read()

# Add Math Modal Event Listener
modal_js = """        // Modal for Cost Savings Math
        const savingsIcon = document.getElementById('savings-info-icon');
        const savingsModal = document.getElementById('savings-math-modal');
        const closeSavingsModal = document.getElementById('close-savings-math');
        
        if (savingsIcon && savingsModal) {
            savingsIcon.addEventListener('click', () => {
                savingsModal.style.display = 'block';
            });
            closeSavingsModal.addEventListener('click', () => {
                savingsModal.style.display = 'none';
            });
            window.addEventListener('click', (e) => {
                if (e.target === savingsModal) savingsModal.style.display = 'none';
            });
        }
        
        // Modal logic (Existing)"""
content = content.replace('// Modal logic', modal_js)

# Add zoom to Volume Chart
old_chart_options = """                                scales: {
                                    x: { grid: { color: gridLinesColor } },
                                    y: { beginAtZero: true, grid: { color: gridLinesColor } }
                                },
                                plugins: {
                                    legend: { display: false }
                                }"""

new_chart_options = """                                scales: {
                                    x: { grid: { color: gridLinesColor } },
                                    y: { beginAtZero: true, grid: { color: gridLinesColor } }
                                },
                                plugins: {
                                    legend: { display: false },
                                    zoom: {
                                        pan: {
                                            enabled: true,
                                            mode: 'x'
                                        },
                                        zoom: {
                                            wheel: {
                                                enabled: true,
                                            },
                                            pinch: {
                                                enabled: true
                                            },
                                            mode: 'x',
                                        }
                                    }
                                }"""
content = content.replace(old_chart_options, new_chart_options)

with open('app.js', 'w') as f:
    f.write(content)
print("Updated app.js")
