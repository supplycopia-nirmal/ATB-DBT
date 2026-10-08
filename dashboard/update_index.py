with open('index.html', 'r') as f:
    html = f.read()

# 1. Add ChartJS Zoom
html = html.replace('<!-- Chart.js -->\n    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>', '<!-- Chart.js -->\n    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>\n    <script src="https://cdn.jsdelivr.net/npm/hammerjs@2.0.8"></script>\n    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-zoom@2.0.1/dist/chartjs-plugin-zoom.min.js"></script>\n    <!-- Font Awesome -->\n    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">')

# 2. Add Info Icon to Cost Savings Analysis title
html = html.replace('<h2>Cost Savings Analysis</h2>', '<h2>Cost Savings Analysis <i class="fas fa-info-circle" id="savings-info-icon" style="cursor: pointer; font-size: 0.6em; color: var(--accent-color); margin-left: 8px; vertical-align: middle;"></i></h2>')

# 3. Add Modal for Math
modal_html = """
    <!-- Math Modal -->
    <div id="savings-math-modal" class="modal">
        <div class="modal-content glass-panel" style="max-width: 600px; text-align: left;">
            <span class="close-modal" id="close-savings-math">&times;</span>
            <h2 style="margin-top:0; color: var(--accent-color);">Cost Savings Calculations</h2>
            <div style="margin-top: 20px; line-height: 1.6;">
                <h3>Price Variance</h3>
                <pre style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 8px; color: #34d399; font-family: monospace;">price_variance = (consumption_unit_price - contract_price) * total_quantity</pre>
                <p style="color: #a0aec0; margin-bottom: 20px;">Calculates the difference between the actual unit price paid during the procedure and the matched historic contract unit price, multiplied by the quantity consumed.</p>
                
                <h3>Total Identified Savings</h3>
                <pre style="background: rgba(0,0,0,0.3); padding: 15px; border-radius: 8px; color: #34d399; font-family: monospace;">Total Savings = SUM(price_variance) WHERE price_variance > 0</pre>
                <p style="color: #a0aec0;">Aggregates all positive price variances across procedures to identify the total systemic cost savings opportunity.</p>
            </div>
        </div>
    </div>
</body>"""
html = html.replace('</body>', modal_html)

# 4. Remove Ask The Bee: Executive Summary title
html = html.replace('<h3>Ask The Bee: Executive Summary <span style="font-size: 0.8em; opacity: 0.7; font-weight: 300;">insight generate insights</span></h3>', '')
html = html.replace('<h3>Ask The Bee: Executive Summary <span style="font-size: 0.8em; opacity: 0.7; font-weight: 300;">✨ generate insights</span></h3>', '')

with open('index.html', 'w') as f:
    f.write(html)
print("Updated index.html")
