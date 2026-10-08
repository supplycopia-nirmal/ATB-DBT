import re

# 1. Update index.html
with open('index.html', 'r') as f:
    html = f.read()

# Add inline styles to savings-math-modal
html = html.replace('<div id="savings-math-modal" class="modal">', '<div id="savings-math-modal" class="modal" style="display: none; position: fixed; z-index: 9999; left: 0; top: 0; width: 100%; height: 100%; overflow: auto; background-color: rgba(0, 0, 0, 0.7);">')

# Add modal content inline style and text color to match bright background
html = html.replace('<div class="modal-content glass-panel" style="max-width: 600px; text-align: left;">', '<div class="modal-content glass-panel" style="margin: 10% auto; padding: 30px; width: 80%; max-width: 600px; border-radius: 12px; position: relative; text-align: left; color: var(--text-primary);">')

# Add JS logic
js_logic = """        // Savings Math Modal
        const savingsMathModal = document.getElementById("savings-math-modal");
        const savingsIconBtn = document.getElementById("savings-info-icon");
        const closeMathModal = document.getElementById("close-savings-math");
        
        if (savingsIconBtn) {
            savingsIconBtn.onclick = function() { savingsMathModal.style.display = "block"; }
        }
        if (closeMathModal) {
            closeMathModal.onclick = function() { savingsMathModal.style.display = "none"; }
        }

        window.onclick = function(event) {
            if (event.target == modal) { modal.style.display = "none"; }
            if (event.target == mvModal) { mvModal.style.display = "none"; }
            if (event.target == vtModal) { vtModal.style.display = "none"; }
            if (event.target == savingsMathModal) { savingsMathModal.style.display = "none"; }
        }"""
html = re.sub(r'window\.onclick = function\(event\) \{.*?\n        \}', js_logic, html, flags=re.DOTALL)

with open('index.html', 'w') as f:
    f.write(html)

# 2. Update style.css
with open('style.css', 'r') as f:
    css = f.read()

# Remove the appended .modal stuff from previous run (if it exists)
css = re.sub(r'/\* Modal Lightbox Styling \*/.*', '', css, flags=re.DOTALL)

# Update .chart-card h2
css = re.sub(r'\.chart-card h2 \{[^\}]+\}', '.chart-card h2 { font-size: 1rem; color: var(--text-secondary); font-weight: 500; margin-bottom: 0.25rem; font-family: inherit; }', css)

# Update .section-header h2
css = re.sub(r'\.section-header h2 \{[^\}]+\}', '.section-header h2 { font-size: 1rem; color: var(--text-secondary); font-weight: 500; font-family: inherit; }', css)

# Update .raw-data-section h2
css = re.sub(r'\.raw-data-section h2 \{[^\}]+\}', '.raw-data-section h2 { font-size: 1rem; color: var(--text-secondary); font-weight: 500; margin-bottom: 1rem; font-family: inherit; }', css)

with open('style.css', 'w') as f:
    f.write(css)

# 3. Update server.py prompt and stripping
with open('server.py', 'r') as f:
    server = f.read()

old_prompt = "prompt = f\"Analyze this supply chain cost savings data for a hospital system:\\nTotal Potential Savings: ${total_savings:,.2f}\\nTop Manufacturers for savings: {mfrs}\\nTop Vendors for savings: {vendors}\\nWrite a short (2-3 paragraphs) summary on these findings with markdown formatting and a bulleted list of 2 actionable recommendations. Adopt the persona of 'Ask The Bee', a proprietary AI assistant. DO NOT include any greeting, welcome message, introduction, or title. Start directly with the key findings and provide sufficient spacing between sections.\""
new_prompt = "prompt = f\"Analyze this supply chain cost savings data:\\nTotal Potential Savings: ${total_savings:,.2f}\\nTop Manufacturers for savings: {mfrs}\\nTop Vendors for savings: {vendors}\\nWrite 2 paragraphs and a bulleted list of 2 actionable recommendations. Output ONLY the raw analytical content. Do NOT include ANY headers, titles, greetings, or introductions.\""
server = server.replace(old_prompt, new_prompt)

# Add stripping logic
old_response = """        response_data = json.loads(response.read().decode('utf-8'))
        insight = response_data['choices'][0]['message']['content']
        return jsonify({"insight": insight})"""
new_response = """        response_data = json.loads(response.read().decode('utf-8'))
        insight = response_data['choices'][0]['message']['content']
        insight = insight.replace('# Executive Summary', '').replace('## Executive Summary', '').replace('### Executive Summary', '').replace('**Executive Summary**', '').strip()
        insight = insight.replace('Welcome to Ask The Bee.', '').replace('Welcome to Ask The Bee!', '').strip()
        if insight.startswith('Executive Summary'): insight = insight[len('Executive Summary'):].strip()
        return jsonify({"insight": insight})"""
server = server.replace(old_response, new_response)

with open('server.py', 'w') as f:
    f.write(server)

print("Patching complete!")
