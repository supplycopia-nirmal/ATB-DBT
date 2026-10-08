import re

# 1. Revert fonts in style.css
with open('style.css', 'r') as f:
    css = f.read()

css = re.sub(r'\.chart-card h2 \{[^\}]+\}', '.chart-card h2 {\\n    font-family: \\\'Outfit\\\', sans-serif;\\n    font-size: 1.25rem;\\n    margin-bottom: 0.25rem;\\n}', css)
css = re.sub(r'\.section-header h2 \{[^\}]+\}', '.section-header h2 {\\n    font-family: \\\'Outfit\\\', sans-serif;\\n    font-size: 1.5rem;\\n    margin-bottom: 0.5rem;\\n    color: var(--text-primary);\\n}', css)
css = re.sub(r'\.raw-data-section h2 \{[^\}]+\}', '.raw-data-section h2 {\\n    font-family: \\\'Outfit\\\', sans-serif;\\n    font-size: 1.25rem;\\n    margin-bottom: 1rem;\\n    color: var(--text-primary);\\n}', css)

with open('style.css', 'w') as f:
    f.write(css)


# 2. Add robust jQuery listener to app.js
with open('app.js', 'r') as f:
    app = f.read()

jquery_modal_logic = """
    // Robust Savings Math Modal Logic
    $(document).on('click', '#savings-info-icon', function() {
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
"""

# Append to app.js inside document ready or at the end
app += jquery_modal_logic

with open('app.js', 'w') as f:
    f.write(app)

print("Fixed fonts and added jQuery modal logic.")
