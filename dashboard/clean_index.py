import re

with open('index.html', 'r') as f:
    html = f.read()

# Remove old savings math logic
html = re.sub(r'\s*// Savings Math Modal.*?if \(closeMathModal\) \{\s*closeMathModal\.onclick = function\(\) \{ savingsMathModal\.style\.display = "none"; \}\s*\}', '', html, flags=re.DOTALL)

# Remove the line if (event.target == savingsMathModal) ... inside window.onclick
html = re.sub(r'\s*if \(event\.target == savingsMathModal\) \{ savingsMathModal\.style\.display = "none"; \}', '', html)

with open('index.html', 'w') as f:
    f.write(html)

print("Cleaned index.html")
