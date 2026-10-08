import re

with open('/Users/nirmalrayan/.gemini/antigravity/brain/c3eb8abd-6329-4327-8df8-28d33e86c183/task.md', 'r') as f:
    task = f.read()
task = task.replace('[ ] Address the Out of Memory error', '[x] Address the Out of Memory error')
task = task.replace('[/] Fix the JOIN operations', '[x] Fix the JOIN operations')
with open('/Users/nirmalrayan/.gemini/antigravity/brain/c3eb8abd-6329-4327-8df8-28d33e86c183/task.md', 'w') as f:
    f.write(task)
