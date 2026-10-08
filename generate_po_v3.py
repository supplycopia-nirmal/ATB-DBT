import re

with open("Spend/spend_pipeline.py", "r") as f:
    code = f.read()

# Remove caching functions
code = re.sub(r'def stage_[a-z_]+\(.*?return full\.join.*?$', '', code, flags=re.DOTALL|re.MULTILINE)

# We just want to extract the pure logic!
# Actually, the user wants me to apply the logic.
