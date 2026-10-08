import urllib.request
import json

endpoints = [
    "http://localhost:8080/api/data?table=transformed&length=1",
    "http://localhost:8080/api/data-health",
    "http://localhost:8080/api/savings/by-manufacturer",
    "http://localhost:8080/api/savings/by-vendor",
    "http://localhost:8080/api/savings/by-procedure",
    "http://localhost:8080/api/volume-trend?dataset=con",
]

for url in endpoints:
    print(f"Testing {url} ...")
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req) as res:
            body = res.read().decode('utf-8')
            data = json.loads(body)
            print(f"SUCCESS: {len(body)} bytes returned")
    except Exception as e:
        print(f"FAILED: {e}")
