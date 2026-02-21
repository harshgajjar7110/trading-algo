"""Test API endpoints for strategy selector"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web', 'backend'))

from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

print("Testing Strategy Selector API Endpoints")
print("=" * 60)

# Test 1: Get available strategies
print("\n1. GET /api/strategy/available")
response = client.get("/api/strategy/available")
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"   Strategies: {len(data.get('strategies', []))}")
    for s in data.get('strategies', []):
        print(f"     - {s['id']}: {s['name']}")
else:
    print(f"   Error: {response.text}")

# Test 2: Get strategy details
print("\n2. GET /api/strategy/survivor/details")
response = client.get("/api/strategy/survivor/details")
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"   Name: {data.get('name')}")
    print(f"   Risk Level: {data.get('risk_level')}")
else:
    print(f"   Error: {response.text}")

# Test 3: Preview config
print("\n3. POST /api/strategy/preview-config")
response = client.post("/api/strategy/preview-config", json={
    "strategy_id": "survivor",
    "config_override": {}
})
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"   Strategy: {data.get('strategy_name')}")
    print(f"   Is Valid: {data.get('is_valid')}")
    print(f"   Risk Level: {data.get('risk_level')}")
else:
    print(f"   Error: {response.text}")

# Test 4: Get current strategy
print("\n4. GET /api/strategy/current")
response = client.get("/api/strategy/current")
print(f"   Status: {response.status_code}")
if response.status_code == 200:
    data = response.json()
    print(f"   Running: {data.get('running')}")
else:
    print(f"   Error: {response.text}")

print("\n" + "=" * 60)
print("API Tests Complete")
