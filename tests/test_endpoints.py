# tests/test_endpoints.py
import requests
import json
import sys

BASE_URL = "http://127.0.0.1:8000"

def pretty(obj):
    print(json.dumps(obj, indent=2))

def test_health():
    print("\n=== Test: /health ===")
    try:
        res = requests.get(f"{BASE_URL}/health")
        pretty(res.json())
    except Exception as e:
        print("ERROR:", e)

def test_sample_funds():
    print("\n=== Test: /sample_funds ===")
    try:
        res = requests.get(f"{BASE_URL}/sample_funds")
        pretty(res.json())
    except Exception as e:
        print("ERROR:", e)

def test_recommend_basic():
    print("\n=== Test: /recommend (basic moderate) ===")
    payload = {
        "monthly_sip": 10000,
        "horizon_years": 5,
        "risk_profile": "moderate",
        "preferences": ["Large Cap"]
    }
    try:
        res = requests.post(f"{BASE_URL}/recommend", json=payload)
        pretty(res.json())
    except Exception as e:
        print("ERROR:", e)

def test_recommend_no_preferences():
    print("\n=== Test: /recommend (auto seeds) ===")
    payload = {
        "monthly_sip": 5000,
        "horizon_years": 3,
        "risk_profile": "low"
    }
    try:
        res = requests.post(f"{BASE_URL}/recommend", json=payload)
        pretty(res.json())
    except Exception as e:
        print("ERROR:", e)

def test_recommend_invalid_risk():
    print("\n=== Test: /recommend (invalid risk) ===")
    payload = {
        "monthly_sip": 5000,
        "horizon_years": 2,
        "risk_profile": "aggressive"  # expected to fail validation
    }
    try:
        res = requests.post(f"{BASE_URL}/recommend", json=payload)
        pretty(res.json())
    except Exception as e:
        print("ERROR:", e)

if __name__ == "__main__":
    print("== Running Manual API Tests ==")
    test_health()
    test_sample_funds()
    test_recommend_basic()
    test_recommend_no_preferences()
    test_recommend_invalid_risk()
    print("\n== Done ==")
