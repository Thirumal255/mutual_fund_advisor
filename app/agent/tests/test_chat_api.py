import requests

BASE_URL = "http://127.0.0.1:8000"

USERNAME = "admin"       # change if needed
PASSWORD = "adminpass"   # change if needed


def get_access_token():
    """Login and fetch JWT token"""
    resp = requests.post(
        f"{BASE_URL}/api/token",
        data={
            "username": USERNAME,
            "password": PASSWORD,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def test_chat():
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    def ask(msg):
        resp = requests.post(
            f"{BASE_URL}/api/chat",
            headers=headers,
            json={"message": msg},
        )
        resp.raise_for_status()
        print("\nUSER:", msg)
        print("BOT:", resp.json())

    ask("Suggest low risk mutual funds")

    ask("My goal is capital protection")
    ask("Investment horizon is 5 years")
    ask("I want to invest via SIP")
    ask("I can invest 15000 per month")

    # 🔥 Now recommendations should trigger
    ask("Please recommend funds")

    # Follow-up (no re-ranking)
    ask("Which one is best for tax efficiency?")


if __name__ == "__main__":
    print("🚀 Testing Chat API")
    test_chat()
