import json
import os
from datetime import datetime, UTC

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
STATUS_FILE = os.path.join(DATA_DIR, "system_status.json")


def _now():
    return datetime.now(UTC).isoformat()


def load_status():
    if os.path.exists(STATUS_FILE):
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def update_module_status(module: str, source: str):
    """
    module: masterlist | metrics | sid | ui_payload
    source: live | cache
    """
    status = load_status()

    entry = status.get(module, {})
    entry["source"] = source
    entry["last_attempt"] = _now()

    if source == "live":
        entry["last_live_update"] = _now()

    status[module] = entry

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, indent=2)


