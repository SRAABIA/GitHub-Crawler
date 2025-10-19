import json, os

STATE_FILE = "progress_state.json"

def load_progress():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"current_range": None, "after_cursor": None, "total_saved": 0}

def save_progress(current_range, after_cursor, total_saved):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "current_range": current_range,
            "after_cursor": after_cursor,
            "total_saved": total_saved
        }, f)
