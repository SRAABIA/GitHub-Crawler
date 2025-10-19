import os
import time
import json
import requests
import psycopg2

# ---------------- CONFIG ----------------
GITHUB_URL = "https://api.github.com/graphql"
TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
TARGET_COUNT = 100000
STAR_RANGES = [
    "stars:50000..100000",
    "stars:10000..50000",
    "stars:5000..9999",
    "stars:1000..4999",
    "stars:100..999",
    "stars:10..99",
    "stars:1..9"
]

# ---------------- DATABASE ----------------
def connect_db():
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST"),
            port=os.getenv("PGPORT"),
            database=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD")
        )
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS repositories (
                id SERIAL PRIMARY KEY,
                repo_id TEXT UNIQUE,   
                name_with_owner TEXT UNIQUE,
                stars INT,
                last_updated TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        return conn, cursor
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        raise

# ---------------- FETCHING ----------------
def fetch_repositories(query, after_cursor=None):
    """Fetch one page of repositories from GitHub GraphQL API."""
    graphql_query = f"""
    {{
      search(query: "{query}", type: REPOSITORY, first: 100, after: {json.dumps(after_cursor) if after_cursor else "null"}) {{
        pageInfo {{
          endCursor
          hasNextPage
        }}
        nodes {{
          ... on Repository {{
            id
            nameWithOwner
            stargazerCount
          }}
        }}
      }}
    }}
    """

    try:
        response = requests.post(GITHUB_URL, json={"query": graphql_query}, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        # Handle rate limit
        remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
        reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
        if remaining == 0:
            sleep_time = max(0, reset_time - int(time.time())) + 10
            print(f"⏸️ Rate limit hit, sleeping for {sleep_time/60:.1f} minutes...")
            time.sleep(sleep_time)
            return fetch_repositories(query, after_cursor)  # retry after sleeping

        if "errors" in data:
            print("⚠️ API Error:", data["errors"])
            return None, None, False

        search_data = data["data"]["search"]
        return search_data["nodes"], search_data["pageInfo"]["endCursor"], search_data["pageInfo"]["hasNextPage"]

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        time.sleep(10)
        return None, None, False

# ---------------- SAVE TO DATABASE ----------------
def save_to_db(cursor, conn, repos):
    """Insert or update repository data using repo_id."""
    for repo in repos:
        repo_id = repo["id"]
        name = repo["nameWithOwner"]
        stars = repo["stargazerCount"]

        try:
            cursor.execute("""
                INSERT INTO repositories (repo_id, name_with_owner, stars, last_updated)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (repo_id) DO UPDATE
                SET stars = EXCLUDED.stars,
                    last_updated = NOW();
            """, (repo_id, name, stars))
        except Exception as e:
            print(f"⚠️ Failed to insert {name}: {e}")
            conn.rollback()
        else:
            conn.commit()

# ---------------- Saving Progress For Automation ----------------

STATE_FILE = "progress_state.json"

def load_progress():
    """Load progress state from file."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"current_range": None, "after_cursor": None, "total_saved": 0}

def save_progress(current_range, after_cursor, total_saved):
    """Save progress state to file."""
    with open(STATE_FILE, "w") as f:
        json.dump({
            "current_range": current_range,
            "after_cursor": after_cursor,
            "total_saved": total_saved
        }, f)

    
# ---------------- MAIN WORKFLOW ----------------
def main():
    conn, cursor = connect_db()
    progress = load_progress()
    total_saved = progress["total_saved"]

    start_from_range = progress["current_range"]
    resume_cursor = progress["after_cursor"]

    try:
        for star_range in STAR_RANGES:
            # Skip previously completed ranges
            if start_from_range and STAR_RANGES.index(star_range) < STAR_RANGES.index(start_from_range):
                continue

            print(f"\n🚀 Fetching repos for range: {star_range}")
            after_cursor = resume_cursor if start_from_range == star_range else None
            has_next_page = True

            while has_next_page and total_saved < TARGET_COUNT:
                repos, after_cursor, has_next_page = fetch_repositories(star_range, after_cursor)
                if not repos:
                    break

                save_to_db(cursor, conn, repos)
                total_saved += len(repos)

                # ✅ Save progress after each batch
                save_progress(star_range, after_cursor, total_saved)
                print(f"✅ Total saved so far: {total_saved}")

                if total_saved >= TARGET_COUNT:
                    break

                time.sleep(1)

            # Reset cursor for next range
            resume_cursor = None

            if total_saved >= TARGET_COUNT:
                break

    except KeyboardInterrupt:
        print("\n🛑 Interrupted manually. Progress saved.")
        save_progress(star_range, after_cursor, total_saved)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        save_progress(star_range, after_cursor, total_saved)
    finally:
        cursor.close()
        conn.close()
        print(f"\n🎉 Done! {total_saved} repositories saved. Progress saved to {STATE_FILE}.")
        
# ---------------- ENTRY ----------------
if __name__ == "__main__":
    main()
