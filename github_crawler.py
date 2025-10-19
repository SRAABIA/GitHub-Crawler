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
def save_to_db(cursor, conn, repos, seen):
    """Insert repository data, skip duplicates."""
    for repo in repos:
        name = repo["nameWithOwner"]
        if name in seen:
            continue
        seen.add(name)
        stars = repo["stargazerCount"]
        try:
            cursor.execute("""
                INSERT INTO repositories (name_with_owner, stars)
                VALUES (%s, %s)
                ON CONFLICT (name_with_owner) DO UPDATE
                SET stars = EXCLUDED.stars,
                    last_updated = NOW();
            """, (name, stars))
        except Exception as e:
            print(f"⚠️ Failed to insert {name}: {e}")
            conn.rollback()
        else:
            conn.commit()

# ---------------- MAIN WORKFLOW ----------------
def main():
    conn, cursor = connect_db()
    seen = set()
    total_saved = 0

    try:
        for star_range in STAR_RANGES:
            print(f"\n🚀 Fetching repos for range: {star_range}")
            after_cursor = None
            has_next_page = True

            while has_next_page and total_saved < TARGET_COUNT:
                repos, after_cursor, has_next_page = fetch_repositories(star_range, after_cursor)
                if not repos:
                    break

                save_to_db(cursor, conn, repos, seen)
                total_saved = len(seen)

                print(f"✅ Total saved so far: {total_saved}")

                if total_saved >= TARGET_COUNT:
                    break

                time.sleep(1)  # safety delay

            if total_saved >= TARGET_COUNT:
                break

    except KeyboardInterrupt:
        print("\n🛑 Interrupted manually. Exiting cleanly...")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    finally:
        cursor.close()
        conn.close()
        print(f"\n🎉 Done! {total_saved} repositories saved to PostgreSQL.")

# ---------------- ENTRY ----------------
if __name__ == "__main__":
    main()
