import os
import requests
import json
import time
import psycopg2
from psycopg2.extras import execute_values
from collections import deque


# --- GitHub API setup ---
url = "https://api.github.com/graphql"
token = os.getenv("GITHUB_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

# --- PostgreSQL setup ---
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
    created_at DATE,
    last_updated TIMESTAMP DEFAULT NOW()
);
""")
conn.commit()

def fetch_repos_for_range(star_query):
    """
    Fetch repositories for a given star range with proper pagination and rate-limit handling.
    """
    has_next_page = True
    after_cursor = None
    total = 0
    repos = []

    query = """
    query($cursor: String, $starQuery: String!) {
      rateLimit {
        limit
        cost
        remaining
        resetAt
      }
      search(query: $starQuery, type: REPOSITORY, first: 100, after: $cursor) {
        pageInfo {
          endCursor
          hasNextPage
        }
        nodes {
          ... on Repository {
            nameWithOwner
            stargazerCount
          }
        }
      }
    }
    """

    while has_next_page:
        variables = {"cursor": after_cursor, "starQuery": f"stars:{star_query}"}

        try:
            response = requests.post(url, json={"query": query, "variables": variables}, headers=headers)
            data = response.json()
        except Exception as e:
            print(f"❌ Network or JSON error for range {star_query}: {e}")
            time.sleep(5)
            continue

        # --- Handle GraphQL or rate errors ---
        if "errors" in data:
            print(f"⚠️ API error for {star_query}: {data['errors']}")
            time.sleep(5)
            continue

        if "data" not in data or not data["data"]:
            print(f"⚠️ No data field for {star_query}. Full response: {data}")
            time.sleep(10)
            continue

        # --- Read rate limit info ---
        rate = data["data"]["rateLimit"]
        remaining = rate["remaining"]
        reset_time = rate["resetAt"]

        if remaining < 20:
            reset_timestamp = datetime.fromisoformat(reset_time.replace("Z", "+00:00"))
            wait_seconds = (reset_timestamp - datetime.utcnow()).total_seconds()
            wait_seconds = max(wait_seconds, 60)
            print(f"⏸️ Rate limit near exhaustion ({remaining} left). Sleeping {int(wait_seconds)}s until reset...")
            time.sleep(wait_seconds)
            continue

        # --- Extract search results ---
        search_data = data["data"]["search"]
        nodes = search_data["nodes"]
        repos.extend(nodes)
        total += len(nodes)

        after_cursor = search_data["pageInfo"]["endCursor"]
        has_next_page = search_data["pageInfo"]["hasNextPage"]

        print(f"✅ Collected {len(nodes)} repos this page (Total: {total}) | Remaining API calls: {remaining}")

        # --- Respectful short sleep ---
        time.sleep(0.5)

        # --- Stop early if GitHub caps to 1000 for safety ---
        if total >= 1000:
            print(f"🧭 Hit 1000 cap for {star_query}, moving to next range.")
            break

    return repos, total

# --- Dynamic partitioning + batch processing ---
queue = deque()
queue.append((1, 200000))  # initial wide range
target_total = 100000
total_saved = 0
batch_buffer = []  # stores repos temporarily before bulk insert
batch_size = 500   # adjust this for optimal speed (100–1000 typical)

while queue and total_saved < target_total:
    low, high = queue.popleft()
    star_query = f"{low}..{high}"
    print(f"🔍 Fetching range: stars:{star_query}")

    repos, count = fetch_repos_for_range(star_query)
    print(f"→ Got {count} repos for stars:{star_query}")

    if count == 1000 and high - low > 1:
        # Split dense range
        mid = (low + high) // 2
        queue.append((low, mid))
        queue.append((mid + 1, high))
        print(f"📉 Range too dense, splitting into {low}..{mid} and {mid+1}..{high}")
    else:
        for repo in repos:
            batch_buffer.append((repo["nameWithOwner"], repo["stargazerCount"]))
            
            # Once batch is full, insert all at once
            if len(batch_buffer) >= batch_size:
                execute_values(cursor, """
                    INSERT INTO repositories (name_with_owner, stars)
                    VALUES %s
                    ON CONFLICT (name_with_owner) DO UPDATE
                    SET stars = EXCLUDED.stars,
                        last_updated = NOW();
                """, batch_buffer)
                conn.commit()
                total_saved += len(batch_buffer)
                print(f"✅ Inserted batch of {len(batch_buffer)} (Total saved: {total_saved})")
                batch_buffer.clear()

        # After processing this range, insert any leftovers
        if batch_buffer:
            execute_values(cursor, """
                INSERT INTO repositories (name_with_owner, stars)
                VALUES %s
                ON CONFLICT (name_with_owner) DO UPDATE
                SET stars = EXCLUDED.stars,
                    last_updated = NOW();
            """, batch_buffer)
            conn.commit()
            total_saved += len(batch_buffer)
            print(f"✅ Final batch inserted ({len(batch_buffer)} items) — Total: {total_saved}")
            batch_buffer.clear()

    if total_saved >= target_total:
        print(f"\n🎉 Reached target of {target_total} repos!")
        break

print(f"\n🏁 Done! {total_saved} repositories saved to PostgreSQL.")
cursor.close()
conn.close()
