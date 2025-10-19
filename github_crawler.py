import os
import requests
import json
import time
import psycopg2

# --- GitHub API setup ---
url = "https://api.github.com/graphql"
token = os.getenv("GITHUB_TOKEN", "YOUR_PERSONAL_ACCESS_TOKEN")
headers = {"Authorization": f"Bearer {token}"}

# --- PostgreSQL setup ---
conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
    database=os.getenv("PGDATABASE", "github_data"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "YOUR_LOCAL_DB_PASSWORD")
)
cursor = conn.cursor()

# --- Create table if not exists ---
cursor.execute("""
CREATE TABLE IF NOT EXISTS repositories (
    id SERIAL PRIMARY KEY,
    name_with_owner TEXT UNIQUE,
    stars INT,
    last_updated TIMESTAMP DEFAULT NOW()
);
""")
conn.commit()

# --- Define multiple star ranges to bypass 1000-result limit ---
star_ranges = [
    "stars:1..10",
    "stars:11..50",
    "stars:51..100",
    "stars:101..500",
    "stars:501..1000",
    "stars:1001..5000",
    "stars:5001..10000",
    "stars:10001..50000",
    "stars:>50000"
]

repositories = []
target_count = 100_000  # you can adjust this as needed

for star_range in star_ranges:
    after_cursor = None
    has_next_page = True
    print(f"\n🔍 Fetching repos for range: {star_range}")

    while has_next_page and len(repositories) < target_count:
        query = f"""
        {{
          rateLimit {{
            cost
            remaining
            resetAt
          }}
          search(query: "{star_range}", type: REPOSITORY, first: 100, after: {json.dumps(after_cursor) if after_cursor else "null"}) {{
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

        response = requests.post(url, json={"query": query}, headers=headers)
        data = response.json()

        # --- Error handling ---
        if "errors" in data:
            print("Error:", data["errors"])
            break

        # --- Rate limit info ---
        rate = data["data"]["rateLimit"]
        cost = rate["cost"]
        remaining = rate["remaining"]
        reset_time = rate["resetAt"]
        print(f"⚙️ Cost: {cost} | Remaining: {remaining} | Resets at: {reset_time}")

        if remaining < 50:
            reset_epoch = int(time.mktime(time.strptime(reset_time, "%Y-%m-%dT%H:%M:%SZ")))
            sleep_seconds = max(0, reset_epoch - int(time.time())) + 10
            print(f"⏸️ Near rate limit. Sleeping for {sleep_seconds/60:.1f} minutes...")
            time.sleep(sleep_seconds)
            continue

        # --- Extract data ---
        search_data = data["data"]["search"]
        batch = search_data["nodes"]
        repositories.extend(batch)

        after_cursor = search_data["pageInfo"]["endCursor"]
        has_next_page = search_data["pageInfo"]["hasNextPage"]

        # --- Insert batch into DB ---
        for repo in batch:
            name = repo["nameWithOwner"]
            stars = repo["stargazerCount"]
            cursor.execute("""
                INSERT INTO repositories (name_with_owner, stars)
                VALUES (%s, %s)
                ON CONFLICT (name_with_owner) DO UPDATE
                SET stars = EXCLUDED.stars,
                    last_updated = NOW();
            """, (name, stars))

        conn.commit()
        print(f"✅ Saved {len(repositories)} repos so far ({star_range})")
        time.sleep(1)  # small delay between requests

        # stop early if total target reached
        if len(repositories) >= target_count:
            break

cursor.close()
conn.close()
print(f"\n🎉 Done! {len(repositories)} repositories saved to PostgreSQL.")
