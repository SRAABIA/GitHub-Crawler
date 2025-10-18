import requests
import json
import time
import psycopg2
import os

# --- GitHub API setup ---
url = "https://api.github.com/graphql"

# Automatically use GitHub Actions token if available
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

# --- Fetch and save repos ---
repositories = []
after_cursor = None
has_next_page = True
target_count = 100000  # total repos you want

while has_next_page and len(repositories) < target_count:
    query = f"""
    {{
      search(query: "stars:>1", type: REPOSITORY, first: 100, after: {json.dumps(after_cursor) if after_cursor else "null"}) {{
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

    # --- Make API request ---
    response = requests.post(url, json={"query": query}, headers=headers)

    # --- Check rate limit headers ---
    remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))

    if remaining == 0:
        sleep_seconds = max(0, reset_time - int(time.time())) + 10
        print(f"⏸️ Rate limit reached. Waiting {sleep_seconds/60:.1f} minutes...")
        time.sleep(sleep_seconds)
        continue  # after sleeping, retry the same query

    data = response.json()

    if "errors" in data:
        print("Error:", data["errors"])
        break

    search_data = data["data"]["search"]
    batch = search_data["nodes"]
    repositories.extend(batch)

    after_cursor = search_data["pageInfo"]["endCursor"]
    has_next_page = search_data["pageInfo"]["hasNextPage"]

    # --- Save batch to PostgreSQL ---
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

    print(f"✅ Saved {len(repositories)} repos so far (remaining: {remaining})")
    time.sleep(1)  # small delay for safety

# --- Cleanup ---
cursor.close()
conn.close()

print(f"\n🎉 Done! {len(repositories)} repositories saved to PostgreSQL.")
