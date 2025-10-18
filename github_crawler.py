def fetch_repos_for_range(star_query):
    has_next_page = True
    after_cursor = None
    total = 0
    repos = []

    while has_next_page:
        query = f"""
        {{
          search(query: "stars:{star_query}", type: REPOSITORY, first: 100, after: {json.dumps(after_cursor) if after_cursor else "null"}) {{
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

        # Handle network issues or invalid JSON
        try:
            data = response.json()
        except Exception as e:
            print(f"❌ Failed to parse JSON for {star_query}: {e}")
            time.sleep(5)
            continue

        # --- Check for API-level errors ---
        if "errors" in data:
            print(f"⚠️ API error for {star_query}: {data['errors']}")
            # If it's rate limit or query too big, split handled outside
            break

        # --- Check if "data" exists ---
        if "data" not in data or not data["data"]:
            print(f"⚠️ No 'data' field returned for {star_query}. Response: {data}")
            time.sleep(5)
            continue

        search_data = data["data"]["search"]
        nodes = search_data["nodes"]
        repos.extend(nodes)
        total += len(nodes)

        after_cursor = search_data["pageInfo"]["endCursor"]
        has_next_page = search_data["pageInfo"]["hasNextPage"]

        # GraphQL limit guard
        if total >= 1000:
            has_next_page = False

        time.sleep(0.2)

    return repos, total
