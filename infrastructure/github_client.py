import requests
import time
import json

class GitHubClient:
    def __init__(self, token):
        self.url = "https://api.github.com/graphql"
        self.headers = {"Authorization": f"Bearer {token}"}

    def fetch_repositories(self, query, after_cursor=None):
        gql = f"""
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

        response = requests.post(self.url, json={"query": gql}, headers=self.headers)
        if response.status_code != 200:
            raise Exception(f"GitHub API failed: {response.status_code}")

        data = response.json()
        if "errors" in data:
            raise Exception(data["errors"])

        return data["data"]["search"]
