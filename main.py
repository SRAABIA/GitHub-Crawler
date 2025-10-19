import os
from infrastructure.github_client import GitHubClient
from infrastructure.db_client import DatabaseClient
from core.crawler import run_crawler

STAR_RANGES = ["100000..*", "50000..100000", "10000..50000", "5000..10000", "1000..5000", "100..1000"]
TARGET_COUNT = 100000

def main():
    github = GitHubClient(os.getenv("GITHUB_TOKEN"))
    db = DatabaseClient(
        host=os.getenv("PGHOST"),
        port=os.getenv("PGPORT"),
        db=os.getenv("PGDATABASE"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD")
    )
    run_crawler(github, db, STAR_RANGES, TARGET_COUNT)
    db.close()

if __name__ == "__main__":
    main()
