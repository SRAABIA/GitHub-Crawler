import time
from core.state_manager import load_progress, save_progress

def run_crawler(github_client, db_client, star_ranges, target_count):
    progress = load_progress()
    total_saved = progress["total_saved"]
    start_range = progress["current_range"]
    resume_cursor = progress["after_cursor"]

    for star_range in star_ranges:
        if start_range and star_ranges.index(star_range) < star_ranges.index(start_range):
            continue

        after_cursor = resume_cursor if start_range == star_range else None
        has_next_page = True

        while has_next_page and total_saved < target_count:
            data = github_client.fetch_repositories(f"stars:{star_range}", after_cursor)
            repos = data["nodes"]
            db_client.save_repositories(repos)
            total_saved += len(repos)

            after_cursor = data["pageInfo"]["endCursor"]
            has_next_page = data["pageInfo"]["hasNextPage"]

            save_progress(star_range, after_cursor, total_saved)
            print(f"✅ Saved {total_saved} repos so far.")
            time.sleep(1)
