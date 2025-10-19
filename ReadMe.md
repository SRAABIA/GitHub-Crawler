
# 🕸️ GitHub Crawler Pipeline
This project is an automated GitHub repository crawler designed to fetch and store public repositories from the GitHub GraphQL API based on star ranges. It is integrated with PostgreSQL for persistent storage and runs automatically using **GitHub Actions**.

---

## 🚀 Features

- Fetches repositories using GitHub’s **GraphQL API**.
- Stores data (repository ID, name, and stars) in a **PostgreSQL** database.
- Skips duplicate records efficiently using `ON CONFLICT`.
- Supports **automatic daily runs** via **GitHub Actions**.
- Handles API pagination and rate limits.
- Designed for scalability and automation.
  
---

## ⚙️ Tech Stack
- **Python 3.11+** – Main programming language for API requests and database operations  
- **PostgreSQL** – Stores repository metadata and star counts  
- **GitHub Actions** – CI/CD automation for the entire pipeline  
---

## 🚀 Workflow Summary
The GitHub Actions workflow (`.github/workflows/main.yml`) automates the following steps:

1. **Container Setup**
   - Runs on `ubuntu-latest`
   - Uses the official **Python** image as the container
   - Starts a **PostgreSQL service container** using the default credentials

2. **Environment Setup**
   - Installs all dependencies listed in `requirements.txt`
   - Verifies Python, pip, and PostgreSQL connectivity

3. **Database Initialization**
   - Creates the required database and schema  
   - Ensures idempotent table creation (safe to rerun without conflicts)

4. **Crawl-Stars Step**
   - Runs the main Python crawler script
   - Collects up to 100,000 GitHub repositories using the GraphQL API
   - Implements pagination, retry logic for rate limits and network errors
   - Ensures efficient, duplicate-safe insertion into PostgreSQL with automatic resume support

5. **Artifact Generation**
   - Exports the final data (as `.csv` ) from the database  
   - Uploads the file as a GitHub Actions artifact

---

## 🧩 Database Schema
```sql
CREATE TABLE IF NOT EXISTS repositories (
    id SERIAL PRIMARY KEY,
    repo_id TEXT UNIQUE,
    name_with_owner TEXT UNIQUE,
    stars INT,
    last_updated TIMESTAMP DEFAULT NOW()
);
```
---

## 🧠 Key Concepts

- **Separation of Concerns (planned):** Next iteration will move toward modular architecture.
- **Immutability:** Repository data is treated as immutable; updates occur only via `ON CONFLICT`.
- **Error Handling:** Safe commits and rollback strategy in case of insert failures.
- **Automation:** GitHub Actions schedules and runs the crawler daily without manual triggers.

---

## 🕒 Workflow Schedule

The workflow runs **every hour at minute 0 (UTC)** to fetch new repositories incrementally.

---

## Schema Evolution and Scaling
If the crawler were designed to handle 500 million repositories instead of 100,000, the approach would need to shift toward distributed, incremental, and asynchronous data collection using message queues, batch inserts, partitioned tables, and horizontal database scaling. For evolving schema to include more metadata (issues, pull requests, comments, etc.), I’d normalize the design into separate related tables repositories, issues, pull_requests, comments, reviews, etc. each linked by foreign keys. Updates like new PR comments would use UPSERT operations or incremental syncs based on timestamps, ensuring only changed rows are updated. This minimizes database writes and keeps operations efficient as the dataset grows over time.
