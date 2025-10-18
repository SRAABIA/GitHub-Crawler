# 🐙 GitHub Crawler – Software Engineer Take-Home Assignment

## 📘 Overview
This project automates the process of retrieving **star counts for 100,000 public GitHub repositories** using the **GitHub GraphQL API**.  
It is fully implemented in **Python**, uses **PostgreSQL** for persistent storage, and runs automatically through a **GitHub Actions workflow**.  
The workflow ensures rate-limit compliance, efficient data insertion, and artifact generation — all without requiring external secrets or manual setup.

---

## ⚙️ Tech Stack
- **Python 3.11+** – Main programming language for API requests and database operations  
- **PostgreSQL** – Stores repository metadata and star counts  
- **GitHub Actions** – CI/CD automation for the entire pipeline  
- **Docker** – Runs PostgreSQL as a service container within GitHub Actions  

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
   - Executes the Python script (e.g., `crawl_stars.py`)  
   - Fetches 100,000 repositories using **GitHub’s GraphQL API**  
   - Handles pagination, retry on rate limits, and stores data efficiently in PostgreSQL

5. **Artifact Generation**
   - Exports the final data (as `.csv` or `.json`) from the database  
   - Uploads the file as a GitHub Actions artifact

---

## 🧩 Database Schema (Example)
```sql
CREATE TABLE IF NOT EXISTS repositories (
    id SERIAL PRIMARY KEY,
    name_with_owner TEXT UNIQUE,
    stars INT,
    last_updated TIMESTAMP DEFAULT NOW()
);