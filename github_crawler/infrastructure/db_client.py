import psycopg2

class DatabaseClient:
    def __init__(self, host, port, db, user, password):
        self.conn = psycopg2.connect(host=host, port=port, database=db, user=user, password=password)
        self.cur = self.conn.cursor()
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS repositories (
            repo_id TEXT UNIQUE,
            name_with_owner TEXT,
            stars INT,
            last_updated TIMESTAMP DEFAULT NOW()
        );
        """)
        self.conn.commit()

    def save_repositories(self, repos):
        for repo in repos:
            self.cur.execute("""
                INSERT INTO repositories (repo_id, name_with_owner, stars)
                VALUES (%s, %s, %s)
                ON CONFLICT (repo_id) DO UPDATE
                SET stars = EXCLUDED.stars, last_updated = NOW();
            """, (repo["id"], repo["nameWithOwner"], repo["stargazerCount"]))
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
