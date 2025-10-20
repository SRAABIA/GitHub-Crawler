import psycopg2

import psycopg2
from psycopg2 import OperationalError

class DatabaseClient:
    def __init__(self, host, port, db, user, password):
        try:
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                database=db,
                user=user,
                password=password
            )
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
        except OperationalError as e:
            raise RuntimeError(f"❌ Failed to connect to PostgreSQL at {host}:{port} - {e}")

    def save_repositories(self, repos):
        data = [
            (r["id"], r["nameWithOwner"], r["stargazerCount"])
            for r in repos
        ]
        self.cur.executemany("""
            INSERT INTO repositories (repo_id, name_with_owner, stars)
            VALUES (%s, %s, %s)
            ON CONFLICT (repo_id) DO UPDATE
            SET stars = EXCLUDED.stars, last_updated = NOW();
        """, data)
        self.conn.commit()
    
    def close(self):
        self.cur.close()
        self.conn.close()
