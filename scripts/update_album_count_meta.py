import sqlite3
from pathlib import Path
from config import (
    ROOT_DIR,
    DB_PATH,
)

conn = sqlite3.connect(str(DB_PATH))
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS app_meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
""")

album_count = cur.execute("SELECT COUNT(*) FROM albums").fetchone()[0]

cur.execute("""
    INSERT OR REPLACE INTO app_meta(key, value)
    VALUES ('album_count', ?)
""", (str(album_count),))

conn.commit()
conn.close()

print(f"album_count saved: {album_count}", flush=True)
