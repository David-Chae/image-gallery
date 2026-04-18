import os
import re
import sqlite3
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "gallery.db"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}

PROGRESS_PRINT_INTERVAL = 1.0
BATCH_COMMIT_EVERY = 200


def log(message: str):
    print(message, flush=True)


def natural_sort_key_name(name: str):
    parts = re.split(r"(\d+)", name.lower())
    return [int(p) if p.isdigit() else p for p in parts]


def format_eta(seconds: float) -> str:
    if seconds < 0:
        return "?"
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {sec}s"
    hours = minutes // 60
    minutes %= 60
    return f"{hours}h {minutes}m {sec}s"


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=DELETE;")
    cur.execute("PRAGMA synchronous=FULL;")
    cur.execute("PRAGMA foreign_keys=ON;")
    return conn


def ensure_album_images_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS album_images (
            album_id INTEGER NOT NULL,
            image_index INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            PRIMARY KEY (album_id, image_index)
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_album_images_album_id
        ON album_images(album_id)
    """)
    conn.commit()


def clear_album_images_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM album_images")
    conn.commit()


def scan_folder_images(folder: Path):
    image_names = []

    try:
        with os.scandir(folder) as it:
            for entry in it:
                try:
                    if entry.is_file(follow_symlinks=False):
                        ext = Path(entry.name).suffix.lower()
                        if ext in IMAGE_EXTS:
                            image_names.append(entry.name)
                except Exception:
                    pass
    except Exception:
        return []

    image_names.sort(key=natural_sort_key_name)
    return image_names


def insert_album_images(conn: sqlite3.Connection, album_id: int, folder: Path, image_names: list[str]):
    cur = conn.cursor()
    rows = [
        (album_id, idx, name, str(folder / name))
        for idx, name in enumerate(image_names)
    ]
    cur.executemany("""
        INSERT INTO album_images (album_id, image_index, file_name, file_path)
        VALUES (?, ?, ?, ?)
    """, rows)


def main():
    log("build_album_images_from_albums started")
    log(f"Python: {sys.executable}")
    log(f"DB_PATH: {DB_PATH}")

    if not DB_PATH.exists():
        raise RuntimeError("gallery.db not found")

    conn = connect_db(DB_PATH)
    cur = conn.cursor()

    album_count = cur.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
    log(f"Albums found in DB: {album_count}")

    ensure_album_images_table(conn)
    clear_album_images_table(conn)

    rows = cur.execute("""
        SELECT id, name, folder_path
        FROM albums
        ORDER BY id
    """).fetchall()

    total = len(rows)
    start_time = time.time()
    last_print = 0.0

    inserted_albums = 0
    inserted_images = 0
    empty = 0
    missing_folder = 0
    errors = 0
    processed_since_commit = 0

    for idx, row in enumerate(rows, start=1):
        album_id = row["id"]
        folder = Path(row["folder_path"])

        try:
            if not folder.exists() or not folder.is_dir():
                missing_folder += 1
            else:
                image_names = scan_folder_images(folder)

                if not image_names:
                    empty += 1
                else:
                    insert_album_images(conn, album_id, folder, image_names)
                    inserted_albums += 1
                    inserted_images += len(image_names)

            processed_since_commit += 1

            if processed_since_commit >= BATCH_COMMIT_EVERY:
                conn.commit()
                processed_since_commit = 0

        except sqlite3.DatabaseError as e:
            log(f"[FATAL DB ERROR] album_id={album_id}, folder={folder}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            raise SystemExit(1)

        except Exception as e:
            errors += 1
            log(f"[ERROR] album_id={album_id}, folder={folder}: {e}")

        now = time.time()
        if (now - last_print >= PROGRESS_PRINT_INTERVAL) or idx == total:
            elapsed = now - start_time
            speed = idx / elapsed if elapsed > 0 else 0
            remain = total - idx
            eta = remain / speed if speed > 0 else -1
            percent = (idx / total * 100) if total > 0 else 100

            log(
                f"[{idx}/{total}] {percent:6.2f}% | "
                f"albums_with_images={inserted_albums}, total_images={inserted_images}, "
                f"empty={empty}, missing_folder={missing_folder}, errors={errors} | "
                f"elapsed={format_eta(elapsed)} | ETA={format_eta(eta)} | "
                f"speed={speed:.1f} albums/sec"
            )
            last_print = now

    if processed_since_commit > 0:
        conn.commit()

    chk = cur.execute("PRAGMA integrity_check").fetchone()[0]
    log(f"integrity_check: {chk}")

    conn.close()

    total_elapsed = time.time() - start_time
    log("Done.")
    log(f"Elapsed: {format_eta(total_elapsed)}")
    log(f"Albums with images: {inserted_albums}")
    log(f"Total cached images: {inserted_images}")
    log(f"Empty folders: {empty}")
    log(f"Missing folders: {missing_folder}")
    log(f"Errors: {errors}")


if __name__ == "__main__":
    main()
