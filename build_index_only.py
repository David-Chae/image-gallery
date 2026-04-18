import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# =========================
# 설정
# =========================
BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = Path(r"E:\miscellaneous\hitomi")   # 반드시 수정
DB_PATH = BASE_DIR / "gallery.db"

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}

DISCOVERY_PRINT_EVERY = 2000
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
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")

    return conn


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS albums (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            folder_path TEXT NOT NULL UNIQUE,
            thumbnail_file TEXT,
            image_count INTEGER NOT NULL,
            first_image_name TEXT,
            approx_signature TEXT,
            thumb_path TEXT,
            updated_at REAL NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS album_images (
            album_id INTEGER NOT NULL,
            image_index INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            PRIMARY KEY (album_id, image_index),
            FOREIGN KEY (album_id) REFERENCES albums(id) ON DELETE CASCADE
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_albums_name
        ON albums(name)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_albums_folder_path
        ON albums(folder_path)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_album_images_album_id
        ON album_images(album_id)
    """)

    cur.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS albums_fts
        USING fts5(
            name,
            folder_path,
            content='albums',
            content_rowid='id'
        )
    """)

    conn.commit()


def rebuild_fts(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM albums_fts")
    cur.execute("""
        INSERT INTO albums_fts(rowid, name, folder_path)
        SELECT id, name, folder_path
        FROM albums
    """)
    conn.commit()


def db_integrity_check(conn: sqlite3.Connection):
    cur = conn.cursor()
    row = cur.execute("PRAGMA integrity_check;").fetchone()
    if not row:
        raise RuntimeError("PRAGMA integrity_check returned no result")
    result = row[0]
    if result != "ok":
        raise RuntimeError(f"SQLite integrity check failed: {result}")


def safe_commit(conn: sqlite3.Connection):
    try:
        conn.commit()
    except sqlite3.DatabaseError as e:
        raise RuntimeError(f"DB commit failed: {e}")


def discover_folders(root_dir: Path) -> List[Path]:
    folders = []
    count = 0
    start = time.time()

    log("Discovering folders...")

    with os.scandir(root_dir) as it:
        for entry in it:
            try:
                if entry.is_dir(follow_symlinks=False):
                    folders.append(Path(entry.path))
                    count += 1

                    if count % DISCOVERY_PRINT_EVERY == 0:
                        elapsed = time.time() - start
                        speed = count / elapsed if elapsed > 0 else 0
                        log(f"  discovered {count} folders... ({speed:.1f} folders/sec)")
            except Exception as e:
                log(f"[DISCOVERY ERROR] {entry.path}: {e}")

    log(f"Folder discovery done. Found {len(folders)} folders.")
    log("Sorting folders...")
    folders.sort(key=lambda p: p.name.lower())
    log("Sorting done.")

    return folders


def scan_folder_images_light(folder: Path) -> Tuple[int, Optional[str], Optional[str], List[str]]:
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
        return 0, None, None, []

    if not image_names:
        return 0, None, None, []

    image_names.sort(key=natural_sort_key_name)

    image_count = len(image_names)
    first_image_name = image_names[0]
    last_image_name = image_names[-1]
    approx_signature = f"{first_image_name}|{last_image_name}|{image_count}"

    return image_count, first_image_name, approx_signature, image_names


def get_existing_album_map(conn: sqlite3.Connection) -> Dict[str, dict]:
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT id, name, folder_path, thumbnail_file, image_count,
               first_image_name, approx_signature, thumb_path, updated_at
        FROM albums
    """).fetchall()

    result = {}
    for row in rows:
        result[row["folder_path"]] = {
            "id": row["id"],
            "name": row["name"],
            "folder_path": row["folder_path"],
            "thumbnail_file": row["thumbnail_file"],
            "image_count": row["image_count"],
            "first_image_name": row["first_image_name"],
            "approx_signature": row["approx_signature"],
            "thumb_path": row["thumb_path"],
            "updated_at": row["updated_at"],
        }
    return result


def insert_album(
    conn: sqlite3.Connection,
    name: str,
    folder_path: str,
    thumbnail_file: str,
    image_count: int,
    first_image_name: str,
    approx_signature: str,
):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO albums (
            name, folder_path, thumbnail_file, image_count,
            first_image_name, approx_signature, thumb_path, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name,
        folder_path,
        thumbnail_file,
        image_count,
        first_image_name,
        approx_signature,
        None,
        time.time(),
    ))
    return cur.lastrowid


def update_album(
    conn: sqlite3.Connection,
    album_id: int,
    name: str,
    folder_path: str,
    thumbnail_file: str,
    image_count: int,
    first_image_name: str,
    approx_signature: str,
    thumb_path: Optional[str],
):
    cur = conn.cursor()
    cur.execute("""
        UPDATE albums
        SET name = ?,
            folder_path = ?,
            thumbnail_file = ?,
            image_count = ?,
            first_image_name = ?,
            approx_signature = ?,
            thumb_path = ?,
            updated_at = ?
        WHERE id = ?
    """, (
        name,
        folder_path,
        thumbnail_file,
        image_count,
        first_image_name,
        approx_signature,
        thumb_path,
        time.time(),
        album_id,
    ))


def replace_album_images(conn: sqlite3.Connection, album_id: int, folder: Path, image_names: List[str]):
    cur = conn.cursor()
    cur.execute("DELETE FROM album_images WHERE album_id = ?", (album_id,))

    rows = [
        (album_id, idx, name, str(folder / name))
        for idx, name in enumerate(image_names)
    ]

    cur.executemany("""
        INSERT INTO album_images (album_id, image_index, file_name, file_path)
        VALUES (?, ?, ?, ?)
    """, rows)


def delete_album(conn: sqlite3.Connection, album_id: int):
    cur = conn.cursor()
    cur.execute("DELETE FROM albums WHERE id = ?", (album_id,))


def remove_missing_albums(conn: sqlite3.Connection, existing_map: Dict[str, dict], current_folder_paths: set) -> int:
    removed = 0
    for folder_path, row in existing_map.items():
        if folder_path not in current_folder_paths:
            thumb_path = row.get("thumb_path")
            if thumb_path:
                try:
                    thumb_file = Path(thumb_path)
                    if thumb_file.exists():
                        thumb_file.unlink()
                except Exception:
                    pass

            delete_album(conn, row["id"])
            removed += 1

    return removed


def process_folder(conn: sqlite3.Connection, folder: Path, existing: Optional[dict]) -> str:
    image_count, first_image_name, approx_signature, image_names = scan_folder_images_light(folder)

    if image_count == 0 or not first_image_name or not approx_signature:
        return "skipped_empty"

    folder_name = folder.name
    folder_path_str = str(folder)
    thumbnail_file = first_image_name

    if existing:
        unchanged = (
            existing["name"] == folder_name and
            existing["image_count"] == image_count and
            existing["first_image_name"] == first_image_name and
            existing["approx_signature"] == approx_signature and
            existing["thumbnail_file"] == thumbnail_file
        )

        if unchanged:
            return "unchanged"

        update_album(
            conn=conn,
            album_id=existing["id"],
            name=folder_name,
            folder_path=folder_path_str,
            thumbnail_file=thumbnail_file,
            image_count=image_count,
            first_image_name=first_image_name,
            approx_signature=approx_signature,
            thumb_path=existing["thumb_path"],
        )
        replace_album_images(conn, existing["id"], folder, image_names)
        return "updated"

    album_id = insert_album(
        conn=conn,
        name=folder_name,
        folder_path=folder_path_str,
        thumbnail_file=thumbnail_file,
        image_count=image_count,
        first_image_name=first_image_name,
        approx_signature=approx_signature,
    )
    replace_album_images(conn, album_id, folder, image_names)
    return "inserted"


def build_index():
    log("Script started")
    log(f"Python: {sys.executable}")
    log(f"ROOT_DIR: {ROOT_DIR}")
    log(f"DB_PATH: {DB_PATH}")
    log(f"BATCH_COMMIT_EVERY: {BATCH_COMMIT_EVERY}")

    if not ROOT_DIR.exists() or not ROOT_DIR.is_dir():
        raise RuntimeError(f"ROOT_DIR does not exist or is not a directory: {ROOT_DIR}")

    conn = connect_db(DB_PATH)

    try:
        init_db(conn)
        db_integrity_check(conn)
    except Exception:
        conn.close()
        raise

    existing_map = get_existing_album_map(conn)
    log(f"Existing indexed albums: {len(existing_map)}")

    folders = discover_folders(ROOT_DIR)
    total = len(folders)

    start_time = time.time()
    last_print = 0.0

    inserted = 0
    updated = 0
    unchanged = 0
    skipped_empty = 0
    errors = 0
    processed_since_commit = 0

    current_folder_paths = set()

    log("Indexing started...")

    for idx, folder in enumerate(folders, start=1):
        current_folder_paths.add(str(folder))
        existing = existing_map.get(str(folder))

        try:
            result = process_folder(conn, folder, existing)

            if result == "inserted":
                inserted += 1
            elif result == "updated":
                updated += 1
            elif result == "unchanged":
                unchanged += 1
            elif result == "skipped_empty":
                skipped_empty += 1

            processed_since_commit += 1

            if processed_since_commit >= BATCH_COMMIT_EVERY:
                safe_commit(conn)
                processed_since_commit = 0

        except sqlite3.DatabaseError as e:
            errors += 1
            log(f"[FATAL DB ERROR] {folder}: {e}")
            log("Database appears unhealthy. Stopping immediately.")
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            raise SystemExit(1)

        except Exception as e:
            errors += 1
            log(f"[ERROR] {folder}: {e}")

        now = time.time()
        if (now - last_print >= PROGRESS_PRINT_INTERVAL) or idx == total:
            elapsed = now - start_time
            speed = idx / elapsed if elapsed > 0 else 0
            remain = total - idx
            eta = remain / speed if speed > 0 else -1
            percent = (idx / total * 100) if total > 0 else 100

            log(
                f"[{idx}/{total}] {percent:6.2f}% | "
                f"inserted={inserted}, updated={updated}, unchanged={unchanged}, "
                f"empty={skipped_empty}, errors={errors} | "
                f"elapsed={format_eta(elapsed)} | ETA={format_eta(eta)} | "
                f"speed={speed:.1f} folders/sec"
            )
            last_print = now

    if processed_since_commit > 0:
        safe_commit(conn)

    removed = remove_missing_albums(conn, existing_map, current_folder_paths)
    safe_commit(conn)

    log("Rebuilding FTS index...")
    rebuild_fts(conn)

    log("Running final DB integrity check...")
    db_integrity_check(conn)

    conn.close()

    total_elapsed = time.time() - start_time
    log("Done.")
    log(f"Elapsed: {format_eta(total_elapsed)}")
    log(f"Inserted: {inserted}")
    log(f"Updated: {updated}")
    log(f"Unchanged: {unchanged}")
    log(f"Skipped empty: {skipped_empty}")
    log(f"Removed missing: {removed}")
    log(f"Errors: {errors}")


if __name__ == "__main__":
    build_index()
