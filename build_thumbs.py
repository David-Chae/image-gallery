import sqlite3
import sys
import time
from pathlib import Path

from PIL import Image, ImageOps

# =========================
# 설정
# =========================
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "gallery.db"
THUMB_CACHE_DIR = BASE_DIR / "thumb_cache"
THUMB_SIZE = (320, 320)

PROGRESS_PRINT_INTERVAL = 1.0
BATCH_COMMIT_EVERY = 100


def log(message: str):
    print(message, flush=True)


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
    return conn


def ensure_thumb_cache_dir():
    THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def make_thumbnail(source_path: Path, album_id: int) -> str:
    thumb_path = THUMB_CACHE_DIR / f"{album_id}.jpg"

    with Image.open(source_path) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        img.thumbnail(THUMB_SIZE)

        canvas = Image.new("RGB", THUMB_SIZE, (18, 18, 18))
        x = (THUMB_SIZE[0] - img.width) // 2
        y = (THUMB_SIZE[1] - img.height) // 2
        canvas.paste(img, (x, y))
        canvas.save(thumb_path, "JPEG", quality=85, optimize=True)

    return str(thumb_path)


def safe_commit(conn: sqlite3.Connection):
    try:
        conn.commit()
    except sqlite3.DatabaseError as e:
        raise RuntimeError(f"DB commit failed: {e}")


def build_thumbnails():
    log("Thumbnail generation started")
    log(f"Python: {sys.executable}")
    log(f"DB_PATH: {DB_PATH}")
    log(f"THUMB_CACHE_DIR: {THUMB_CACHE_DIR}")

    if not DB_PATH.exists():
        raise RuntimeError("gallery.db not found. Run build_index.py first.")

    ensure_thumb_cache_dir()

    conn = connect_db(DB_PATH)
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT a.id, a.name, a.thumb_path, ai.file_path
        FROM albums a
        JOIN album_images ai
          ON ai.album_id = a.id AND ai.image_index = 0
        ORDER BY a.id
    """).fetchall()

    total = len(rows)
    log(f"Albums eligible for thumbnail generation: {total}")

    start_time = time.time()
    last_print = 0.0
    generated = 0
    skipped = 0
    errors = 0
    processed_since_commit = 0

    for idx, row in enumerate(rows, start=1):
        album_id = row["id"]
        existing_thumb_path = row["thumb_path"]
        source_path = Path(row["file_path"])

        try:
            thumb_should_build = True

            if existing_thumb_path:
                thumb_file = Path(existing_thumb_path)
                if thumb_file.exists():
                    thumb_should_build = False

            if not source_path.exists():
                errors += 1
                log(f"[ERROR] Missing source image for album {album_id}: {source_path}")
            elif thumb_should_build:
                new_thumb_path = make_thumbnail(source_path, album_id)
                cur.execute("""
                    UPDATE albums
                    SET thumb_path = ?, updated_at = ?
                    WHERE id = ?
                """, (new_thumb_path, time.time(), album_id))
                generated += 1
                processed_since_commit += 1
            else:
                skipped += 1

            if processed_since_commit >= BATCH_COMMIT_EVERY:
                safe_commit(conn)
                processed_since_commit = 0

        except sqlite3.DatabaseError as e:
            log(f"[FATAL DB ERROR] album_id={album_id}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            conn.close()
            raise SystemExit(1)

        except Exception as e:
            errors += 1
            log(f"[ERROR] album_id={album_id}: {e}")

        now = time.time()
        if (now - last_print >= PROGRESS_PRINT_INTERVAL) or idx == total:
            elapsed = now - start_time
            speed = idx / elapsed if elapsed > 0 else 0
            remain = total - idx
            eta = remain / speed if speed > 0 else -1
            percent = (idx / total * 100) if total > 0 else 100

            log(
                f"[{idx}/{total}] {percent:6.2f}% | "
                f"generated={generated}, skipped={skipped}, errors={errors} | "
                f"elapsed={format_eta(elapsed)} | ETA={format_eta(eta)} | "
                f"speed={speed:.1f} albums/sec"
            )
            last_print = now

    if processed_since_commit > 0:
        safe_commit(conn)

    conn.close()

    total_elapsed = time.time() - start_time
    log("Done.")
    log(f"Elapsed: {format_eta(total_elapsed)}")
    log(f"Generated: {generated}")
    log(f"Skipped: {skipped}")
    log(f"Errors: {errors}")


if __name__ == "__main__":
    build_thumbnails()
