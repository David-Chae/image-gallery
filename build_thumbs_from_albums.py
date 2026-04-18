import sqlite3
import sys
import time
from pathlib import Path

from PIL import Image, ImageOps

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "gallery.db"
THUMB_CACHE_DIR = BASE_DIR / "thumb_cache"

THUMB_SIZE = (320, 320)
PROGRESS_PRINT_INTERVAL = 1.0


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
    conn = sqlite3.connect(str(db_path), timeout=60)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_thumb_cache_dir():
    THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def make_thumbnail(source_path: Path, album_id: int) -> Path:
    thumb_path = THUMB_CACHE_DIR / f"{album_id}.jpg"

    with Image.open(source_path) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        img.thumbnail(THUMB_SIZE)

        canvas = Image.new("RGB", THUMB_SIZE, (18, 18, 18))
        x = (THUMB_SIZE[0] - img.width) // 2
        y = (THUMB_SIZE[1] - img.height) // 2
        canvas.paste(img, (x, y))
        canvas.save(thumb_path, "JPEG", quality=82, optimize=True)

    return thumb_path


def main():
    log("build_thumbs_from_albums started")
    log(f"Python: {sys.executable}")
    log(f"DB_PATH: {DB_PATH}")
    log(f"THUMB_CACHE_DIR: {THUMB_CACHE_DIR}")

    if not DB_PATH.exists():
        raise RuntimeError("gallery.db not found")

    ensure_thumb_cache_dir()

    conn = connect_db(DB_PATH)
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id, name, folder_path, first_image_name
        FROM albums
        ORDER BY id
    """).fetchall()

    conn.close()

    total = len(rows)
    log(f"Albums found: {total}")

    start_time = time.time()
    last_print = 0.0

    generated = 0
    skipped_existing = 0
    missing_source = 0
    errors = 0

    for idx, row in enumerate(rows, start=1):
        album_id = row["id"]
        folder_path = row["folder_path"]
        first_image_name = row["first_image_name"]

        try:
            if not folder_path or not first_image_name:
                missing_source += 1
            else:
                source_path = Path(folder_path) / first_image_name
                thumb_path = THUMB_CACHE_DIR / f"{album_id}.jpg"

                if thumb_path.exists():
                    skipped_existing += 1
                elif not source_path.exists() or not source_path.is_file():
                    missing_source += 1
                else:
                    make_thumbnail(source_path, album_id)
                    generated += 1

        except Exception as e:
            errors += 1
            log(f"[ERROR] album_id={album_id}, folder={folder_path}: {e}")

        now = time.time()
        if (now - last_print >= PROGRESS_PRINT_INTERVAL) or idx == total:
            elapsed = now - start_time
            speed = idx / elapsed if elapsed > 0 else 0
            remain = total - idx
            eta = remain / speed if speed > 0 else -1
            percent = (idx / total * 100) if total > 0 else 100

            log(
                f"[{idx}/{total}] {percent:6.2f}% | "
                f"generated={generated}, skipped_existing={skipped_existing}, "
                f"missing_source={missing_source}, errors={errors} | "
                f"elapsed={format_eta(elapsed)} | ETA={format_eta(eta)} | "
                f"speed={speed:.1f} albums/sec"
            )
            last_print = now

    total_elapsed = time.time() - start_time
    log("Done.")
    log(f"Elapsed: {format_eta(total_elapsed)}")
    log(f"Generated: {generated}")
    log(f"Skipped existing: {skipped_existing}")
    log(f"Missing source: {missing_source}")
    log(f"Errors: {errors}")


if __name__ == "__main__":
    main()
