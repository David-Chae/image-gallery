import sys
import time
import sqlite3
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

from PIL import Image, ImageOps

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "gallery.db"
THUMB_CACHE_DIR = BASE_DIR / "thumb_cache"

THUMB_SIZE = (320, 320)
JPEG_QUALITY = 82

# HDD면 2~4 권장, SSD/NVMe면 4~8 권장
MAX_WORKERS = 4

# 한 번에 워커 풀에 넣을 작업 수
CHUNK_SIZE = 2000

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


def ensure_thumb_cache_dir():
    THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_album_rows():
    if not DB_PATH.exists():
        raise RuntimeError("gallery.db not found")

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT id, folder_path, first_image_name
        FROM albums
        ORDER BY id
    """).fetchall()

    conn.close()

    tasks = []
    for row in rows:
        tasks.append((
            int(row["id"]),
            row["folder_path"],
            row["first_image_name"],
            str(THUMB_CACHE_DIR),
            THUMB_SIZE,
            JPEG_QUALITY,
        ))
    return tasks


def iter_chunks(items, chunk_size):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def make_one_thumbnail(task):
    """
    워커 프로세스에서 실행.
    반환값:
      ("generated" | "skipped_existing" | "missing_source" | "error", album_id, detail)
    """
    album_id, folder_path, first_image_name, thumb_cache_dir, thumb_size, jpeg_quality = task

    try:
        if not folder_path or not first_image_name:
            return ("missing_source", album_id, "missing folder_path or first_image_name")

        source_path = Path(folder_path) / first_image_name
        thumb_path = Path(thumb_cache_dir) / f"{album_id}.jpg"

        if thumb_path.exists():
            return ("skipped_existing", album_id, str(thumb_path))

        if not source_path.exists() or not source_path.is_file():
            return ("missing_source", album_id, str(source_path))

        thumb_path.parent.mkdir(parents=True, exist_ok=True)

        with Image.open(source_path) as img:
            img = ImageOps.exif_transpose(img)
            img = img.convert("RGB")
            img.thumbnail(tuple(thumb_size))

            canvas = Image.new("RGB", tuple(thumb_size), (18, 18, 18))
            x = (thumb_size[0] - img.width) // 2
            y = (thumb_size[1] - img.height) // 2
            canvas.paste(img, (x, y))
            canvas.save(thumb_path, "JPEG", quality=jpeg_quality, optimize=True)

        return ("generated", album_id, str(thumb_path))

    except Exception as e:
        return ("error", album_id, repr(e))


def main():
    log("build_thumbs_multiprocess_chunked started")
    log(f"Python: {sys.executable}")
    log(f"DB_PATH: {DB_PATH}")
    log(f"THUMB_CACHE_DIR: {THUMB_CACHE_DIR}")
    log(f"MAX_WORKERS: {MAX_WORKERS}")
    log(f"CHUNK_SIZE: {CHUNK_SIZE}")
    log(f"THUMB_SIZE: {THUMB_SIZE}")
    log(f"JPEG_QUALITY: {JPEG_QUALITY}")

    ensure_thumb_cache_dir()

    tasks = load_album_rows()
    total = len(tasks)

    log(f"Albums found: {total}")

    start_time = time.time()
    last_print = 0.0

    generated = 0
    skipped_existing = 0
    missing_source = 0
    errors = 0
    completed = 0
    chunk_no = 0
    total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE if total > 0 else 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for chunk in iter_chunks(tasks, CHUNK_SIZE):
            chunk_no += 1
            log(f"Submitting chunk {chunk_no}/{total_chunks} ({len(chunk)} tasks)")

            futures = [executor.submit(make_one_thumbnail, task) for task in chunk]

            for future in as_completed(futures):
                completed += 1

                try:
                    status, album_id, detail = future.result()

                    if status == "generated":
                        generated += 1
                    elif status == "skipped_existing":
                        skipped_existing += 1
                    elif status == "missing_source":
                        missing_source += 1
                    elif status == "error":
                        errors += 1
                        log(f"[ERROR] album_id={album_id}: {detail}")

                except Exception as e:
                    errors += 1
                    log(f"[FUTURE ERROR] {e}")

                now = time.time()
                if (now - last_print >= PROGRESS_PRINT_INTERVAL) or completed == total:
                    elapsed = now - start_time
                    speed = completed / elapsed if elapsed > 0 else 0
                    remain = total - completed
                    eta = remain / speed if speed > 0 else -1
                    percent = (completed / total * 100) if total > 0 else 100

                    log(
                        f"[{completed}/{total}] {percent:6.2f}% | "
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
