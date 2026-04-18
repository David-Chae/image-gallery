import math
import sqlite3
from pathlib import Path
import hashlib
import time

from flask import Flask, render_template, request, abort, send_file, jsonify
from PIL import Image, ImageOps

app = Flask(__name__)

from config import (
    DB_PATH,
    THUMB_CACHE_DIR,
    THUMB_SIZE,
    PAGE_SIZE_DEFAULT,
    PAGE_SIZE_MAX,
    PLACEHOLDER_PATH,
    HOST,
    PORT,
    DEBUG,
)


def ensure_thumb_cache_dir():
    THUMB_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def make_thumb_cache_name(folder_path: str, source_path: Path) -> str:
    st = source_path.stat()
    key = f"{folder_path}|{source_path.name}|{st.st_size}|{st.st_mtime_ns}"
    digest = hashlib.sha1(key.encode("utf-8", errors="ignore")).hexdigest()
    return f"{digest}.jpg"


def build_thumbnail_if_missing(source_path: Path, cache_path: Path):
    if cache_path.exists():
        return

    ensure_thumb_cache_dir()

    with Image.open(source_path) as img:
        img = ImageOps.exif_transpose(img)
        img = img.convert("RGB")
        img.thumbnail(THUMB_SIZE)

        canvas = Image.new("RGB", THUMB_SIZE, (18, 18, 18))
        x = (THUMB_SIZE[0] - img.width) // 2
        y = (THUMB_SIZE[1] - img.height) // 2
        canvas.paste(img, (x, y))
        canvas.save(cache_path, "JPEG", quality=75, optimize=True)


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def clamp_page_size(value: int):
    if value < 1:
        return PAGE_SIZE_DEFAULT
    return min(value, PAGE_SIZE_MAX)


def get_cached_album_count(conn):
    cur = conn.cursor()
    row = cur.execute("""
        SELECT value
        FROM app_meta
        WHERE key = 'album_count'
    """).fetchone()

    if not row:
        return None

    try:
        return int(row["value"])
    except (TypeError, ValueError):
        return None


@app.route("/")
def index():
    query = request.args.get("q", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    page_size = clamp_page_size(request.args.get("page_size", PAGE_SIZE_DEFAULT, type=int))
    offset = (page - 1) * page_size

    t0 = time.time()

    conn = get_conn()
    cur = conn.cursor()
    print("DB open:", time.time() - t0, flush=True)

    t1 = time.time()

    if query:
        # search mode: exact total_pages
        terms = [term.strip() for term in query.split() if term.strip()]

        where_clauses = []
        params = []

        for term in terms:
            where_clauses.append("name LIKE ?")
            params.append(f"%{term}%")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        count_sql = f"""
            SELECT COUNT(*)
            FROM albums
            WHERE {where_sql}
        """

        rows_sql = f"""
            SELECT id, name, image_count
            FROM albums
            WHERE {where_sql}
            ORDER BY id
            LIMIT ? OFFSET ?
        """

        total = cur.execute(count_sql, tuple(params)).fetchone()[0]
        rows = cur.execute(rows_sql, (*params, page_size, offset)).fetchall()

        total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 1
        has_prev = page > 1
        has_next = page < total_pages
        search_mode = "like_name_and"

    else:
        # browse mode: cached total_pages
        cached_total = get_cached_album_count(conn)

        rows = cur.execute("""
            SELECT id, name, image_count
            FROM albums
            ORDER BY id
            LIMIT ? OFFSET ?
        """, (page_size, offset)).fetchall()

        total = cached_total
        total_pages = max(1, math.ceil(total / page_size)) if total and total > 0 else 1
        has_prev = page > 1
        has_next = page < total_pages
        search_mode = "browse"

    print("rows query:", time.time() - t1, flush=True)

    t2 = time.time()
    conn.close()
    print("close:", time.time() - t2, flush=True)

    t3 = time.time()
    albums = [{
        "id": row["id"],
        "name": row["name"],
        "image_count": row["image_count"],
        "thumbnail_url": f"/thumb/{row['id']}"
    } for row in rows]
    print("serialize:", time.time() - t3, flush=True)

    t4 = time.time()
    response = render_template(
        "index.html",
        albums=albums,
        query=query,
        page=page,
        page_size=page_size,
        total=total,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        search_mode=search_mode
    )
    print("render_template:", time.time() - t4, flush=True)
    print("index total:", time.time() - t0, flush=True)

    return response


@app.route("/album/<int:album_id>")
def album_view(album_id: int):
    conn = get_conn()
    cur = conn.cursor()

    album_row = cur.execute("""
        SELECT id, name, image_count
        FROM albums
        WHERE id = ?
    """, (album_id,)).fetchone()

    if not album_row:
        conn.close()
        abort(404)

    image_rows = cur.execute("""
        SELECT image_index, file_name
        FROM album_images
        WHERE album_id = ?
        ORDER BY image_index ASC
    """, (album_id,)).fetchall()

    conn.close()

    images = [{
        "index": row["image_index"],
        "name": row["file_name"],
        "url": f"/image/{album_id}/{row['image_index']}"
    } for row in image_rows]

    return render_template(
        "album.html",
        album={
            "id": album_row["id"],
            "name": album_row["name"],
            "image_count": album_row["image_count"]
        },
        images=images
    )


@app.route("/thumb/<int:album_id>")
def serve_thumbnail(album_id: int):
    thumb_path = THUMB_CACHE_DIR / f"{album_id}.jpg"
    if thumb_path.exists():
        return send_file(thumb_path)
    if PLACEHOLDER_PATH.exists():
        return send_file(PLACEHOLDER_PATH)
    abort(404)


@app.route("/image/<int:album_id>/<int:image_index>")
def serve_image(album_id: int, image_index: int):
    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute("""
        SELECT file_path
        FROM album_images
        WHERE album_id = ? AND image_index = ?
    """, (album_id, image_index)).fetchone()

    conn.close()

    if not row:
        abort(404)

    file_path = Path(row["file_path"])
    if not file_path.exists():
        abort(404)

    return send_file(file_path)


@app.route("/api/albums")
def api_albums():
    query = request.args.get("q", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    page_size = clamp_page_size(request.args.get("page_size", PAGE_SIZE_DEFAULT, type=int))
    offset = (page - 1) * page_size

    conn = get_conn()
    cur = conn.cursor()

    if query:
        terms = [term.strip() for term in query.split() if term.strip()]

        where_clauses = []
        params = []

        for term in terms:
            where_clauses.append("name LIKE ?")
            params.append(f"%{term}%")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        count_sql = f"""
            SELECT COUNT(*)
            FROM albums
            WHERE {where_sql}
        """

        rows_sql = f"""
            SELECT id, name, image_count
            FROM albums
            WHERE {where_sql}
            ORDER BY id
            LIMIT ? OFFSET ?
        """

        total = cur.execute(count_sql, tuple(params)).fetchone()[0]
        rows = cur.execute(rows_sql, (*params, page_size, offset)).fetchall()
        total_pages = max(1, math.ceil(total / page_size)) if total > 0 else 1
        search_mode = "like_name_and"

    else:
        total = get_cached_album_count(conn)
        rows = cur.execute("""
            SELECT id, name, image_count
            FROM albums
            ORDER BY id
            LIMIT ? OFFSET ?
        """, (page_size, offset)).fetchall()
        total_pages = max(1, math.ceil(total / page_size)) if total and total > 0 else 1
        search_mode = "browse"

    conn.close()

    return jsonify({
        "query": query,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "search_mode": search_mode,
        "albums": [{
            "id": row["id"],
            "name": row["name"],
            "image_count": row["image_count"],
            "thumbnail_url": f"/thumb/{row['id']}",
            "album_url": f"/album/{row['id']}"
        } for row in rows]
    })


@app.route("/api/album/<int:album_id>")
def api_album(album_id: int):
    conn = get_conn()
    cur = conn.cursor()

    album_row = cur.execute("""
        SELECT id, name, image_count
        FROM albums
        WHERE id = ?
    """, (album_id,)).fetchone()

    if not album_row:
        conn.close()
        abort(404)

    image_rows = cur.execute("""
        SELECT image_index, file_name, file_path
        FROM album_images
        WHERE album_id = ?
        ORDER BY image_index ASC
    """, (album_id,)).fetchall()

    conn.close()

    return jsonify({
        "album": {
            "id": album_row["id"],
            "name": album_row["name"],
            "image_count": album_row["image_count"]
        },
        "images": [{
            "index": row["image_index"],
            "name": row["file_name"],
            "file_path": row["file_path"],
            "url": f"/image/{album_id}/{row['image_index']}"
        } for row in image_rows]
    })


@app.route("/api/health")
def api_health():
    db_exists = DB_PATH.exists()
    thumb_exists = THUMB_CACHE_DIR.exists()
    album_count = 0
    image_count = 0

    if db_exists:
        conn = get_conn()
        cur = conn.cursor()
        album_count = cur.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        image_count = cur.execute("SELECT COUNT(*) FROM album_images").fetchone()[0]
        conn.close()

    return jsonify({
        "status": "ok",
        "db_exists": db_exists,
        "thumb_cache_exists": thumb_exists,
        "db_path": str(DB_PATH),
        "thumb_cache_dir": str(THUMB_CACHE_DIR),
        "album_count": album_count,
        "cached_image_rows": image_count
    })


if __name__ == "__main__":
    if not DB_PATH.exists():
        print("gallery.db not found. Run build_index.py first.", flush=True)
    app.run(host=HOST, port=PORT, debug=DEBUG)
