from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parent

# ---- Core paths ----
# Please replace "E:\" with your root directory containing folders of images.
ROOT_DIR = Path(os.environ.get("IMAGE_GALLERY_ROOT", r"E:\")) 
DB_PATH = PROJECT_ROOT / "gallery.db"
THUMB_CACHE_DIR = PROJECT_ROOT / "thumb_cache"
STATIC_DIR = PROJECT_ROOT / "static"
TEMPLATES_DIR = PROJECT_ROOT / "templates"
PLACEHOLDER_PATH = STATIC_DIR / "placeholder.jpg"

# ---- Gallery / thumbnails ----
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}
THUMB_SIZE = (320, 320)
JPEG_QUALITY = 75

# ---- Web app ----
PAGE_SIZE_DEFAULT = 25
PAGE_SIZE_MAX = 200
HOST = "0.0.0.0"
PORT = 5000
DEBUG = False

# ---- Batch indexing ----
ENABLE_THUMBNAILS_DURING_INDEX = False
DISCOVERY_PRINT_EVERY = 2000
PROGRESS_PRINT_INTERVAL = 1.0
BATCH_COMMIT_EVERY = 200

# ---- Thumbnail batch generation ----
MAX_WORKERS = 4
CHUNK_SIZE = 2000

# ---- DB behavior ----
SQLITE_JOURNAL_MODE = "DELETE"
SQLITE_SYNCHRONOUS = "FULL"

# ---- Optional safety flags ----
SKIP_STARTUP_INTEGRITY_CHECK = True
SKIP_FINAL_INTEGRITY_CHECK = True
SKIP_FTS_REBUILD = True
