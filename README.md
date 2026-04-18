# image-gallery

Local image gallery for very large folder collections.

This project indexes local folders into SQLite, generates thumbnail caches, and serves a searchable web gallery with slideshow viewing.

## Stack

- Flask web app
- SQLite metadata/index
- Pillow thumbnail generation

## Main workflow

1. Build album index
2. Generate thumbnails
3. Run web app

## Commands

```bash
pip install -r requirements.txt
python build_index.py
python build_thumbs_multiprocess_chunked.py
python app.py
