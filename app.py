from flask import Flask, jsonify, send_from_directory
import json
import os

app = Flask(__name__, static_folder='static')

# File paths
BLACKLIST_FILE = "/data/blacklist.json"
FEATURED_FILE = "/data/featured.json"
STREAMS_FILE = "/data/pumpfun_streams.json"
FEATURED_CACHE_FILE = "/data/featured_cache.json"

def load_json_set(file_path):
    """Load a JSON file as a set for fast lookup, return empty set if missing."""
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error loading {file_path}: {e}")
            return set()
    return set()

def load_featured_list():
    """Load featured.json as a list of mintIds."""
    if os.path.exists(FEATURED_FILE):
        try:
            with open(FEATURED_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error loading {FEATURED_FILE}: {e}")
            return []
    return []

def load_featured_cache():
    """Load cached featured stream data."""
    if os.path.exists(FEATURED_CACHE_FILE):
        try:
            with open(FEATURED_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            print(f"Error loading {FEATURED_CACHE_FILE}: {e}")
            return {}
    return {}

def save_featured_cache(cache_data):
    """Save featured stream cache to file."""
    try:
        with open(FEATURED_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"Error saving featured cache: {e}")

@app.route('/api/streams')
def get_streams():
    if not os.path.exists(STREAMS_FILE):
        return jsonify({"streams": []})

    try:
        with open(STREAMS_FILE, "r", encoding="utf-8") as f:
            streams = json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error loading streams: {e}")
        return jsonify({"streams": [], "error": "Failed to load streams"})

    blacklist = load_json_set(BLACKLIST_FILE)
    featured_ids = set(load_featured_list())
    featured_cache = load_featured_cache()

    # Update cache with current live featured streams
    for s in streams:
        mint_id = s.get("mintId")
        if mint_id in featured_ids and s.get("isLive"):
            viewer_count = s.get("viewers")
            if viewer_count is None:
                viewer_count = 0
            
            featured_cache[mint_id] = {
                "title": s.get("title", "Unknown Stream"),
                "thumbnail": s.get("thumbnail", ""),
                "viewerCount": viewer_count,
                "streamerName": s.get("streamerName", "Unknown"),
                "gameCategory": s.get("gameCategory", "Unknown"),
                "url": s.get("url", "#"),
                "mintId": mint_id
            }

    # Remove cached streams that are no longer featured
    featured_cache = {k: v for k, v in featured_cache.items() if k in featured_ids}
    save_featured_cache(featured_cache)

    transformed = []
    featured_mint_ids_processed = set()

    # Process current streams
    for s in streams:
        mint_id = s.get("mintId")
        if mint_id in blacklist:
            continue

        viewer_count = s.get("viewers")
        if viewer_count is None:
            viewer_count = 0

        stream_data = {
            "title": s.get("title", "Unknown Stream"),
            "thumbnail": s.get("thumbnail", ""),
            "viewerCount": viewer_count,
            "streamerName": s.get("streamerName", "Unknown"),
            "gameCategory": s.get("gameCategory", "Unknown"),
            "url": s.get("url", "#"),
            "featured": mint_id in featured_ids,
            "isLive": s.get("isLive", False)
        }

        transformed.append(stream_data)
        
        if mint_id in featured_ids:
            featured_mint_ids_processed.add(mint_id)

    # Add offline featured streams from cache
    for mint_id in featured_ids:
        if mint_id not in featured_mint_ids_processed and mint_id in featured_cache:
            cached = featured_cache[mint_id]
            transformed.append({
                "title": cached.get("title", "Unknown Stream"),
                "thumbnail": cached.get("thumbnail", ""),
                "viewerCount": 0,
                "streamerName": cached.get("streamerName", "Unknown"),
                "gameCategory": cached.get("gameCategory", "Unknown"),
                "url": cached.get("url", "#"),
                "featured": True,
                "isLive": False
            })

    return jsonify({"streams": transformed})

@app.route('/health')
def health():
    """Health check endpoint for monitoring"""
    return jsonify({"status": "healthy"}), 200

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files (images, etc.)"""
    return send_from_directory('static', filename)

if __name__ == '__main__':
    # Local development
    app.run(debug=True, host='0.0.0.0', port=5000)
else:
    # Production (Gunicorn will handle this)
    pass
