from flask import Flask, jsonify, send_from_directory
import json
import os
import threading
import asyncio
import aiohttp
import time

app = Flask(__name__, static_folder='static')

# File paths (in same container, no /data/ prefix)
BLACKLIST_FILE = "blacklist.json"
FEATURED_FILE = "featured.json"
STREAMS_FILE = "pumpfun_streams.json"
FEATURED_CACHE_FILE = "featured_cache.json"

# API endpoints
LIST_API = "https://frontend-api-v3.pump.fun/coins/currently-live"
DETAIL_API = "https://livestream-api.pump.fun/livestream?mintId={}"

shutdown_flag = False

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

# ===== SCRAPER CODE =====

async def fetch_stream_detail(session, stream_info):
    """Fetch viewer count, isLive, and stream title asynchronously."""
    mintId = stream_info["mintId"]
    try:
        async with session.get(DETAIL_API.format(mintId), timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status == 200:
                details = await resp.json()
                stream_info["viewers"] = details.get("numParticipants", 0)
                stream_info["isLive"] = details.get("isLive", False)
                stream_info["title"] = details.get("title") or stream_info["title"]
            else:
                stream_info["viewers"] = 0
                stream_info["isLive"] = False
    except Exception as e:
        stream_info["viewers"] = 0
        stream_info["isLive"] = False

def load_blacklist():
    """Load blacklist with error handling."""
    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except (json.JSONDecodeError, Exception) as e:
            print(f"⚠️ Error loading blacklist: {e}")
            return set()
    return set()

async def fetch_live_streams(session, limit=100, include_viewers=True):
    all_streams = []
    offset = 0
    blacklist = load_blacklist()

    while not shutdown_flag:
        params = {
            "offset": offset,
            "limit": limit,
            "sort": "currently_live",
            "order": "DESC",
            "includeNsfw": "false"
        }
        print(f"📡 Fetching streams offset={offset} limit={limit}...")
        
        try:
            async with session.get(LIST_API, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
                r.raise_for_status()
                data = await r.json()
        except Exception as e:
            print(f"❌ Error fetching streams at offset {offset}: {e}")
            break

        if not data:
            break

        for coin in data:
            mint_id = coin.get("mint")
            if mint_id in blacklist:
                continue

            # Get thumbnail and replace IPFS gateway for better reliability
            thumbnail = coin.get("image_uri", "")
            if thumbnail and "ipfs.io/ipfs/" in thumbnail:
                thumbnail = thumbnail.replace("ipfs.io/ipfs/", "dweb.link/ipfs/")
            elif thumbnail and "cf-ipfs.com/ipfs/" in thumbnail:
                thumbnail = thumbnail.replace("cf-ipfs.com/ipfs/", "dweb.link/ipfs/")

            all_streams.append({
                "title": "Unknown Title",
                "streamerName": coin.get("name", "Unknown"),
                "gameCategory": "Crypto",
                "mintId": mint_id,
                "url": f"https://pump.fun/{mint_id}",
                "thumbnail": thumbnail,
                "viewers": 0,
                "isLive": False
            })

        if len(data) < limit:
            break

        offset += limit
        await asyncio.sleep(0.1)

    if include_viewers and not shutdown_flag:
        print("⚡ Fetching viewer counts and titles asynchronously...")
        tasks = [fetch_stream_detail(session, s) for s in all_streams]
        await asyncio.gather(*tasks)

        retry_count = 0
        for stream in all_streams:
            if shutdown_flag:
                break
            if stream["title"] == "Unknown Title":
                try:
                    async with session.get(
                        DETAIL_API.format(stream["mintId"]), 
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            details = await resp.json()
                            stream["title"] = details.get("title") or f"Live Stream of {stream['streamerName']}"
                            stream["viewers"] = details.get("numParticipants", 0)
                            stream["isLive"] = details.get("isLive", False)
                            retry_count += 1
                except Exception as e:
                    stream["title"] = f"Live Stream of {stream['streamerName']}"
        
        if retry_count > 0:
            print(f"🔄 Successfully retried {retry_count} streams")

    live_streams = [s for s in all_streams if s["isLive"]]
    return live_streams

async def run_scraper():
    """Main scraper loop"""
    print("🚀 Starting scraper in background thread...")
    async with aiohttp.ClientSession() as session:
        while not shutdown_flag:
            start_time = time.time()
            try:
                streams = await fetch_live_streams(session, limit=60, include_viewers=True)

                if not shutdown_flag:
                    with open(STREAMS_FILE, "w", encoding="utf-8") as f:
                        json.dump(streams, f, indent=2)

                    elapsed = time.time() - start_time
                    print(f"\n✅ Collected {len(streams)} live streams in {elapsed:.2f}s.")
                    print(f"⏱️  Waiting 90 seconds before next update...\n")
                
            except Exception as e:
                print(f"❌ Error in scraper loop: {e}")
                if not shutdown_flag:
                    await asyncio.sleep(30)
                continue
            
            # Sleep in small intervals to check shutdown flag
            for _ in range(90):
                if shutdown_flag:
                    break
                await asyncio.sleep(1)
    
    print("👋 Scraper stopped.")

def start_scraper_thread():
    """Start the scraper in a background thread"""
    def run_async_scraper():
        asyncio.run(run_scraper())
    
    scraper_thread = threading.Thread(target=run_async_scraper, daemon=True)
    scraper_thread.start()
    print("✅ Scraper thread started")

# ===== FLASK ROUTES =====

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

# Initialize empty JSON files if they don't exist
def initialize_files():
    """Create empty JSON files if they don't exist"""
    files = {
        BLACKLIST_FILE: [],
        FEATURED_FILE: [],
        FEATURED_CACHE_FILE: {}
    }
    
    for filepath, default_content in files.items():
        if not os.path.exists(filepath):
            with open(filepath, 'w') as f:
                json.dump(default_content, f)
            print(f"✅ Created {filepath}")

# Always initialize and start scraper on import
initialize_files()
start_scraper_thread()

if __name__ == '__main__':
    # Local development
    app.run(debug=True, host='0.0.0.0', port=5000)
