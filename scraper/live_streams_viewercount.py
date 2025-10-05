import asyncio
import aiohttp
import json
import time
import os
import sys
import signal

# Add parent directory to path so we can access root JSON files
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LIST_API = "https://frontend-api-v3.pump.fun/coins/currently-live"
DETAIL_API = "https://livestream-api.pump.fun/livestream?mintId={}"

# JSON files are in root directory (one level up from scraper/)
BLACKLIST_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "blacklist.json")
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "pumpfun_streams.json")

# Graceful shutdown flag
shutdown_flag = False

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    global shutdown_flag
    print("\n🛑 Shutting down gracefully...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

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
    except asyncio.TimeoutError:
        print(f"⏱️ Timeout fetching details for {mintId}")
        stream_info["viewers"] = 0
        stream_info["isLive"] = False
    except Exception as e:
        print(f"⚠️ Failed to fetch details for {mintId}: {e}")
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
        except asyncio.TimeoutError:
            print(f"⏱️ Timeout at offset {offset}, retrying...")
            await asyncio.sleep(2)
            continue
        except Exception as e:
            print(f"❌ Error fetching streams at offset {offset}: {e}")
            break

        if not data:
            break

        for coin in data:
            mint_id = coin.get("mint")
            if mint_id in blacklist:
                continue

            all_streams.append({
                "title": "Unknown Title",
                "streamerName": coin.get("name", "Unknown"),
                "gameCategory": "Crypto",
                "mintId": mint_id,
                "url": f"https://pump.fun/{mint_id}",
                "thumbnail": coin.get("image_uri", ""),
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
                    print(f"⚠️ Retry failed for {stream['mintId']}: {e}")
                    stream["title"] = f"Live Stream of {stream['streamerName']}"
        
        if retry_count > 0:
            print(f"🔄 Successfully retried {retry_count} streams")

    live_streams = [s for s in all_streams if s["isLive"]]
    return live_streams

async def main():
    print(f"📁 Blacklist file: {BLACKLIST_FILE}")
    print(f"📁 Output file: {OUTPUT_FILE}\n")
    
    async with aiohttp.ClientSession() as session:
        while not shutdown_flag:
            start_time = time.time()
            try:
                streams = await fetch_live_streams(session, limit=60, include_viewers=True)

                if not shutdown_flag:
                    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                        json.dump(streams, f, indent=2)

                    elapsed = time.time() - start_time
                    print(f"\n✅ Collected {len(streams)} live streams in {elapsed:.2f}s.")
                    print(f"⏱️  Waiting 90 seconds before next update...\n")
                
            except Exception as e:
                print(f"❌ Error in main loop: {e}")
                if not shutdown_flag:
                    print("⏱️  Waiting 30 seconds before retry...\n")
                    await asyncio.sleep(30)
                continue
            
            # Sleep in small intervals to check shutdown flag
            for _ in range(90):
                if shutdown_flag:
                    break
                await asyncio.sleep(1)
    
    print("👋 Scraper stopped.")

if __name__ == "__main__":
    asyncio.run(main())