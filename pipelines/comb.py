import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datetime import datetime
import requests
from googleapiclient.discovery import build
from google.cloud import bigquery
import os
import asyncio
from TikTokApi import TikTokApi
from dotenv import load_dotenv
import time
import random
try:
    import nest_asyncio  # for event loop reuse in notebooks/runners
    nest_asyncio.apply()
except Exception:
    pass

# Load environment variables from .env file
load_dotenv()

# -----------------------------
# Beam options (from env for multi-org reuse)
# -----------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET = os.environ.get("BQ_DATASET_ANALYTICS", "")

# Main tables
TWITTER_OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET}.tweets"
YOUTUBE_OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET}.youtube_videos"
LINKEDIN_OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET}.linkedin_posts_v2"
TIKTOK_OUTPUT_TABLE = f"{PROJECT_ID}:{DATASET}.tiktok_videos"
GA4_TABLE = f"{PROJECT_ID}:{DATASET}.events_20250930"

# Staging tables for UPSERT pattern
TWITTER_STAGING_TABLE = f"{PROJECT_ID}:{DATASET}.tweets_staging"
YOUTUBE_STAGING_TABLE = f"{PROJECT_ID}:{DATASET}.youtube_videos_staging"
LINKEDIN_STAGING_TABLE = f"{PROJECT_ID}:{DATASET}.linkedin_posts_v2_staging"

BEAM_TEMP = os.environ.get("BEAM_TEMP_LOCATION", "")
if not PROJECT_ID or not DATASET:
    raise SystemExit("Set GCP_PROJECT and BQ_DATASET_ANALYTICS in .env (see .env.example)")
if not BEAM_TEMP:
    raise SystemExit("Set BEAM_TEMP_LOCATION (e.g. gs://your-bucket/temp) in .env")
pipeline_options = PipelineOptions(
    project=PROJECT_ID,
    runner=os.environ.get("BEAM_RUNNER", "DirectRunner"),
    temp_location=BEAM_TEMP,
)

# -----------------------------
# Twitter API setup (from env)
# -----------------------------
BEARER_TOKEN = os.environ.get("TWITTER_BEARER_TOKEN", "")
USERNAME = os.environ.get("TWITTER_USERNAME", "")

TWITTER_API_URL = f"https://api.twitter.com/2/users/by/username/{USERNAME}"

headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}

# Get user ID first (guarded) with rate limit handling
USER_ID = None
try:
    user_resp = requests.get(TWITTER_API_URL, headers=headers)
    user_resp.raise_for_status()
    USER_ID = user_resp.json().get("data", {}).get("id")
    # Add delay after user lookup to avoid rate limits on tweets fetch
    print(f"✅ Twitter user lookup successful. User ID: {USER_ID}")
    time.sleep(2)  # Wait 2 seconds to avoid rate limits
except Exception as e:
    print(f"⚠️ Twitter user lookup failed: {e}")

TWEETS_API_URL = f"https://api.twitter.com/2/users/{USER_ID}/tweets?tweet.fields=created_at,public_metrics&max_results=10" if USER_ID else None


# -----------------------------
# YouTube API setup
# -----------------------------
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
YOUTUBE_CHANNEL_ID = os.environ.get("YOUTUBE_CHANNEL_ID", "")


# -----------------------------
# LinkedIn API setup
# -----------------------------
# IMPORTANT: LinkedIn requires OAuth 2.0. You will need to create a developer app
# and follow the authentication flow to get an access token.
# See: https://docs.microsoft.com/en-us/linkedin/shared/authentication/authentication
LINKEDIN_ACCESS_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_ORGANIZATION_ID = os.environ.get("LINKEDIN_ORGANIZATION_ID", "")
LINKEDIN_API_VERSION = os.environ.get("LINKEDIN_API_VERSION", "202502")
# -----------------------------
# TikTok API setup (from env)
# -----------------------------
TIKTOK_USER = os.environ.get("TIKTOK_USERNAME", "")

# -----------------------------
# BigQuery UPSERT helpers - staging + MERGE pattern
# -----------------------------
def run_merge_statement(client, staging_table: str, target_table: str, id_column: str, columns: list) -> int:
    """Run a MERGE statement to upsert from staging to target table.
    
    Returns the number of rows affected.
    """
    staging_ref = staging_table.replace(":", ".")
    target_ref = target_table.replace(":", ".")
    
    # Build UPDATE SET clause (all columns except ID)
    update_cols = [c for c in columns if c != id_column]
    update_set = ", ".join([f"T.{c} = S.{c}" for c in update_cols])
    
    # Build INSERT columns and values
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"S.{c}" for c in columns])
    
    merge_sql = f"""
    MERGE `{target_ref}` T
    USING `{staging_ref}` S
    ON T.{id_column} = S.{id_column}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    """
    
    try:
        job = client.query(merge_sql)
        job.result()  # Wait for completion
        # Get affected rows from job stats
        affected = job.num_dml_affected_rows or 0
        return affected
    except Exception as e:
        print(f"❌ MERGE failed: {e}")
        return 0

def ensure_table_exists(client, table_ref: str, schema_str: str):
    """Ensure the target table exists, create if needed."""
    table_ref_clean = table_ref.replace(":", ".")
    try:
        client.get_table(table_ref_clean)
    except Exception:
        # Table doesn't exist, it will be created by the first MERGE
        print(f"📝 Table {table_ref_clean} will be created on first write")

def run_all_merges():
    """Run MERGE statements for all platforms after pipeline completes."""
    client = bigquery.Client(project=PROJECT_ID)
    results = {}
    
    # Twitter MERGE
    print("\n🔄 Merging Twitter data...")
    twitter_cols = ["tweet_id", "text", "created_at", "retweet_count", "reply_count", "like_count", "quote_count"]
    try:
        affected = run_merge_statement(client, TWITTER_STAGING_TABLE, TWITTER_OUTPUT_TABLE, "tweet_id", twitter_cols)
        results["twitter"] = affected
        print(f"   ✅ Twitter: {affected} rows upserted")
    except Exception as e:
        print(f"   ❌ Twitter merge failed: {e}")
        results["twitter"] = 0
    
    # YouTube MERGE
    print("🔄 Merging YouTube data...")
    youtube_cols = ["video_id", "title", "description", "published_at", "view_count", "like_count", "comment_count"]
    try:
        affected = run_merge_statement(client, YOUTUBE_STAGING_TABLE, YOUTUBE_OUTPUT_TABLE, "video_id", youtube_cols)
        results["youtube"] = affected
        print(f"   ✅ YouTube: {affected} rows upserted")
    except Exception as e:
        print(f"   ❌ YouTube merge failed: {e}")
        results["youtube"] = 0
    
    # LinkedIn MERGE (v2 schema)
    print("🔄 Merging LinkedIn data...")
    linkedin_cols = [
        "post_id", "post_type", "text", "author_urn",
        "created_at", "published_at", "last_modified_at",
        "visibility", "lifecycle_state", "is_reshare", "reshare_parent_urn",
        "has_media", "media_type",
        "comments_state", "comment_count", "top_level_comment_count",
        "reaction_like", "reaction_celebrate", "reaction_support",
        "reaction_love", "reaction_insightful", "reaction_funny", "total_reactions",
        "fetched_at"
    ]
    try:
        affected = run_merge_statement(client, LINKEDIN_STAGING_TABLE, LINKEDIN_OUTPUT_TABLE, "post_id", linkedin_cols)
        results["linkedin"] = affected
        print(f"   ✅ LinkedIn: {affected} rows upserted")
    except Exception as e:
        print(f"   ❌ LinkedIn merge failed: {e}")
        results["linkedin"] = 0
    
    # Cleanup staging tables
    print("\n🧹 Cleaning up staging tables...")
    for staging in [TWITTER_STAGING_TABLE, YOUTUBE_STAGING_TABLE, LINKEDIN_STAGING_TABLE]:
        try:
            staging_ref = staging.replace(":", ".")
            client.delete_table(staging_ref, not_found_ok=True)
        except Exception:
            pass
    
    return results

# -----------------------------
# Helper functions
# -----------------------------
def fetch_tweets(_):
    """Fetch latest tweets with rate limit handling"""
    if not USER_ID or not TWEETS_API_URL:
        print("⚠️ Twitter not configured or lookup failed. Skipping Twitter fetch.")
        return
    
    # Retry logic for rate limits
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            response = requests.get(TWEETS_API_URL, headers=headers)
            if response.status_code == 429:
                # Rate limited - wait and retry
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"⚠️ Twitter rate limit hit. Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_attempts}")
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            resp = response.json()
            tweet_count = len(resp.get("data", []))
            print(f"✅ Fetched {tweet_count} tweets successfully")
            for tweet in resp.get("data", []):
                yield tweet
            return
        except requests.exceptions.RequestException as e:
            if attempt < max_attempts - 1:
                print(f"⚠️ Twitter fetch error: {e}. Retrying...")
                time.sleep(2)
            else:
                print(f"⚠️ Twitter fetch failed after {max_attempts} attempts: {e}")
                return

def fetch_youtube_videos(_):
    """Fetch latest YouTube videos"""
    if not YOUTUBE_API_KEY:
        print("⚠️ YouTube API key not found. Skipping YouTube fetch.")
        return

    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

        request = youtube.search().list(
            part="snippet",
            channelId=YOUTUBE_CHANNEL_ID,
            maxResults=10,
            order="date",
            type="video"
        )
        response = request.execute()

        video_ids = [item['id']['videoId'] for item in response.get('items', []) if item.get('id', {}).get('videoId')]
        print(f"✅ YouTube search found {len(video_ids)} videos")

        if not video_ids:
            print("⚠️ No YouTube videos found for channel")
            return

        video_request = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(video_ids)
        )
        video_response = video_request.execute()
        
        video_count = len(video_response.get('items', []))
        print(f"✅ Fetched details for {video_count} YouTube videos")

        for item in video_response.get('items', []):
            yield item
    except Exception as e:
        print(f"⚠️ YouTube fetch failed: {e}")
        return

# Global flag to track if daily rate limit was hit
_linkedin_daily_limit_reached = False

def _fetch_linkedin_social_metadata_map(headers: dict, urns: list[str]) -> dict:
    """Fetch social metadata per URN using single GET endpoint. Returns map: urn -> metadata dict.
    
    Stops immediately if daily rate limit is reached to avoid wasting API calls.
    """
    global _linkedin_daily_limit_reached
    
    if not urns or _linkedin_daily_limit_reached:
        return {}
    
    results: dict = {}
    from urllib.parse import quote
    
    for idx, urn in enumerate(urns):
        if _linkedin_daily_limit_reached:
            break
            
        try:
            encoded = quote(urn, safe="")
            resp = requests.get(
                f"https://api.linkedin.com/rest/socialMetadata/{encoded}",
                headers=headers,
            )
            
            if resp.status_code == 200:
                results[urn] = resp.json()
                time.sleep(0.2)  # Small delay between successful calls
            elif resp.status_code == 429:
                # Check if it's a daily limit
                try:
                    error_body = resp.json()
                    error_msg = error_body.get("message", "")
                    if "DAY limit" in error_msg:
                        print(f"🛑 LinkedIn daily rate limit reached! Stopping metadata fetch.")
                        print(f"   Posts will be saved without reaction data.")
                        _linkedin_daily_limit_reached = True
                        break
                except:
                    pass
                # If not daily limit, just skip this one
                print(f"⚠️ Rate limited for {urn[:50]}..., skipping")
            elif resp.status_code == 403:
                # Permission issue - skip silently
                pass
            else:
                resp.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            print(f"⚠️ SocialMetadata failed for {urn[:50]}...: {e}")
            continue
    
    return results


def fetch_linkedin_posts(_):
    """Fetch latest LinkedIn company posts.
    
    Only fetches the most recent 50 posts to avoid excessive API calls.
    Use linkedin_pipeline.py for full historical data with proper incremental loading.
    """
    global _linkedin_daily_limit_reached
    _linkedin_daily_limit_reached = False  # Reset at start of each run
    
    if not LINKEDIN_ACCESS_TOKEN:
        print("⚠️ LinkedIn Access Token not found. Skipping LinkedIn fetch.")
        return

    headers = {
        "Authorization": f"Bearer {LINKEDIN_ACCESS_TOKEN}",
        "LinkedIn-Version": LINKEDIN_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Accept": "application/json",
    }
    params = {
        "q": "author",
        "author": LINKEDIN_ORGANIZATION_ID,
        "count": 50,  # Fetch only 50 most recent posts
    }
    url_rest_posts = "https://api.linkedin.com/rest/posts"
    
    print(f"🔄 Fetching LinkedIn posts for organization: {LINKEDIN_ORGANIZATION_ID}")
    print(f"   (Limited to 50 most recent posts - use linkedin_pipeline.py for full data)")
    
    try:
        # Only fetch first page (50 posts) to avoid excessive API calls
        response = requests.get(url_rest_posts, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        posts = data.get("elements", [])
        print(f"✅ LinkedIn: Fetched {len(posts)} posts")
        
        if not posts:
            return
            
        # Build URN candidates for social metadata per post
        urn_candidates_per_post = []
        all_urns_set = set()
        for p in posts:
            candidates = []
            pid = p.get("id") or p.get("urn")
            if isinstance(pid, str) and pid.startswith("urn:li:"):
                candidates.append(pid)
            elif pid is not None:
                spid = str(pid)
                if spid.isdigit():
                    candidates.append(f"urn:li:ugcPost:{spid}")
                    candidates.append(f"urn:li:share:{spid}")
                else:
                    candidates.append(spid)
            urn_candidates_per_post.append(candidates)
            for c in candidates:
                all_urns_set.add(c)

        # Fetch metadata (will stop if rate limited)
        meta_map = _fetch_linkedin_social_metadata_map(headers, list(all_urns_set))
        
        metadata_count = len(meta_map)
        print(f"✅ LinkedIn: Fetched metadata for {metadata_count}/{len(posts)} posts")
        if _linkedin_daily_limit_reached:
            print(f"⚠️ Daily limit reached - remaining posts saved without reaction data")

        # Attach best-matching metadata to each post and yield
        for idx, post in enumerate(posts):
            selected_meta = {}
            for cand in urn_candidates_per_post[idx]:
                if cand in meta_map:
                    selected_meta = meta_map.get(cand, {})
                    break
            comment_summary = selected_meta.get("commentSummary", {}) if isinstance(selected_meta, dict) else {}
            reaction_summaries = selected_meta.get("reactionSummaries", {}) if isinstance(selected_meta, dict) else {}
            post["__social"] = {
                "commentsState": selected_meta.get("commentsState"),
                "comment_count": comment_summary.get("count"),
                "top_level_count": comment_summary.get("topLevelCount"),
                "reactions": reaction_summaries,
            }
            yield post
        
        return
    except requests.exceptions.RequestException as e:
        print(f"⚠️ LinkedIn REST posts failed: {e}")
        if getattr(e, 'response', None) is not None:
            print(f"Response body: {e.response.text}")

    # Fallback to v2 shares
    headers_v2 = {"Authorization": f"Bearer {LINKEDIN_ACCESS_TOKEN}"}
    params_v2 = {
        "q": "owners",
        "owners": LINKEDIN_ORGANIZATION_ID,
        "count": 10,
        "projection": "(elements*(id,created,specificContent(com.linkedin.ugc.ShareContent(shareCommentary(text)))))"
    }
    try:
        response_v2 = requests.get("https://api.linkedin.com/v2/shares", headers=headers_v2, params=params_v2)
        response_v2.raise_for_status()
        data_v2 = response_v2.json()
        for post in data_v2.get("elements", []):
            yield post
    except requests.exceptions.RequestException as e2:
        print(f"⚠️ LinkedIn v2 shares fetch failed: {e2}")
        if getattr(e2, 'response', None) is not None:
            print(f"Response body: {e2.response.text}")
        return


async def get_tiktok_videos():
    """Async helper to fetch tiktok videos"""
    videos = []
    async with TikTokApi() as api:
        # Try both sync and async session creation depending on lib version
        try:
            cs = getattr(api, "create_sessions", None)
            if cs is not None:
                maybe_coro = cs(headless=False, num_sessions=1, sleep_after=3)
                if hasattr(maybe_coro, "__await__"):
                    await maybe_coro
            else:
                # Older versions may use set_session or no-op
                pass
        except Exception:
            pass
        async for video in api.user(TIKTOK_USER).videos(count=10):
            videos.append(video.as_dict)
    return videos

def fetch_tiktok_videos(_):
    """Fetch latest TikTok videos"""
    try:
        videos = asyncio.run(get_tiktok_videos())
        for video in videos:
            yield video
    except Exception as e:
        print(f"⚠️ TikTok fetch failed: {e}")
        return


def safe_timestamp(ts_str):
    """Convert ISO timestamp string to Python datetime, or None if invalid"""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt
    except Exception:
        return None

def safe_timestamp_ms(ts_ms):
    """Convert Unix timestamp in milliseconds to Python datetime, or None if invalid"""
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000)
    except (ValueError, TypeError):
        return None

def transform_tweet(tweet):
    """Transform tweet for BigQuery"""
    metrics = tweet.get("public_metrics", {})
    return {
        "tweet_id": tweet["id"],
        "text": tweet.get("text", ""),
        "created_at": safe_timestamp(tweet.get("created_at", None)),
        "retweet_count": metrics.get("retweet_count", 0),
        "reply_count": metrics.get("reply_count", 0),
        "like_count": metrics.get("like_count", 0),
        "quote_count": metrics.get("quote_count", 0),
    }

def transform_youtube_video(video):
    """Transform YouTube video for BigQuery"""
    snippet = video.get('snippet', {})
    stats = video.get('statistics', {})
    return {
        "video_id": video.get("id"),
        "title": snippet.get("title"),
        "description": snippet.get("description"),
        "published_at": safe_timestamp(snippet.get("publishedAt")),
        "view_count": int(stats.get("viewCount", 0)),
        "like_count": int(stats.get("likeCount", 0)),
        "comment_count": int(stats.get("commentCount", 0)),
    }

def _extract_reaction_count(reactions: dict, key: str) -> int:
    try:
        entry = reactions.get(key, {})
        return int(entry.get("count", 0))
    except Exception:
        return 0

def _detect_linkedin_media_type(content: dict) -> tuple:
    """Detect media type from LinkedIn post content. Returns (media_type, has_media)"""
    if not content:
        return "none", False
    
    if "media" in content:
        media = content["media"]
        media_id = media.get("id", "")
        if "video:" in media_id:
            return "video", True
        elif "image:" in media_id:
            return "image", True
        else:
            return "media", True
    elif "article" in content:
        return "article", True
    elif "multiImage" in content:
        return "multiImage", True
    
    return "none", False

def transform_linkedin_post(post):
    """Transform LinkedIn post for BigQuery v2 schema with rich metadata"""
    post_id = post.get("id") or post.get("urn")
    
    # Determine post type
    post_type = "ugcPost" if "ugcPost" in str(post_id) else "share"
    
    # Get text content
    commentary = post.get("commentary")
    if commentary is None:
        commentary = post.get("specificContent", {}).get("com.linkedin.ugc.ShareContent", {}).get("shareCommentary", {}).get("text", "")
    
    # Timestamps
    created_ms = post.get("createdAt") or post.get("created", {}).get("time")
    published_ms = post.get("publishedAt")
    modified_ms = post.get("lastModifiedAt")
    
    # Detect media type
    content = post.get("content", {})
    media_type, has_media = _detect_linkedin_media_type(content)
    
    # Check if reshare
    reshare_context = post.get("reshareContext", {})
    is_reshare = bool(reshare_context)
    reshare_parent_urn = reshare_context.get("parent") if is_reshare else None
    
    # Social metadata
    social = post.get("__social", {}) if isinstance(post, dict) else {}
    reactions = social.get("reactions", {}) if isinstance(social, dict) else {}
    
    # Calculate reactions
    reaction_like = _extract_reaction_count(reactions, "LIKE")
    reaction_celebrate = _extract_reaction_count(reactions, "PRAISE")
    reaction_support = _extract_reaction_count(reactions, "APPRECIATION")
    reaction_love = _extract_reaction_count(reactions, "EMPATHY")
    reaction_insightful = _extract_reaction_count(reactions, "INTEREST")
    reaction_funny = _extract_reaction_count(reactions, "ENTERTAINMENT")
    total_reactions = reaction_like + reaction_celebrate + reaction_support + reaction_love + reaction_insightful + reaction_funny

    return {
        "post_id": post_id,
        "post_type": post_type,
        "text": commentary or "",
        "author_urn": post.get("author"),
        
        "created_at": safe_timestamp_ms(created_ms) if created_ms else None,
        "published_at": safe_timestamp_ms(published_ms) if published_ms else None,
        "last_modified_at": safe_timestamp_ms(modified_ms) if modified_ms else None,
        
        "visibility": post.get("visibility"),
        "lifecycle_state": post.get("lifecycleState"),
        "is_reshare": is_reshare,
        "reshare_parent_urn": reshare_parent_urn,
        
        "has_media": has_media,
        "media_type": media_type,
        
        "comments_state": social.get("commentsState"),
        "comment_count": int(social.get("comment_count") or 0),
        "top_level_comment_count": int(social.get("top_level_count") or 0),
        
        "reaction_like": reaction_like,
        "reaction_celebrate": reaction_celebrate,
        "reaction_support": reaction_support,
        "reaction_love": reaction_love,
        "reaction_insightful": reaction_insightful,
        "reaction_funny": reaction_funny,
        "total_reactions": total_reactions,
        
        "fetched_at": datetime.utcnow(),
    }

def transform_tiktok_video(video):
    """Transform TikTok video for BigQuery"""
    stats = video.get("stats", {})
    return {
        "video_id": video.get("id"),
        "description": video.get("desc"),
        "created_at": datetime.utcfromtimestamp(video.get("createTime")),
        "play_count": stats.get("playCount", 0),
        "like_count": stats.get("diggCount", 0),
        "comment_count": stats.get("commentCount", 0),
        "share_count": stats.get("shareCount", 0),
    }



# -----------------------------
# Beam pipeline - writes to STAGING tables, then MERGE to main tables
# -----------------------------
print("\n📤 Starting data pipeline...")
print("   Using STAGING + MERGE pattern for idempotent upserts")

with beam.Pipeline(options=pipeline_options) as p:

    # --- Twitter Pipeline (writes to staging) ---
    tweets = (
        p
        | "CreateDummyTwitter" >> beam.Create([None])
        | "FetchTweets" >> beam.FlatMap(fetch_tweets)
        | "TransformTweets" >> beam.Map(transform_tweet)
        | "FilterInvalidTweetTimestamps" >> beam.Filter(lambda x: x["created_at"] is not None)
        | "WriteTweetsToStaging" >> beam.io.WriteToBigQuery(
            TWITTER_STAGING_TABLE,
            schema="""
                tweet_id:STRING,
                text:STRING,
                created_at:TIMESTAMP,
                retweet_count:INTEGER,
                reply_count:INTEGER,
                like_count:INTEGER,
                quote_count:INTEGER
            """,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

    # --- YouTube Pipeline (writes to staging) ---
    youtube_videos = (
        p
        | "CreateDummyYouTube" >> beam.Create([None])
        | "FetchYouTubeVideos" >> beam.FlatMap(fetch_youtube_videos)
        | "TransformYouTubeVideo" >> beam.Map(transform_youtube_video)
        | "FilterInvalidVideoTimestamps" >> beam.Filter(lambda x: x["published_at"] is not None)
        | "WriteVideosToStaging" >> beam.io.WriteToBigQuery(
            YOUTUBE_STAGING_TABLE,
            schema="""
                video_id:STRING,
                title:STRING,
                description:STRING,
                published_at:TIMESTAMP,
                view_count:INTEGER,
                like_count:INTEGER,
                comment_count:INTEGER
            """,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

    # --- LinkedIn Pipeline (writes to staging with v2 schema) ---
    linkedin_posts = (
        p
        | "CreateDummyLinkedIn" >> beam.Create([None])
        | "FetchLinkedInPosts" >> beam.FlatMap(fetch_linkedin_posts)
        | "TransformLinkedInPost" >> beam.Map(transform_linkedin_post)
        | "FilterInvalidLinkedInTimestamps" >> beam.Filter(lambda x: x.get("created_at") is not None)
        | "WritePostsToStaging" >> beam.io.WriteToBigQuery(
            LINKEDIN_STAGING_TABLE,
            schema="""
                post_id:STRING,
                post_type:STRING,
                text:STRING,
                author_urn:STRING,
                created_at:TIMESTAMP,
                published_at:TIMESTAMP,
                last_modified_at:TIMESTAMP,
                visibility:STRING,
                lifecycle_state:STRING,
                is_reshare:BOOLEAN,
                reshare_parent_urn:STRING,
                has_media:BOOLEAN,
                media_type:STRING,
                comments_state:STRING,
                comment_count:INTEGER,
                top_level_comment_count:INTEGER,
                reaction_like:INTEGER,
                reaction_celebrate:INTEGER,
                reaction_support:INTEGER,
                reaction_love:INTEGER,
                reaction_insightful:INTEGER,
                reaction_funny:INTEGER,
                total_reactions:INTEGER,
                fetched_at:TIMESTAMP
            """,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    )

    # --- TikTok Pipeline (guarded) ---
    if os.environ.get("ENABLE_TIKTOK", "false").lower() == "true":
        tiktok_videos = (
            p
            | "CreateDummyTikTok" >> beam.Create([None])
            | "FetchTikTokVideos" >> beam.FlatMap(fetch_tiktok_videos)
            | "TransformTikTokVideo" >> beam.Map(transform_tiktok_video)
            | "FilterInvalidTikTokTimestamps" >> beam.Filter(lambda x: x["created_at"] is not None)
            | "WriteTikTokVideosToBQ" >> beam.io.WriteToBigQuery(
                TIKTOK_OUTPUT_TABLE,
                schema="""
                    video_id:STRING,
                    description:STRING,
                    created_at:TIMESTAMP,
                    play_count:INTEGER,
                    like_count:INTEGER,
                    comment_count:INTEGER,
                    share_count:INTEGER
                """,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )
    else:
        print("⚠️ TikTok pipeline skipped. Set ENABLE_TIKTOK=true to enable.")

    # Example: Read GA4 data (optional)
    ga4_events = (
        p
        | "ReadGA4" >> beam.io.ReadFromBigQuery(table=GA4_TABLE)
        # you can transform or join GA4 data here
        # | "TransformGA4" >> beam.Map(transform_event)  # if needed
    )

# Run MERGE statements to upsert data from staging to main tables
print("\n🔀 Running MERGE operations (insert new, update existing)...")
merge_results = run_all_merges()

print("\n" + "="*50)
print("📊 UPSERT SUMMARY")
print("="*50)
print(f"   Twitter rows upserted:  {merge_results.get('twitter', 0)}")
print(f"   YouTube rows upserted:  {merge_results.get('youtube', 0)}")
print(f"   LinkedIn rows upserted: {merge_results.get('linkedin', 0)}")
print("="*50)

total_upserted = sum(merge_results.values())
if total_upserted > 0:
    print("✅ Pipeline completed successfully!")
    print("   - New records were INSERTED")
    print("   - Existing records were UPDATED with latest data")
else:
    print("ℹ️ No changes - data is already up to date")
