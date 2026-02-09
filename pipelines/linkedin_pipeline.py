#!/usr/bin/env python3
"""
LinkedIn Data Pipeline - Proper Schema with Incremental Loading
Author: Solutions Architect
Purpose: Fetch LinkedIn posts with proper schema and incremental loading support

Features:
- Proper hierarchical schema with all post metadata
- Incremental loading (only fetch posts after last stored timestamp)
- Social metadata fetching with rate limit handling
- Media type detection
- Reshare detection
"""

import os
import json
import requests
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime, timezone
from google.cloud import bigquery
import time
import random
import argparse

load_dotenv()

# Configuration (from env for multi-org reuse)
PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET = os.environ.get("BQ_DATASET_ANALYTICS", "")
LINKEDIN_TABLE = f"{PROJECT_ID}.{DATASET}.linkedin_posts_v2"

LINKEDIN_ACCESS_TOKEN = os.environ.get("LINKEDIN_ACCESS_TOKEN", "")
LINKEDIN_ORGANIZATION_ID = os.environ.get("LINKEDIN_ORGANIZATION_ID", "")
LINKEDIN_API_VERSION = os.environ.get("LINKEDIN_API_VERSION", "202502")
if not PROJECT_ID or not DATASET:
    raise SystemExit("Set GCP_PROJECT and BQ_DATASET_ANALYTICS in .env (see .env.example)")

# BigQuery Schema
BQ_SCHEMA = [
    bigquery.SchemaField("post_id", "STRING", mode="REQUIRED", description="URN of the post"),
    bigquery.SchemaField("post_type", "STRING", description="ugcPost or share"),
    bigquery.SchemaField("text", "STRING", description="Post commentary/text content"),
    bigquery.SchemaField("author_urn", "STRING", description="Author URN"),
    
    # Timestamps
    bigquery.SchemaField("created_at", "TIMESTAMP", description="When post was created"),
    bigquery.SchemaField("published_at", "TIMESTAMP", description="When post was published"),
    bigquery.SchemaField("last_modified_at", "TIMESTAMP", description="Last modification time"),
    
    # Post metadata
    bigquery.SchemaField("visibility", "STRING", description="PUBLIC, CONNECTIONS, etc."),
    bigquery.SchemaField("lifecycle_state", "STRING", description="PUBLISHED, DRAFT, etc."),
    bigquery.SchemaField("is_reshare", "BOOLEAN", description="Whether this is a reshare"),
    bigquery.SchemaField("reshare_parent_urn", "STRING", description="Parent post URN if reshare"),
    
    # Content metadata
    bigquery.SchemaField("has_media", "BOOLEAN", description="Has image/video/article"),
    bigquery.SchemaField("media_type", "STRING", description="image, video, article, multiImage, none"),
    
    # Social metrics
    bigquery.SchemaField("comments_state", "STRING", description="OPEN, CLOSED"),
    bigquery.SchemaField("comment_count", "INTEGER", description="Total comments"),
    bigquery.SchemaField("top_level_comment_count", "INTEGER", description="Top-level comments only"),
    
    # Reactions
    bigquery.SchemaField("reaction_like", "INTEGER", description="LIKE count"),
    bigquery.SchemaField("reaction_celebrate", "INTEGER", description="PRAISE/Celebrate count"),
    bigquery.SchemaField("reaction_support", "INTEGER", description="APPRECIATION/Support count"),
    bigquery.SchemaField("reaction_love", "INTEGER", description="EMPATHY/Love count"),
    bigquery.SchemaField("reaction_insightful", "INTEGER", description="INTEREST/Insightful count"),
    bigquery.SchemaField("reaction_funny", "INTEGER", description="ENTERTAINMENT/Funny count"),
    bigquery.SchemaField("total_reactions", "INTEGER", description="Sum of all reactions"),
    
    # ETL metadata
    bigquery.SchemaField("fetched_at", "TIMESTAMP", description="When this row was fetched"),
]

def get_headers():
    return {
        "Authorization": f"Bearer {LINKEDIN_ACCESS_TOKEN}",
        "LinkedIn-Version": LINKEDIN_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Accept": "application/json",
    }

def ms_to_datetime(ms):
    """Convert milliseconds timestamp to datetime"""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    except:
        return None

def detect_media_type(content):
    """Detect media type from post content"""
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

def extract_reaction_count(reactions, key):
    """Extract reaction count safely"""
    try:
        entry = reactions.get(key, {})
        return int(entry.get("count", 0))
    except:
        return 0

def get_last_post_timestamp(client):
    """Get the timestamp of the most recent post in BigQuery"""
    query = f"""
    SELECT MAX(created_at) as last_created_at
    FROM `{LINKEDIN_TABLE}`
    """
    try:
        result = client.query(query).result()
        for row in result:
            if row.last_created_at:
                return row.last_created_at
    except Exception as e:
        print(f"⚠️ Could not get last timestamp (table might not exist): {e}")
    return None

def fetch_social_metadata(headers, urn, max_retries=3):
    """Fetch social metadata with rate limit handling
    
    Returns: (data, should_stop)
        - data: the response JSON or empty dict
        - should_stop: True if we hit daily rate limit and should stop
    """
    encoded_urn = quote(urn, safe="")
    url = f"https://api.linkedin.com/rest/socialMetadata/{encoded_urn}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json(), False
            elif response.status_code == 429:
                # Check if it's a daily limit (APPLICATION_AND_MEMBER DAY)
                error_body = response.json() if response.text else {}
                error_msg = error_body.get("message", "")
                
                if "DAY limit" in error_msg:
                    print(f"  🛑 Daily rate limit reached! Will need to continue tomorrow.")
                    return {}, True  # Signal to stop processing
                
                # Temporary rate limit - wait and retry
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"  ⏳ Rate limited, waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue
            elif response.status_code == 403:
                # Permission issue - return empty
                return {}, False
            else:
                return {}, False
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return {}, False
    return {}, False

def fetch_all_posts(headers, after_timestamp=None, max_posts=None, fetch_metadata=True):
    """Fetch all posts from LinkedIn, optionally after a certain timestamp"""
    encoded_org = quote(LINKEDIN_ORGANIZATION_ID, safe="")
    base_url = "https://api.linkedin.com/rest/posts"
    
    all_posts = []
    start = 0
    page_size = 50
    
    print(f"🔄 Fetching LinkedIn posts...")
    if after_timestamp:
        print(f"   Filtering: only posts after {after_timestamp}")
    
    while True:
        url = f"{base_url}?q=author&author={encoded_org}&count={page_size}&start={start}"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ Error fetching posts: {e}")
            break
        
        posts = data.get("elements", [])
        if not posts:
            break
        
        print(f"  📄 Fetched {len(posts)} posts (page starting at {start})")
        
        for post in posts:
            created_ms = post.get("createdAt")
            created_at = ms_to_datetime(created_ms)
            
            # Skip posts older than the filter timestamp (incremental loading)
            if after_timestamp and created_at and created_at <= after_timestamp:
                print(f"  ⏭️ Reached posts older than last fetch, stopping...")
                return all_posts
            
            all_posts.append(post)
            
            if max_posts and len(all_posts) >= max_posts:
                print(f"  ✅ Reached max posts limit ({max_posts})")
                return all_posts
        
        # Check pagination
        paging = data.get("paging", {})
        next_start = paging.get("start", 0) + paging.get("count", len(posts))
        if next_start <= start or not posts:
            break
        start = next_start
        
        # Small delay to avoid rate limits
        time.sleep(0.5)
    
    return all_posts

def transform_post(post, social_metadata=None):
    """Transform a LinkedIn post to BigQuery row format"""
    post_id = post.get("id") or post.get("urn")
    
    # Determine post type
    post_type = "ugcPost" if "ugcPost" in str(post_id) else "share"
    
    # Detect media
    content = post.get("content", {})
    media_type, has_media = detect_media_type(content)
    
    # Check if reshare
    reshare_context = post.get("reshareContext", {})
    is_reshare = bool(reshare_context)
    reshare_parent_urn = reshare_context.get("parent") if is_reshare else None
    
    # Social metadata
    social = social_metadata or {}
    comment_summary = social.get("commentSummary", {})
    reactions = social.get("reactionSummaries", {})
    
    # Calculate total reactions
    reaction_like = extract_reaction_count(reactions, "LIKE")
    reaction_celebrate = extract_reaction_count(reactions, "PRAISE")
    reaction_support = extract_reaction_count(reactions, "APPRECIATION")
    reaction_love = extract_reaction_count(reactions, "EMPATHY")
    reaction_insightful = extract_reaction_count(reactions, "INTEREST")
    reaction_funny = extract_reaction_count(reactions, "ENTERTAINMENT")
    total_reactions = reaction_like + reaction_celebrate + reaction_support + reaction_love + reaction_insightful + reaction_funny
    
    return {
        "post_id": post_id,
        "post_type": post_type,
        "text": post.get("commentary", ""),
        "author_urn": post.get("author"),
        
        "created_at": ms_to_datetime(post.get("createdAt")),
        "published_at": ms_to_datetime(post.get("publishedAt")),
        "last_modified_at": ms_to_datetime(post.get("lastModifiedAt")),
        
        "visibility": post.get("visibility"),
        "lifecycle_state": post.get("lifecycleState"),
        "is_reshare": is_reshare,
        "reshare_parent_urn": reshare_parent_urn,
        
        "has_media": has_media,
        "media_type": media_type,
        
        "comments_state": social.get("commentsState"),
        "comment_count": int(comment_summary.get("count", 0) or 0),
        "top_level_comment_count": int(comment_summary.get("topLevelCount", 0) or 0),
        
        "reaction_like": reaction_like,
        "reaction_celebrate": reaction_celebrate,
        "reaction_support": reaction_support,
        "reaction_love": reaction_love,
        "reaction_insightful": reaction_insightful,
        "reaction_funny": reaction_funny,
        "total_reactions": total_reactions,
        
        "fetched_at": datetime.now(timezone.utc),
    }

def create_table_if_not_exists(client):
    """Create the BigQuery table if it doesn't exist"""
    table_ref = bigquery.Table(LINKEDIN_TABLE, schema=BQ_SCHEMA)
    table_ref.description = "LinkedIn posts with proper schema and social metrics"
    
    try:
        client.get_table(LINKEDIN_TABLE)
        print(f"✅ Table {LINKEDIN_TABLE} already exists")
        return False
    except:
        table = client.create_table(table_ref)
        print(f"✅ Created table {LINKEDIN_TABLE}")
        # Wait for table to be ready
        print("   Waiting for table to be ready...")
        time.sleep(5)
        return True

def insert_rows(client, rows, batch_size=500):
    """Insert rows into BigQuery in batches"""
    if not rows:
        print("⚠️ No rows to insert")
        return
    
    # Convert datetime objects to ISO format strings for JSON serialization
    json_rows = []
    for row in rows:
        json_row = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                json_row[key] = value.isoformat()
            else:
                json_row[key] = value
        json_rows.append(json_row)
    
    # Insert in batches
    total_inserted = 0
    for i in range(0, len(json_rows), batch_size):
        batch = json_rows[i:i + batch_size]
        try:
            errors = client.insert_rows_json(LINKEDIN_TABLE, batch)
            if errors:
                print(f"❌ Errors inserting batch {i//batch_size + 1}: {errors[:2]}...")
            else:
                total_inserted += len(batch)
                print(f"  📤 Inserted batch {i//batch_size + 1}: {len(batch)} rows")
        except Exception as e:
            print(f"❌ Error inserting batch {i//batch_size + 1}: {e}")
        time.sleep(1)  # Small delay between batches
    
    print(f"✅ Total inserted: {total_inserted} rows")

def flush_table(client):
    """Delete all data from the table"""
    query = f"DELETE FROM `{LINKEDIN_TABLE}` WHERE TRUE"
    try:
        client.query(query).result()
        print(f"✅ Flushed all data from {LINKEDIN_TABLE}")
    except Exception as e:
        print(f"⚠️ Could not flush table: {e}")

def run_pipeline(mode="incremental", max_posts=None, fetch_metadata=True, max_metadata_calls=None):
    """Run the LinkedIn data pipeline"""
    print("=" * 80)
    print("LINKEDIN DATA PIPELINE")
    print("=" * 80)
    
    if not LINKEDIN_ACCESS_TOKEN:
        print("❌ LINKEDIN_ACCESS_TOKEN not set!")
        return
    
    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Create table if needed
    create_table_if_not_exists(client)
    
    # Determine filter timestamp for incremental loading
    after_timestamp = None
    if mode == "incremental":
        after_timestamp = get_last_post_timestamp(client)
        if after_timestamp:
            print(f"📅 Last post timestamp: {after_timestamp}")
        else:
            print("📅 No existing posts found, doing full load")
    elif mode == "full":
        print("🔄 Full reload mode - flushing existing data...")
        flush_table(client)
    
    # Fetch posts
    headers = get_headers()
    posts = fetch_all_posts(headers, after_timestamp=after_timestamp, max_posts=max_posts)
    
    if not posts:
        print("📭 No new posts to process")
        return
    
    print(f"\n📊 Processing {len(posts)} posts...")
    if max_metadata_calls:
        print(f"   Max metadata calls this run: {max_metadata_calls}")
    
    # Transform posts
    rows = []
    metadata_fetched = 0
    rate_limit_hit = False
    
    for i, post in enumerate(posts):
        post_id = post.get("id")
        
        # Optionally fetch social metadata (rate limited)
        social_metadata = None
        
        # Check if we should fetch metadata
        should_fetch = (
            fetch_metadata and 
            not rate_limit_hit and
            (max_metadata_calls is None or metadata_fetched < max_metadata_calls)
        )
        
        if should_fetch:
            print(f"  🔍 [{metadata_fetched+1}] Fetching metadata for post {i+1}/{len(posts)}: {post_id[:50]}...")
            social_metadata, should_stop = fetch_social_metadata(headers, post_id)
            
            if should_stop:
                rate_limit_hit = True
                print(f"  ⚠️ Daily limit reached after {metadata_fetched} metadata calls.")
                print(f"  📝 Continuing to process remaining posts without metadata...")
            elif social_metadata:
                metadata_fetched += 1
                # Add delay between successful metadata calls to respect rate limits
                # LinkedIn limits reset at midnight UTC - spread calls to ~100-500/day
                time.sleep(1.5)  # ~2400 calls/hour max, but safer for daily limits
        
        row = transform_post(post, social_metadata)
        rows.append(row)
    
    print(f"\n✅ Metadata fetched for {metadata_fetched}/{len(posts)} posts")
    if rate_limit_hit:
        print(f"⚠️ Daily rate limit was reached. Run again tomorrow to fetch more metadata.")
    
    # Insert into BigQuery
    print(f"\n💾 Inserting {len(rows)} rows into BigQuery...")
    insert_rows(client, rows)
    
    # Summary
    print("\n" + "=" * 80)
    print("PIPELINE SUMMARY")
    print("=" * 80)
    print(f"  Mode: {mode}")
    print(f"  Posts processed: {len(rows)}")
    print(f"  Table: {LINKEDIN_TABLE}")
    
    # Show count
    query = f"SELECT COUNT(*) as count FROM `{LINKEDIN_TABLE}`"
    result = client.query(query).result()
    for row in result:
        print(f"  Total rows in table: {row.count}")

def update_missing_metadata(max_calls=90):  # Default 90 to stay under 100/day user limit
    """Update posts that have no reaction data (total_reactions = 0)"""
    print("=" * 80)
    print("UPDATE MISSING METADATA")
    print("=" * 80)
    
    if not LINKEDIN_ACCESS_TOKEN:
        print("❌ LINKEDIN_ACCESS_TOKEN not set!")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    headers = get_headers()
    
    # Get posts with no metadata
    query = f"""
    SELECT post_id 
    FROM `{LINKEDIN_TABLE}` 
    WHERE total_reactions = 0 OR total_reactions IS NULL
    ORDER BY created_at DESC
    LIMIT {max_calls}
    """
    
    print(f"🔍 Finding posts without metadata (limit: {max_calls})...")
    result = client.query(query).result()
    post_ids = [row.post_id for row in result]
    
    if not post_ids:
        print("✅ All posts have metadata!")
        return
    
    print(f"📋 Found {len(post_ids)} posts without metadata")
    
    updated = 0
    for i, post_id in enumerate(post_ids):
        print(f"  🔍 [{i+1}/{len(post_ids)}] Fetching: {post_id[:50]}...")
        
        metadata, should_stop = fetch_social_metadata(headers, post_id)
        
        if should_stop:
            print(f"  🛑 Daily limit reached after {updated} updates.")
            break
        
        if metadata:
            # Extract reaction counts
            reactions = metadata.get("reactionSummaries", {})
            comment_summary = metadata.get("commentSummary", {})
            
            reaction_like = extract_reaction_count(reactions, "LIKE")
            reaction_celebrate = extract_reaction_count(reactions, "PRAISE")
            reaction_support = extract_reaction_count(reactions, "APPRECIATION")
            reaction_love = extract_reaction_count(reactions, "EMPATHY")
            reaction_insightful = extract_reaction_count(reactions, "INTEREST")
            reaction_funny = extract_reaction_count(reactions, "ENTERTAINMENT")
            total_reactions = reaction_like + reaction_celebrate + reaction_support + reaction_love + reaction_insightful + reaction_funny
            
            # Update the row
            update_query = f"""
            UPDATE `{LINKEDIN_TABLE}`
            SET 
                comments_state = @comments_state,
                comment_count = @comment_count,
                top_level_comment_count = @top_level_comment_count,
                reaction_like = @reaction_like,
                reaction_celebrate = @reaction_celebrate,
                reaction_support = @reaction_support,
                reaction_love = @reaction_love,
                reaction_insightful = @reaction_insightful,
                reaction_funny = @reaction_funny,
                total_reactions = @total_reactions,
                fetched_at = CURRENT_TIMESTAMP()
            WHERE post_id = @post_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("post_id", "STRING", post_id),
                    bigquery.ScalarQueryParameter("comments_state", "STRING", metadata.get("commentsState")),
                    bigquery.ScalarQueryParameter("comment_count", "INT64", int(comment_summary.get("count", 0) or 0)),
                    bigquery.ScalarQueryParameter("top_level_comment_count", "INT64", int(comment_summary.get("topLevelCount", 0) or 0)),
                    bigquery.ScalarQueryParameter("reaction_like", "INT64", reaction_like),
                    bigquery.ScalarQueryParameter("reaction_celebrate", "INT64", reaction_celebrate),
                    bigquery.ScalarQueryParameter("reaction_support", "INT64", reaction_support),
                    bigquery.ScalarQueryParameter("reaction_love", "INT64", reaction_love),
                    bigquery.ScalarQueryParameter("reaction_insightful", "INT64", reaction_insightful),
                    bigquery.ScalarQueryParameter("reaction_funny", "INT64", reaction_funny),
                    bigquery.ScalarQueryParameter("total_reactions", "INT64", total_reactions),
                ]
            )
            
            try:
                client.query(update_query, job_config=job_config).result()
                updated += 1
                print(f"    ✅ Updated with {total_reactions} reactions")
            except Exception as e:
                print(f"    ❌ Update failed: {e}")
        
        time.sleep(1.5)  # Respect rate limits
    
    print(f"\n✅ Updated {updated} posts with metadata")


def main():
    parser = argparse.ArgumentParser(description="LinkedIn Data Pipeline")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental",
                       help="incremental: only new posts, full: flush and reload all")
    parser.add_argument("--max-posts", type=int, default=None,
                       help="Maximum posts to fetch (for testing)")
    parser.add_argument("--no-metadata", action="store_true",
                       help="Skip fetching social metadata (faster)")
    parser.add_argument("--max-metadata-calls", type=int, default=None,
                       help="Max metadata API calls per run (to stay within daily limits, e.g., 100)")
    parser.add_argument("--update-metadata", action="store_true",
                       help="Update existing posts that have no reaction data")
    parser.add_argument("--update-limit", type=int, default=90,
                       help="Max posts to update when using --update-metadata (default: 90, LinkedIn limit is 100/user/day)")
    
    args = parser.parse_args()
    
    if args.update_metadata:
        update_missing_metadata(max_calls=args.update_limit)
    else:
        run_pipeline(
            mode=args.mode,
            max_posts=args.max_posts,
            fetch_metadata=not args.no_metadata,
            max_metadata_calls=args.max_metadata_calls
        )

if __name__ == "__main__":
    main()
