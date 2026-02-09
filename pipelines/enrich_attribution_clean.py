"""
Clean GA4 Attribution Enrichment Script

This script enriches the Descope users table with ONLY essential fields:
- Marketing attribution (source, medium, campaign, channel)
- Conversion timing (days to convert)
- Geo location (Descope + GA4 fallback)
- Engagement metrics (sessions, events, days active)
- Product usage (platform, apps, version)
- Page/session info
"""

import os
import json
from datetime import datetime
from google.cloud import bigquery
import argparse

# --- CONFIGURATION (from env for multi-org reuse) ---
PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET_ID = os.environ.get("BQ_DATASET_USERS", "")
TABLE_ID = os.environ.get("BQ_TABLE_USERS_GA4_ENRICHMENT", "users_ga4_enrichment")
GA4_DATASET_ID = os.environ.get("BQ_DATASET_ANALYTICS", "")
if not PROJECT_ID or not DATASET_ID or not GA4_DATASET_ID:
    raise SystemExit("Set GCP_PROJECT, BQ_DATASET_USERS, BQ_DATASET_ANALYTICS in .env (see .env.example)")

# --- CREATE BIGQUERY CLIENT ---
bq_client = bigquery.Client(project=PROJECT_ID)


def add_clean_schema_fields():
    """Add new clean fields to the schema"""
    print("📋 Adding clean schema fields...")
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    table = bq_client.get_table(table_ref)
    
    new_fields = [
        # Simple traffic source (cleaner field names)
        bigquery.SchemaField("traffic_source_simple", "STRING"),
        bigquery.SchemaField("traffic_medium_simple", "STRING"),
        bigquery.SchemaField("traffic_campaign_simple", "STRING"),
        
        # Page info
        bigquery.SchemaField("first_page_title", "STRING"),
        bigquery.SchemaField("first_page_path", "STRING"),
        
        # Session info
        bigquery.SchemaField("ga_session_count", "INTEGER"),
        bigquery.SchemaField("first_ga_session_id", "STRING"),
    ]
    
    existing_fields = {field.name for field in table.schema}
    fields_to_add = [field for field in new_fields if field.name not in existing_fields]
    
    if not fields_to_add:
        print("✅ All clean fields already exist")
        return
    
    new_schema = table.schema + fields_to_add
    table.schema = new_schema
    table = bq_client.update_table(table, ["schema"])
    
    print(f"✅ Added {len(fields_to_add)} new fields:")
    for field in fields_to_add:
        print(f"  - {field.name} ({field.field_type})")


def enrich_attribution_clean(dry_run=False, limit=None):
    """
    Enrich with CLEAN, essential fields only
    """
    print("🚀 Starting clean GA4 attribution enrichment...")
    
    # Helper to extract event params
    def extract_param(key, value_type='string_value'):
        return f"(SELECT value.{value_type} FROM UNNEST(event_params) WHERE key = '{key}')"
    
    # Build enrichment query
    enrichment_query = f"""
    WITH ga4_events AS (
      SELECT
        {extract_param('pieces_descope_id')} as descope_id,
        event_timestamp,
        user_first_touch_timestamp,
        
        -- Simple traffic source (cleaner)
        traffic_source.source as traffic_source,
        traffic_source.medium as traffic_medium,
        traffic_source.name as traffic_campaign,
        
        -- Session traffic source (for channel grouping)
        session_traffic_source_last_click.cross_channel_campaign.source as session_source,
        session_traffic_source_last_click.cross_channel_campaign.medium as session_medium,
        session_traffic_source_last_click.cross_channel_campaign.campaign_name as session_campaign,
        session_traffic_source_last_click.cross_channel_campaign.default_channel_group as channel_group,
        
        -- Page info
        {extract_param('page_location')} as page_location,
        {extract_param('page_title')} as page_title,
        {extract_param('path')} as page_path,
        {extract_param('page_referrer')} as page_referrer,
        
        -- Session info
        {extract_param('ga_session_id', 'int_value')} as session_id,
        {extract_param('ga_session_number', 'int_value')} as session_number,
        {extract_param('engagement_time_msec', 'int_value')} as engagement_time_msec,
        
        -- Product info
        {extract_param('pieces_user_id')} as pieces_user_id,
        {extract_param('pieces_apps')} as pieces_apps,
        {extract_param('pieces_apps_json')} as pieces_apps_json,
        
        -- Geo from GA4
        geo.city as ga4_city,
        geo.region as ga4_region,
        geo.country as ga4_country,
        geo.continent as ga4_continent
        
      FROM `{PROJECT_ID}.{GA4_DATASET_ID}.events_*`
      WHERE event_name = 'piecesos_data'
        AND {extract_param('pieces_descope_id')} IS NOT NULL
    ),
    
    first_touch AS (
      SELECT
        descope_id,
        
        -- Simple traffic source (first touch)
        FIRST_VALUE(traffic_source) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_traffic_source,
        FIRST_VALUE(traffic_medium) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_traffic_medium,
        FIRST_VALUE(traffic_campaign) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_traffic_campaign,
        
        -- Session source (for channel grouping - more reliable)
        FIRST_VALUE(COALESCE(session_source, traffic_source)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_session_source,
        FIRST_VALUE(COALESCE(session_medium, traffic_medium)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_session_medium,
        FIRST_VALUE(COALESCE(session_campaign, traffic_campaign)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_session_campaign,
        FIRST_VALUE(channel_group) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_channel_group,
        
        -- Page info
        FIRST_VALUE(page_location) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_page_location,
        FIRST_VALUE(page_title) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_page_title,
        FIRST_VALUE(page_path) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_page_path,
        FIRST_VALUE(page_referrer) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_referrer,
        
        -- First touch timestamp (use user_first_touch_timestamp if available, otherwise event timestamp)
        FIRST_VALUE(TIMESTAMP_MICROS(COALESCE(user_first_touch_timestamp, event_timestamp))) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_website_visit,
        
        -- First session info
        FIRST_VALUE(session_id) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_session_id
        
      FROM ga4_events
      QUALIFY ROW_NUMBER() OVER (PARTITION BY descope_id ORDER BY event_timestamp) = 1
    ),
    
    engagement_metrics AS (
      SELECT
        descope_id,
        
        -- Activation
        MIN(TIMESTAMP_MICROS(event_timestamp)) as activation_date,
        MAX(TIMESTAMP_MICROS(event_timestamp)) as last_activity_timestamp,
        TRUE as is_activated,
        
        -- Engagement
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(*) as total_events,
        CAST(SUM(COALESCE(engagement_time_msec, 0)) / 1000 AS INT64) as total_engagement_time_sec,
        COUNT(DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp))) as days_active,
        MAX(session_number) as ga_session_count,
        
        -- Product usage
        MAX(pieces_user_id) as pieces_user_id,
        MAX(pieces_apps) as pieces_apps,
        
        -- Parse product details from JSON
        MAX(REGEXP_EXTRACT(pieces_apps_json, r'"name":"OS_SERVER","version":"([^"]+)"')) as pieces_os_version,
        MAX(REGEXP_EXTRACT(pieces_apps_json, r'"platform":"([^"]+)"')) as pieces_platform,
        
        -- Geo from GA4 (most recent)
        ARRAY_AGG(ga4_city ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_city,
        ARRAY_AGG(ga4_region ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_region,
        ARRAY_AGG(ga4_country ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_country,
        ARRAY_AGG(ga4_continent ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_continent
        
      FROM ga4_events
      GROUP BY descope_id
    )
    
    -- Combine all enrichments
    SELECT
      ft.descope_id as user_id,
      
      -- Simple traffic source (cleaner field names)
      ft.first_traffic_source as traffic_source_simple,
      ft.first_traffic_medium as traffic_medium_simple,
      ft.first_traffic_campaign as traffic_campaign_simple,
      
      -- Session source (for existing first_touch_* fields compatibility)
      ft.first_session_source as first_touch_source,
      ft.first_session_medium as first_touch_medium,
      ft.first_session_campaign as first_touch_campaign,
      ft.first_channel_group as first_touch_channel_group,
      
      -- Page info
      ft.first_page_location as first_touch_landing_page,
      ft.first_page_title,
      ft.first_page_path,
      ft.first_referrer as first_touch_referrer,
      
      -- Timing
      ft.first_website_visit as first_touch_timestamp,
      
      -- Engagement
      em.activation_date,
      em.last_activity_timestamp,
      em.is_activated,
      em.total_sessions,
      em.total_events,
      em.total_engagement_time_sec,
      em.days_active,
      
      -- Session info
      em.ga_session_count,
      CAST(ft.first_session_id AS STRING) as first_ga_session_id,
      
      -- Product
      em.pieces_user_id,
      em.pieces_apps,
      em.pieces_os_version,
      em.pieces_platform,
      
      -- Geo from GA4
      em.ga4_city,
      em.ga4_region,
      em.ga4_country,
      em.ga4_continent
      
    FROM first_touch ft
    LEFT JOIN engagement_metrics em ON ft.descope_id = em.descope_id
    """
    
    if limit:
        enrichment_query += f"\nLIMIT {limit}"
    
    if dry_run:
        print("\n📋 DRY RUN - Showing sample enriched data:\n")
        query_job = bq_client.query(enrichment_query + " LIMIT 5")
        results = query_job.result()
        
        for row in results:
            print(f"\n{'='*80}")
            print(f"User ID: {row.user_id}")
            print(f"Traffic: {row.traffic_source_simple} / {row.traffic_medium_simple} ({row.first_touch_channel_group})")
            print(f"First Page: {row.first_page_title} ({row.first_page_path})")
            print(f"Sessions: {row.total_sessions}, Events: {row.total_events}, Days Active: {row.days_active}")
            print(f"Product: {row.pieces_platform} v{row.pieces_os_version}")
            print(f"Location: {row.ga4_city}, {row.ga4_country}")
        
        print(f"\n{'='*80}\n")
        print("✅ Dry run complete!")
        return
    
    # Execute enrichment
    print("🔄 Creating temporary table with enriched data...")
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_attribution_{int(datetime.now().timestamp())}"
    
    job_config = bigquery.QueryJobConfig(destination=temp_table_id)
    query_job = bq_client.query(enrichment_query, job_config=job_config)
    query_job.result()
    
    print(f"✅ Temporary table created: {temp_table_id}")
    
    # Get count
    count_query = f"SELECT COUNT(*) as count FROM `{temp_table_id}`"
    result = list(bq_client.query(count_query).result())[0]
    enriched_count = result.count
    
    if enriched_count == 0:
        print("  ⚠️ No users to enrich")
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        return 0
    
    print(f"📊 Enriched {enriched_count} users")
    
    # MERGE into main table
    print("🔄 Merging enriched data...")
    
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN UPDATE SET
        -- Simple traffic source
        traffic_source_simple = S.traffic_source_simple,
        traffic_medium_simple = S.traffic_medium_simple,
        traffic_campaign_simple = S.traffic_campaign_simple,
        
        -- Session source (existing fields)
        first_touch_source = S.first_touch_source,
        first_touch_medium = S.first_touch_medium,
        first_touch_campaign = S.first_touch_campaign,
        first_touch_channel_group = S.first_touch_channel_group,
        first_touch_landing_page = S.first_touch_landing_page,
        first_touch_referrer = S.first_touch_referrer,
        first_touch_timestamp = S.first_touch_timestamp,
        
        -- Page info
        first_page_title = S.first_page_title,
        first_page_path = S.first_page_path,
        
        -- Engagement
        activation_date = S.activation_date,
        last_activity_timestamp = S.last_activity_timestamp,
        is_activated = S.is_activated,
        total_sessions = S.total_sessions,
        total_events = S.total_events,
        total_engagement_time_sec = S.total_engagement_time_sec,
        days_active = S.days_active,
        
        -- Session info
        ga_session_count = S.ga_session_count,
        first_ga_session_id = S.first_ga_session_id,
        
        -- Product
        pieces_user_id = S.pieces_user_id,
        pieces_apps = S.pieces_apps,
        pieces_os_version = S.pieces_os_version,
        pieces_platform = S.pieces_platform,
        
        -- Geo from GA4
        ga4_city = S.ga4_city,
        ga4_region = S.ga4_region,
        ga4_country = S.ga4_country,
        ga4_continent = S.ga4_continent
    """
    
    bq_client.query(merge_query).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    print(f"✅ MERGE complete! Updated {enriched_count} users")
    
    # Show stats
    print("\n📊 Attribution Summary:")
    stats_query = f"""
    SELECT 
      first_touch_channel_group,
      COUNT(*) as users,
      ROUND(AVG(DATE_DIFF(CURRENT_DATE(), DATE(created_time), DAY)), 1) as avg_account_age_days,
      ROUND(AVG(total_sessions), 1) as avg_sessions,
      ROUND(AVG(days_active), 1) as avg_days_active
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE first_touch_channel_group IS NOT NULL
    GROUP BY 1
    ORDER BY users DESC
    """
    
    stats_result = bq_client.query(stats_query).result()
    print(f"\n{'Channel Group':<25} {'Users':<10} {'Avg Age (days)':<18} {'Avg Sessions':<15} {'Avg Days Active'}")
    print("-" * 95)
    for row in stats_result:
        print(f"{row.first_touch_channel_group:<25} {row.users:<10} {row.avg_account_age_days:<18} {row.avg_sessions:<15} {row.avg_days_active}")
    
    print("\n✅ Clean enrichment complete!")
    return enriched_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Clean GA4 attribution enrichment')
    parser.add_argument('--add-schema', action='store_true', help='Add new schema fields')
    parser.add_argument('--dry-run', action='store_true', help='Preview data without updating')
    parser.add_argument('--limit', type=int, help='Limit users to process')
    
    args = parser.parse_args()
    
    try:
        if args.add_schema:
            add_clean_schema_fields()
        else:
            enrich_attribution_clean(dry_run=args.dry_run, limit=args.limit)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)

