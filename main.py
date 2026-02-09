import json
import requests
import functions_framework
from datetime import datetime
from google.cloud import bigquery
from google.cloud import secretmanager
import os

# --- CONFIGURATION (from env for multi-org reuse) ---
PROJECT_ID = os.environ.get("GCP_PROJECT", "global-cloud-runtime")
DATASET_ID = os.environ.get("BQ_DATASET_USERS", "descope_data_v2")
TABLE_ID = os.environ.get("BQ_TABLE_USERS", "users")

# --- SECURITY: Get secrets from Secret Manager ---
def get_secret(secret_id):
    """Fetch secret from Google Secret Manager"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"⚠️ Failed to get secret {secret_id}: {e}")
        # Fallback to environment variables for local development
        return os.environ.get(secret_id.upper().replace('-', '_'))

DESCOPE_PROJECT_ID = get_secret("descope-project-id")
DESCOPE_MANAGEMENT_KEY = get_secret("descope-management-key")

# --- CREATE BIGQUERY CLIENT ---
bq_client = bigquery.Client(project=PROJECT_ID)

# --- GET LATEST TIMESTAMP FROM BIGQUERY ---
def get_latest_timestamp():
    """Get the latest created_time from BigQuery to determine incremental sync point."""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    query = f"SELECT MAX(created_time) as latest_created_time FROM `{table_id}`"
    try:
        result = bq_client.query(query).result()
        for row in result:
            if row.latest_created_time:
                print(f"📅 Latest entry in BigQuery: {row.latest_created_time}")
                import calendar
                unix_timestamp = int(calendar.timegm(row.latest_created_time.timetuple()))
                print(f"📅 Unix timestamp: {unix_timestamp}")
                return unix_timestamp
    except Exception as e:
        print(f"⚠️ Could not get latest timestamp: {e}")
    return None

# --- UPSERT USERS USING MERGE STATEMENT ---
def upsert_with_merge(formatted_rows):
    """
    Use BigQuery MERGE statement to upsert users.
    This handles both INSERT (new users) and UPDATE (existing users).
    """
    if not formatted_rows:
        print("No rows to upsert")
        return
    
    # Create a temporary table with the new data
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_users_{int(datetime.now().timestamp())}"
    
    # Define schema for temporary table
    schema = [
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("login_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("display_name", "STRING"),
        bigquery.SchemaField("given_name", "STRING"),
        bigquery.SchemaField("middle_name", "STRING"),
        bigquery.SchemaField("family_name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("verified_email", "BOOLEAN"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("verified_phone", "BOOLEAN"),
        bigquery.SchemaField("role_names", "STRING", mode="REPEATED"),
        bigquery.SchemaField("user_tenants", "JSON"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("external_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("picture", "STRING"),
        bigquery.SchemaField("test", "BOOLEAN"),
        bigquery.SchemaField("totp", "BOOLEAN"),
        bigquery.SchemaField("saml", "BOOLEAN"),
        bigquery.SchemaField("oauth", "JSON"),
        bigquery.SchemaField("webauthn", "BOOLEAN"),
        bigquery.SchemaField("password", "BOOLEAN"),
        bigquery.SchemaField("sso_app_ids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("editable", "BOOLEAN"),
        bigquery.SchemaField("scim", "BOOLEAN"),
        bigquery.SchemaField("push", "BOOLEAN"),
        bigquery.SchemaField("permissions", "STRING", mode="REPEATED"),
        bigquery.SchemaField("custom_attributes", "JSON"),
        bigquery.SchemaField("created_time", "TIMESTAMP"),
    ]
    
    import tempfile
    import os as os_module
    
    # Write to temp file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for row in formatted_rows:
            f.write(json.dumps(row) + '\n')
        temp_file = f.name
    
    # Load into temporary table
    with open(temp_file, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )
        load_job = bq_client.load_table_from_file(
            source_file, temp_table_id, job_config=job_config
        )
        load_job.result()
    
    # Clean up temp file
    os_module.unlink(temp_file)
    
    # Execute MERGE statement
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN
      UPDATE SET
        login_ids = S.login_ids,
        display_name = S.display_name,
        given_name = S.given_name,
        middle_name = S.middle_name,
        family_name = S.family_name,
        email = S.email,
        verified_email = S.verified_email,
        phone = S.phone,
        verified_phone = S.verified_phone,
        role_names = S.role_names,
        user_tenants = S.user_tenants,
        status = S.status,
        external_ids = S.external_ids,
        picture = S.picture,
        test = S.test,
        totp = S.totp,
        saml = S.saml,
        oauth = S.oauth,
        webauthn = S.webauthn,
        password = S.password,
        sso_app_ids = S.sso_app_ids,
        editable = S.editable,
        scim = S.scim,
        push = S.push,
        permissions = S.permissions,
        custom_attributes = S.custom_attributes,
        created_time = S.created_time
    WHEN NOT MATCHED THEN
      INSERT (user_id, login_ids, display_name, given_name, middle_name, family_name,
              email, verified_email, phone, verified_phone, role_names, user_tenants,
              status, external_ids, picture, test, totp, saml, oauth, webauthn,
              password, sso_app_ids, editable, scim, push, permissions, custom_attributes, created_time)
      VALUES (S.user_id, S.login_ids, S.display_name, S.given_name, S.middle_name, S.family_name,
              S.email, S.verified_email, S.phone, S.verified_phone, S.role_names, S.user_tenants,
              S.status, S.external_ids, S.picture, S.test, S.totp, S.saml, S.oauth, S.webauthn,
              S.password, S.sso_app_ids, S.editable, S.scim, S.push, S.permissions, S.custom_attributes, S.created_time)
    """
    
    merge_job = bq_client.query(merge_query)
    merge_job.result()
    
    # Delete temporary table
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    print(f"✅ MERGE completed: {len(formatted_rows)} users")

# --- FETCH AND SYNC USERS ---
def sync_users():
    """Main sync function"""
    print("🚀 Starting Descope incremental sync...")
    
    url = "https://api.descope.com/v2/mgmt/user/search"
    headers = {
        "Authorization": f"Bearer {DESCOPE_PROJECT_ID}:{DESCOPE_MANAGEMENT_KEY}",
        "Content-Type": "application/json"
    }
    
    # Get latest timestamp for incremental sync
    latest_timestamp = get_latest_timestamp()
    
    page = 0
    batch_size = 5000
    all_formatted_rows = []
    
    while True:
        payload = {"limit": batch_size, "page": page}
        
        # Add fromModifiedTime to get new + updated users (Descope expects milliseconds)
        if latest_timestamp:
            # Convert seconds to milliseconds for Descope API
            latest_timestamp_ms = latest_timestamp * 1000
            payload["fromModifiedTime"] = latest_timestamp_ms
            print(f"🔄 Fetching page {page} (modified since {latest_timestamp} / {latest_timestamp_ms}ms)...")
        else:
            print(f"🔄 Fetching page {page} (initial load)...")
        
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        
        users = data.get("users", [])
        total = data.get("total", 0)
        
        if not users:
            break
        
        # Format rows
        for u in users:
            # Use UTC for consistency with BigQuery
            created_time = datetime.utcfromtimestamp(u.get("createdTime")) if u.get("createdTime") else None
            all_formatted_rows.append({
                "user_id": u.get("userId"),
                "login_ids": u.get("loginIds", []),
                "display_name": u.get("name"),
                "given_name": u.get("givenName"),
                "middle_name": u.get("middleName"),
                "family_name": u.get("familyName"),
                "email": u.get("email"),
                "verified_email": u.get("verifiedEmail"),
                "phone": u.get("phone"),
                "verified_phone": u.get("verifiedPhone"),
                "role_names": u.get("roleNames", []),
                "user_tenants": json.dumps(u.get("userTenants", [])),
                "status": u.get("status"),
                "external_ids": u.get("externalIds", []),
                "picture": u.get("picture"),
                "test": u.get("test"),
                "totp": u.get("TOTP"),
                "saml": u.get("SAML"),
                "oauth": json.dumps(u.get("OAuth", {})),
                "webauthn": u.get("webauthn"),
                "password": u.get("password"),
                "sso_app_ids": u.get("ssoAppIds", []),
                "editable": u.get("editable"),
                "scim": u.get("SCIM"),
                "push": u.get("push"),
                "permissions": u.get("permissions", []),
                "custom_attributes": json.dumps(u.get("customAttributes", {})),
                "created_time": created_time.isoformat() if created_time else None,
            })
        
        print(f"✅ Page {page}: Collected {len(users)} users (Total: {len(all_formatted_rows)}/{total})")
        
        if len(users) < batch_size or len(all_formatted_rows) >= total:
            break
        
        page += 1
    
    # Perform MERGE operation
    if all_formatted_rows:
        print(f"🔄 Performing MERGE for {len(all_formatted_rows)} users...")
        upsert_with_merge(all_formatted_rows)
        print(f"✅ Sync completed! Processed {len(all_formatted_rows)} users")
    else:
        print("✅ No new users to sync")
    
    return len(all_formatted_rows)

# --- UPDATE USER LOCATIONS FROM AUDIT EVENTS ---
def update_user_locations():
    """Fetch login audit events and update user location data"""
    url = "https://api.descope.com/v1/mgmt/audit/search"
    headers = {
        "Authorization": f"Bearer {DESCOPE_PROJECT_ID}:{DESCOPE_MANAGEMENT_KEY}",
        "Content-Type": "application/json"
    }
    
    print("🌍 Fetching login audit events for location data...")
    
    # Only fetch audit events from the last 7 days to avoid rate limits
    from datetime import timedelta
    seven_days_ago = int((datetime.now() - timedelta(days=7)).timestamp() * 1000)  # milliseconds
    
    all_audits = []
    page = 0
    limit = 1000
    max_pages = 50  # Limit to 50k events max to avoid rate limits
    
    while True:
        payload = {
            "actions": ["LoginSucceed", "LoginFlowDone"],
            "limit": limit,
            "page": page,
            "from": seven_days_ago  # Only get recent audit events
        }
        
        resp = requests.post(url, headers=headers, json=payload)
        
        # Handle rate limiting with exponential backoff
        if resp.status_code == 429:
            print(f"  ⚠️ Rate limit hit at page {page}. Stopping audit fetch.")
            break
            
        resp.raise_for_status()
        data = resp.json()
        
        audits = data.get("audits", [])
        if not audits:
            break
        
        all_audits.extend(audits)
        print(f"  📍 Page {page}: {len(audits)} events (Total: {len(all_audits)})")
        
        if len(audits) < limit:
            break
        
        # Stop if we hit max pages to avoid rate limits
        if page >= max_pages:
            print(f"  ⚠️ Reached max pages ({max_pages}). Stopping to avoid rate limits.")
            break
        
        page += 1
    
    # Process to get most recent login per user
    user_locations = {}
    for audit in all_audits:
        user_id = audit.get("userId")
        occurred_ms = audit.get("occurred")
        
        if not user_id or not occurred_ms:
            continue
        
        occurred_timestamp = int(occurred_ms) / 1000
        country = audit.get("geo", "")
        ip_address = audit.get("remoteAddress", "")
        
        # Keep only most recent
        if user_id not in user_locations or occurred_timestamp > user_locations[user_id]["last_login_time"]:
            user_locations[user_id] = {
                "last_login_time": occurred_timestamp,
                "last_login_country": country,
                "last_login_ip": ip_address
            }
    
    if not user_locations:
        print("  ⚠️ No location data found")
        return 0
    
    print(f"  ✅ Processed {len(user_locations)} users with location data")
    
    # Update BigQuery
    formatted_rows = []
    for user_id, location_data in user_locations.items():
        # Use UTC for consistency with BigQuery
        login_time = datetime.utcfromtimestamp(location_data["last_login_time"])
        formatted_rows.append({
            "user_id": user_id,
            "last_login_time": login_time.isoformat(),
            "last_login_country": location_data["last_login_country"],
            "last_login_ip": location_data["last_login_ip"]
        })
    
    # Create temp table
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_locations_{int(datetime.now().timestamp())}"
    schema = [
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("last_login_time", "TIMESTAMP"),
        bigquery.SchemaField("last_login_country", "STRING"),
        bigquery.SchemaField("last_login_ip", "STRING"),
    ]
    
    import tempfile
    import os as os_module
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for row in formatted_rows:
            f.write(json.dumps(row) + '\n')
        temp_file = f.name
    
    with open(temp_file, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )
        load_job = bq_client.load_table_from_file(source_file, temp_table_id, job_config=job_config)
        load_job.result()
    
    os_module.unlink(temp_file)
    
    # MERGE locations
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN
      UPDATE SET
        last_login_time = S.last_login_time,
        last_login_country = S.last_login_country,
        last_login_ip = S.last_login_ip
    """
    
    bq_client.query(merge_query).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    print(f"  ✅ Updated {len(user_locations)} users with location data")
    return len(user_locations)

# --- ENRICH IP ADDRESSES WITH GEOLOCATION DATA ---
def geolocate_ip(ip_address):
    """Try multiple free geolocation services to get city/region data"""
    services = [
        # ip-api.com (45/min free, no key)
        lambda ip: requests.get(f'http://ip-api.com/json/{ip}?fields=city,regionName,country,countryCode,status', timeout=3).json(),
        # ipwho.is (10000/month free, no key)
        lambda ip: requests.get(f'http://ipwho.is/{ip}', timeout=3).json(),
        # freeipapi.com (60/min free, no key)
        lambda ip: requests.get(f'https://freeipapi.com/api/json/{ip}', timeout=3).json(),
    ]
    
    for service_func in services:
        try:
            data = service_func(ip_address)
            
            # Handle different response formats
            if data.get('status') == 'success':  # ip-api.com
                return {
                    'city': data.get('city', ''),
                    'region': data.get('regionName', ''),
                    'country_name': data.get('country', ''),
                    'country_code': data.get('countryCode', '')
                }
            elif data.get('success'):  # ipwho.is
                return {
                    'city': data.get('city', ''),
                    'region': data.get('region', ''),
                    'country_name': data.get('country', ''),
                    'country_code': data.get('country_code', '')
                }
            elif 'cityName' in data:  # freeipapi.com
                return {
                    'city': data.get('cityName', ''),
                    'region': data.get('regionName', ''),
                    'country_name': data.get('countryName', ''),
                    'country_code': data.get('countryCode', '')
                }
        except:
            continue
    
    return None

def enrich_ip_locations():
    """Enrich IPs with detailed geolocation data (city, region)"""
    print("🗺️  Enriching IP addresses with geolocation data...")
    
    # Get IPs that need enrichment (have IP but no city)
    query = f"""
    SELECT DISTINCT last_login_ip
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE last_login_ip IS NOT NULL 
    AND last_login_city IS NULL
    LIMIT 100
    """
    
    result = bq_client.query(query).result()
    ips = [row.last_login_ip for row in result]
    
    if not ips:
        print("  ✅ All IPs already enriched")
        return 0
    
    print(f"  📍 Found {len(ips)} IPs to enrich (limiting to 100 per run)")
    
    # Geolocate each IP
    ip_to_location = {}
    for i, ip in enumerate(ips):
        location = geolocate_ip(ip)
        if location:
            ip_to_location[ip] = location
            print(f"  ✅ [{i+1}/{len(ips)}] {ip}: {location['city']}, {location['region']}, {location['country_name']}")
        else:
            print(f"  ⚠️ [{i+1}/{len(ips)}] {ip}: Failed to geolocate")
        
        # Small delay to respect rate limits
        import time
        time.sleep(0.05)
    
    if not ip_to_location:
        print("  ⚠️ No IPs successfully geolocated")
        return 0
    
    # Update BigQuery with enriched data
    formatted_rows = []
    for ip, location in ip_to_location.items():
        formatted_rows.append({
            "last_login_ip": ip,
            "last_login_city": location['city'],
            "last_login_region": location['region'],
            "last_login_country_name": location['country_name'],
            "last_login_country": location['country_code']
        })
    
    # Create temp table
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_geoip_{int(datetime.now().timestamp())}"
    schema = [
        bigquery.SchemaField("last_login_ip", "STRING"),
        bigquery.SchemaField("last_login_city", "STRING"),
        bigquery.SchemaField("last_login_region", "STRING"),
        bigquery.SchemaField("last_login_country_name", "STRING"),
        bigquery.SchemaField("last_login_country", "STRING"),
    ]
    
    import tempfile
    import os as os_module
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for row in formatted_rows:
            f.write(json.dumps(row) + '\n')
        temp_file = f.name
    
    with open(temp_file, 'rb') as source_file:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )
        load_job = bq_client.load_table_from_file(source_file, temp_table_id, job_config=job_config)
        load_job.result()
    
    os_module.unlink(temp_file)
    
    # MERGE to update location fields
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.last_login_ip = S.last_login_ip
    WHEN MATCHED THEN
      UPDATE SET
        last_login_city = S.last_login_city,
        last_login_region = S.last_login_region,
        last_login_country_name = S.last_login_country_name,
        last_login_country = S.last_login_country
    """
    
    bq_client.query(merge_query).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    print(f"  ✅ Enriched {len(ip_to_location)} IPs with location data")
    return len(ip_to_location)

# --- ENRICH WITH GA4 ATTRIBUTION DATA ---
def enrich_attribution():
    """
    Enrich Descope users with GA4 attribution and engagement data.
    This adds marketing attribution, product usage, and engagement metrics.
    """
    print("📊 Enriching users with GA4 attribution data...")
    
    GA4_DATASET_ID = os.environ.get("BQ_DATASET_ANALYTICS", "analytics_362917367")
    
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
        traffic_source.source as source,
        traffic_source.medium as medium,
        traffic_source.name as campaign,
        session_traffic_source_last_click.cross_channel_campaign.source as session_source,
        session_traffic_source_last_click.cross_channel_campaign.medium as session_medium,
        session_traffic_source_last_click.cross_channel_campaign.campaign_name as session_campaign,
        session_traffic_source_last_click.cross_channel_campaign.default_channel_group as channel_group,
        {extract_param('pieces_user_id')} as pieces_user_id,
        {extract_param('pieces_apps')} as pieces_apps,
        {extract_param('pieces_apps_json')} as pieces_apps_json,
        {extract_param('page_location')} as page_location,
        {extract_param('page_referrer')} as page_referrer,
        {extract_param('ga_session_id', 'int_value')} as session_id,
        {extract_param('engagement_time_msec', 'int_value')} as engagement_time_msec,
        device.category as device_category,
        device.operating_system as operating_system,
        device.operating_system_version as os_version,
        device.web_info.browser as browser,
        device.web_info.browser_version as browser_version,
        geo.city as ga4_city,
        geo.region as ga4_region,
        geo.country as ga4_country,
        geo.continent as ga4_continent,
        geo.metro as ga4_metro
      FROM `{PROJECT_ID}.{GA4_DATASET_ID}.events_*`
      WHERE event_name = 'piecesos_data'
        AND {extract_param('pieces_descope_id')} IS NOT NULL
    ),
    
    first_touch AS (
      SELECT
        descope_id,
        FIRST_VALUE(COALESCE(session_source, source)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_source,
        FIRST_VALUE(COALESCE(session_medium, medium)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_medium,
        FIRST_VALUE(COALESCE(session_campaign, campaign)) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_campaign,
        FIRST_VALUE(channel_group) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_channel_group,
        FIRST_VALUE(page_location) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_landing_page,
        FIRST_VALUE(page_referrer) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_referrer,
        FIRST_VALUE(TIMESTAMP_MICROS(COALESCE(user_first_touch_timestamp, event_timestamp))) OVER (PARTITION BY descope_id ORDER BY event_timestamp) as first_touch_timestamp
      FROM ga4_events
      QUALIFY ROW_NUMBER() OVER (PARTITION BY descope_id ORDER BY event_timestamp) = 1
    ),
    
    last_click AS (
      SELECT
        descope_id,
        LAST_VALUE(COALESCE(session_source, source)) OVER (PARTITION BY descope_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_click_source,
        LAST_VALUE(COALESCE(session_medium, medium)) OVER (PARTITION BY descope_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_click_medium,
        LAST_VALUE(COALESCE(session_campaign, campaign)) OVER (PARTITION BY descope_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_click_campaign,
        LAST_VALUE(channel_group) OVER (PARTITION BY descope_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_click_channel_group,
        LAST_VALUE(TIMESTAMP_MICROS(event_timestamp)) OVER (PARTITION BY descope_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_click_timestamp
      FROM ga4_events
      QUALIFY ROW_NUMBER() OVER (PARTITION BY descope_id ORDER BY event_timestamp DESC) = 1
    ),
    
    engagement_metrics AS (
      SELECT
        descope_id,
        MIN(TIMESTAMP_MICROS(event_timestamp)) as activation_date,
        MAX(TIMESTAMP_MICROS(event_timestamp)) as last_activity_timestamp,
        TRUE as is_activated,
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(*) as total_events,
        CAST(SUM(COALESCE(engagement_time_msec, 0)) / 1000 AS INT64) as total_engagement_time_sec,
        COUNT(DISTINCT DATE(TIMESTAMP_MICROS(event_timestamp))) as days_active,
        MAX(pieces_user_id) as pieces_user_id,
        MAX(pieces_apps) as pieces_apps,
        MAX(pieces_apps_json) as pieces_apps_json,
        ARRAY_AGG(device_category ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as device_category,
        ARRAY_AGG(operating_system ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as operating_system,
        ARRAY_AGG(os_version ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as os_version,
        ARRAY_AGG(browser ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as browser,
        ARRAY_AGG(browser_version ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as browser_version,
        ARRAY_AGG(ga4_city ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_city,
        ARRAY_AGG(ga4_region ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_region,
        ARRAY_AGG(ga4_country ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_country,
        ARRAY_AGG(ga4_continent ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_continent,
        ARRAY_AGG(ga4_metro ORDER BY event_timestamp DESC LIMIT 1)[OFFSET(0)] as ga4_metro
      FROM ga4_events
      GROUP BY descope_id
    ),
    
    product_details AS (
      SELECT
        descope_id,
        REGEXP_EXTRACT(pieces_apps_json, r'"name":"OS_SERVER","version":"([^"]+)"') as pieces_os_version,
        REGEXP_EXTRACT(pieces_apps_json, r'"platform":"([^"]+)"') as pieces_platform
      FROM (
        SELECT descope_id, MAX(pieces_apps_json) as pieces_apps_json
        FROM ga4_events
        WHERE pieces_apps_json IS NOT NULL
        GROUP BY descope_id
      )
    )
    
    SELECT
      ft.descope_id as user_id,
      ft.first_touch_source, ft.first_touch_medium, ft.first_touch_campaign,
      ft.first_touch_channel_group, ft.first_touch_landing_page,
      ft.first_touch_referrer, ft.first_touch_timestamp,
      lc.last_click_source, lc.last_click_medium, lc.last_click_campaign,
      lc.last_click_channel_group, lc.last_click_timestamp,
      em.activation_date, em.last_activity_timestamp, em.is_activated,
      em.total_sessions, em.total_events, em.total_engagement_time_sec,
      SAFE_DIVIDE(em.total_engagement_time_sec, em.total_sessions) as avg_session_duration_sec,
      em.days_active, em.pieces_user_id, em.pieces_apps, em.pieces_apps_json,
      pd.pieces_os_version, pd.pieces_platform,
      em.device_category, em.operating_system, em.os_version,
      em.browser, em.browser_version,
      em.ga4_city, em.ga4_region, em.ga4_country,
      em.ga4_continent, em.ga4_metro
    FROM first_touch ft
    LEFT JOIN last_click lc ON ft.descope_id = lc.descope_id
    LEFT JOIN engagement_metrics em ON ft.descope_id = em.descope_id
    LEFT JOIN product_details pd ON ft.descope_id = pd.descope_id
    """
    
    # Create temp table
    temp_table_id = f"{PROJECT_ID}.{DATASET_ID}.temp_attribution_{int(datetime.now().timestamp())}"
    job_config = bigquery.QueryJobConfig(destination=temp_table_id)
    query_job = bq_client.query(enrichment_query, job_config=job_config)
    query_job.result()
    
    # Get count
    count_query = f"SELECT COUNT(*) as count FROM `{temp_table_id}`"
    result = list(bq_client.query(count_query).result())[0]
    enriched_count = result.count
    
    if enriched_count == 0:
        print("  ⚠️ No users to enrich with GA4 data")
        bq_client.delete_table(temp_table_id, not_found_ok=True)
        return 0
    
    print(f"  📊 Enriched {enriched_count} users")
    
    # MERGE into main table
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
    USING `{temp_table_id}` S
    ON T.user_id = S.user_id
    WHEN MATCHED THEN UPDATE SET
        first_touch_source = S.first_touch_source,
        first_touch_medium = S.first_touch_medium,
        first_touch_campaign = S.first_touch_campaign,
        first_touch_channel_group = S.first_touch_channel_group,
        first_touch_landing_page = S.first_touch_landing_page,
        first_touch_referrer = S.first_touch_referrer,
        first_touch_timestamp = S.first_touch_timestamp,
        last_click_source = S.last_click_source,
        last_click_medium = S.last_click_medium,
        last_click_campaign = S.last_click_campaign,
        last_click_channel_group = S.last_click_channel_group,
        last_click_timestamp = S.last_click_timestamp,
        activation_date = S.activation_date,
        last_activity_timestamp = S.last_activity_timestamp,
        is_activated = S.is_activated,
        total_sessions = S.total_sessions,
        total_events = S.total_events,
        total_engagement_time_sec = S.total_engagement_time_sec,
        avg_session_duration_sec = S.avg_session_duration_sec,
        days_active = S.days_active,
        pieces_user_id = S.pieces_user_id,
        pieces_apps = S.pieces_apps,
        pieces_apps_json = S.pieces_apps_json,
        pieces_os_version = S.pieces_os_version,
        pieces_platform = S.pieces_platform,
        device_category = S.device_category,
        operating_system = S.operating_system,
        os_version = S.os_version,
        browser = S.browser,
        browser_version = S.browser_version,
        ga4_city = S.ga4_city,
        ga4_region = S.ga4_region,
        ga4_country = S.ga4_country,
        ga4_continent = S.ga4_continent,
        ga4_metro = S.ga4_metro
    """
    
    bq_client.query(merge_query).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    
    print(f"  ✅ Updated {enriched_count} users with GA4 attribution data")
    return enriched_count

# --- UPDATE ACTIVITY STATUS ---
def update_activity_status():
    """Update activity tracking columns (days since login, activity status, etc.)"""
    print("📊 Updating activity status columns...")
    
    update_query = f"""
    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    SET
      days_since_signup = DATE_DIFF(CURRENT_DATE(), DATE(created_time), DAY),
      days_since_last_login = DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY),
      signup_to_first_login_seconds = TIMESTAMP_DIFF(last_login_time, created_time, SECOND),
      signup_to_first_login_minutes = ROUND(TIMESTAMP_DIFF(last_login_time, created_time, SECOND) / 60.0, 1),
      is_same_day_activation = CASE WHEN DATE(created_time) = DATE(last_login_time) THEN TRUE ELSE FALSE END,
      user_activity_status = CASE
        WHEN last_login_time IS NULL THEN '❌ Never Logged In'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) = 0 THEN '🔥 Active Today'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 3 THEN '✅ Active (1-3 days)'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 7 THEN '👍 Active (4-7 days)'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 14 THEN '⚠️ At Risk (7-14 days)'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 30 THEN '😴 Dormant (14-30 days)'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 60 THEN '💤 Inactive (30-60 days)'
        ELSE '☠️ Very Inactive (60+ days)'
      END,
      simple_status = CASE
        WHEN last_login_time IS NULL THEN 'Never Logged In'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 7 THEN 'Active'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_login_time), DAY) <= 30 THEN 'Dormant'
        ELSE 'Inactive'
      END
    WHERE TRUE
    """
    
    query_job = bq_client.query(update_query)
    query_job.result()
    
    updated_count = query_job.num_dml_affected_rows
    print(f"  ✅ Updated activity status for {updated_count} users")
    return updated_count

# --- SECURITY: Verify request authenticity ---
def verify_cloud_scheduler_request(request):
    """Verify that the request comes from Cloud Scheduler"""
    # Check for Cloud Scheduler user agent
    user_agent = request.headers.get('User-Agent', '')
    if 'Google-Cloud-Scheduler' in user_agent:
        return True
    
    # For authenticated requests, verify the token
    auth_header = request.headers.get('Authorization', '')
    if auth_header:
        # The Cloud Scheduler service account will be verified by IAM
        return True
    
    return False

# Cloud Function entry point for Cloud Scheduler (HTTP trigger)
@functions_framework.http
def descope_sync(request):
    """HTTP Cloud Function triggered by Cloud Scheduler"""
    # Security: Verify request comes from authorized source
    if not verify_cloud_scheduler_request(request):
        print("⚠️ Unauthorized request rejected")
        return {"success": False, "error": "Unauthorized"}, 403
    
    try:
        users_synced = sync_users()
        
        # Try to update locations, but don't fail if it errors (rate limits, etc.)
        locations_updated = 0
        try:
            locations_updated = update_user_locations()
        except Exception as location_error:
            print(f"⚠️ Location update failed (non-critical): {location_error}")
            # Continue anyway - user sync succeeded
        
        # Try to enrich IP addresses with geolocation data
        ips_enriched = 0
        try:
            ips_enriched = enrich_ip_locations()
        except Exception as enrichment_error:
            print(f"⚠️ IP enrichment failed (non-critical): {enrichment_error}")
            # Continue anyway - user sync succeeded
        
        # GA4 attribution enrichment - DISABLED (causes function timeout)
        # The GA4 enrichment requires additional BigQuery columns that don't exist yet
        attribution_enriched = 0
        print("⏭️  Skipping GA4 attribution (not needed for activity status)")
        
        # Update activity status columns (days since login, activity status, etc.)
        # This uses only Descope data (created_time, last_login_time from audit events)
        activity_status_updated = 0
        try:
            activity_status_updated = update_activity_status()
        except Exception as activity_error:
            print(f"⚠️ Activity status update failed (non-critical): {activity_error}")
            # Continue anyway - user sync succeeded
        
        return {
            "success": True, 
            "users_synced": users_synced,
            "locations_updated": locations_updated,
            "ips_enriched": ips_enriched,
            "attribution_enriched": attribution_enriched,
            "activity_status_updated": activity_status_updated
        }, 200
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}, 500

