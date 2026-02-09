import json
import requests
import time
from datetime import datetime
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION (from env for multi-org reuse) ---
PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET_ID = os.environ.get("BQ_DATASET_USERS", "")
TABLE_ID = os.environ.get("BQ_TABLE_USERS", "users")
if not PROJECT_ID or not DATASET_ID:
    raise SystemExit("Set GCP_PROJECT and BQ_DATASET_USERS in .env (see .env.example)")

# --- CREATE BIGQUERY CLIENT ---
bq_client = bigquery.Client(project=PROJECT_ID)

def get_ips_to_geolocate(force_update=False):
    """Get all unique IPs that need geolocation
    
    Args:
        force_update: If True, re-geolocate ALL IPs (even if they already have city data)
    """
    if force_update:
        query = f"""
        SELECT DISTINCT last_login_ip
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE last_login_ip IS NOT NULL
        """
        print("🔄 Force update mode: Will re-geolocate ALL IPs")
    else:
        query = f"""
        SELECT DISTINCT last_login_ip
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE last_login_ip IS NOT NULL 
        AND last_login_city IS NULL
        """
    
    result = bq_client.query(query).result()
    ips = [row.last_login_ip for row in result]
    print(f"📍 Found {len(ips)} unique IPs to geolocate")
    return ips

def geolocate_ip_ipapi(ip_address):
    """Use ip-api.com (45/min free, no key)"""
    try:
        resp = requests.get(f'http://ip-api.com/json/{ip_address}?fields=city,regionName,country,countryCode,status,message', timeout=5)
        data = resp.json()
        
        if data.get('status') == 'success':
            return {
                'city': data.get('city', ''),
                'region': data.get('regionName', ''),
                'country_name': data.get('country', ''),
                'country_code': data.get('countryCode', ''),
                'source': 'ip-api.com'
            }
    except Exception as e:
        pass
    return None

def geolocate_ip_ipapi_co(ip_address):
    """Use ipapi.co (1000/day free, no key)"""
    try:
        resp = requests.get(f'https://ipapi.co/{ip_address}/json/', timeout=5)
        data = resp.json()
        
        if 'error' not in data:
            return {
                'city': data.get('city', ''),
                'region': data.get('region', ''),
                'country_name': data.get('country_name', ''),
                'country_code': data.get('country_code', ''),
                'source': 'ipapi.co'
            }
    except Exception as e:
        pass
    return None

def geolocate_ip_ipwhois(ip_address):
    """Use ipwhois.io (10000/month free, no key)"""
    try:
        resp = requests.get(f'http://ipwho.is/{ip_address}', timeout=5)
        data = resp.json()
        
        if data.get('success'):
            return {
                'city': data.get('city', ''),
                'region': data.get('region', ''),
                'country_name': data.get('country', ''),
                'country_code': data.get('country_code', ''),
                'source': 'ipwho.is'
            }
    except Exception as e:
        pass
    return None

def geolocate_ip_freeipapi(ip_address):
    """Use freeipapi.com (60/min free, no key)"""
    try:
        resp = requests.get(f'https://freeipapi.com/api/json/{ip_address}', timeout=5)
        data = resp.json()
        
        if 'cityName' in data:
            return {
                'city': data.get('cityName', ''),
                'region': data.get('regionName', ''),
                'country_name': data.get('countryName', ''),
                'country_code': data.get('countryCode', ''),
                'source': 'freeipapi.com'
            }
    except Exception as e:
        pass
    return None

def geolocate_ip(ip_address):
    """Try multiple services until one works"""
    # Try services in order
    services = [
        geolocate_ip_ipapi,
        geolocate_ip_freeipapi,
        geolocate_ip_ipwhois,
        geolocate_ip_ipapi_co,
    ]
    
    for service in services:
        try:
            result = service(ip_address)
            if result:
                return result
        except:
            continue
    
    return None

def batch_geolocate_ips_parallel(ips, batch_size=100, max_workers=10):
    """Geolocate IPs in parallel using multiple services"""
    ip_to_location = {}
    processed = 0
    
    print(f"🌍 Starting PARALLEL geolocation of {len(ips)} IPs...")
    print(f"🚀 Using {max_workers} parallel workers across multiple free APIs")
    print(f"   - ip-api.com (45/min)")
    print(f"   - freeipapi.com (60/min)")
    print(f"   - ipwho.is (10000/month)")
    print(f"   - ipapi.co (1000/day)")
    print("")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_ip = {executor.submit(geolocate_ip, ip): ip for ip in ips}
        
        for future in as_completed(future_to_ip):
            ip = future_to_ip[future]
            processed += 1
            
            try:
                location = future.result()
                if location:
                    ip_to_location[ip] = location
                    print(f"  ✅ [{processed}/{len(ips)}] {ip}: {location['city']}, {location['region']}, {location['country_name']} (via {location['source']})")
                else:
                    print(f"  ⚠️ [{processed}/{len(ips)}] {ip}: Failed")
            except Exception as e:
                print(f"  ❌ [{processed}/{len(ips)}] {ip}: Error - {e}")
            
            # Update BigQuery every batch_size IPs
            if len(ip_to_location) >= batch_size:
                update_bigquery_locations(ip_to_location)
                ip_to_location = {}
        
        # Update remaining
        if ip_to_location:
            update_bigquery_locations(ip_to_location)
    
    print(f"🎉 Geolocation complete!")

def update_bigquery_locations(ip_to_location):
    """Update BigQuery with geolocated data"""
    if not ip_to_location:
        return
    
    print(f"  💾 Updating BigQuery with {len(ip_to_location)} geolocated IPs...")
    
    # Prepare data for temp table
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
    import os
    
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
        load_job = bq_client.load_table_from_file(source_file, temp_table_id, job_config=job_config)
        load_job.result()
    
    os.unlink(temp_file)
    
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
    
    print(f"  ✅ Updated {len(ip_to_location)} users in BigQuery")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Enrich IP addresses with geolocation data')
    parser.add_argument('--force', action='store_true', 
                       help='Force re-geolocation of ALL IPs (even if they already have city data)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Number of parallel workers (default: 10)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for BigQuery updates (default: 100)')
    args = parser.parse_args()
    
    print("🚀 Starting FAST IP geolocation enrichment...", flush=True)
    print("📡 Using multiple free APIs in parallel for 10x+ speed", flush=True)
    print("")
    
    try:
        # Get IPs that need geolocation
        ips = get_ips_to_geolocate(force_update=args.force)
        
        if not ips:
            if args.force:
                print("⚠️ No IPs found in database!")
            else:
                print("✅ All IPs already geolocated!")
                print("   Use --force to re-geolocate all IPs")
            sys.exit(0)
        
        # Estimate time with parallelization
        estimated_minutes = len(ips) / 60  # ~60 IPs per minute with 10 workers
        print(f"⏱️  Estimated time: ~{estimated_minutes:.1f} minutes (10x faster!)")
        print("")
        
        # Geolocate and update in parallel
        batch_geolocate_ips_parallel(ips, batch_size=args.batch_size, max_workers=args.max_workers)
        
        # Show final stats
        query = f"""
        SELECT 
            COUNT(*) as total_users,
            COUNT(last_login_city) as users_with_city,
            COUNT(DISTINCT last_login_city) as unique_cities,
            COUNT(DISTINCT last_login_country_name) as unique_countries
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        """
        result = bq_client.query(query).result()
        for row in result:
            print(f"\n📊 Final Statistics:")
            print(f"   Total users: {row.total_users}")
            print(f"   Users with city data: {row.users_with_city}")
            print(f"   Unique cities: {row.unique_cities}")
            print(f"   Unique countries: {row.unique_countries}")
        
        print("\n✅ IP geolocation enrichment completed successfully!", flush=True)
        
    except Exception as e:
        print(f"❌ Error: {e}", flush=True)
        import traceback
        traceback.print_exc()
        sys.exit(1)

