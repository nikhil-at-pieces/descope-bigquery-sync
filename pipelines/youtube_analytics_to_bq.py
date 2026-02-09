import os
import json
import datetime as dt
from typing import List, Dict

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.environ.get("GCP_PROJECT", "")
DATASET_ID = os.environ.get("BQ_DATASET_ANALYTICS", "")
TABLE_DAILY = f"{PROJECT_ID}.{DATASET_ID}.youtube_video_daily"
TABLE_PLAYBACK = f"{PROJECT_ID}.{DATASET_ID}.youtube_video_playback_daily"
TABLE_TRAFFIC = f"{PROJECT_ID}.{DATASET_ID}.youtube_video_traffic_daily"

# OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

CLIENT_SECRETS_FILE = os.environ.get("YOUTUBE_CLIENT_SECRETS", "client_secret.json")
TOKEN_FILE = os.environ.get("YOUTUBE_TOKEN_FILE", ".yt_token.json")
if not PROJECT_ID or not DATASET_ID:
    raise SystemExit("Set GCP_PROJECT and BQ_DATASET_ANALYTICS in .env (see .env.example)")


def get_credentials() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=8081, prompt='consent', authorization_prompt_message='')
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


def ensure_tables():
    client = bigquery.Client(project=PROJECT_ID)
    # youtube_video_daily
    schemas = {
        TABLE_DAILY: [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("views", "INTEGER"),
            bigquery.SchemaField("likes", "INTEGER"),
            bigquery.SchemaField("comments", "INTEGER"),
            bigquery.SchemaField("estimated_minutes_watched", "INTEGER"),
            bigquery.SchemaField("average_view_duration_seconds", "INTEGER"),
        ],
        TABLE_PLAYBACK: [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("playback_location_type", "STRING"),
            bigquery.SchemaField("views", "INTEGER"),
        ],
        TABLE_TRAFFIC: [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("video_id", "STRING"),
            bigquery.SchemaField("traffic_source_type", "STRING"),
            bigquery.SchemaField("views", "INTEGER"),
        ],
    }
    for table_id, schema in schemas.items():
        try:
            client.get_table(table_id)
        except Exception:
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)


def query_report(yta, start_date: str, end_date: str, metrics: str, dimensions: str, max_results: int = 20000):
    # Channel mine scope
    return yta.reports().query(
        ids="channel==MINE",
        startDate=start_date,
        endDate=end_date,
        metrics=metrics,
        dimensions=dimensions,
        sort=dimensions,
        maxResults=max_results,
    ).execute()


def to_date_string(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def ingest_daily(yta, bq: bigquery.Client, start_date: str, end_date: str):
    resp = query_report(
        yta,
        start_date,
        end_date,
        metrics="views,likes,comments,estimatedMinutesWatched,averageViewDuration",
        dimensions="day,video",
    )
    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", [])
    out: List[Dict] = []
    for r in rows:
        row = dict(zip(headers, r))
        out.append({
            "date": row.get("day"),
            "video_id": row.get("video"),
            "views": int(row.get("views", 0)),
            "likes": int(row.get("likes", 0)),
            "comments": int(row.get("comments", 0)),
            "estimated_minutes_watched": int(row.get("estimatedMinutesWatched", 0)),
            "average_view_duration_seconds": int(row.get("averageViewDuration", 0)),
        })
    if out:
        bq.insert_rows_json(TABLE_DAILY, out)
        print(f"Inserted {len(out)} rows into {TABLE_DAILY}")
    else:
        print("No daily rows returned.")


def try_ingest_playback(yta, bq: bigquery.Client, start_date: str, end_date: str):
    try:
        resp = query_report(
            yta,
            start_date,
            end_date,
            metrics="views",
            dimensions="day,video,playbackLocationType",
        )
        headers = [h["name"] for h in resp.get("columnHeaders", [])]
        rows = resp.get("rows", [])
        out: List[Dict] = []
        for r in rows:
            row = dict(zip(headers, r))
            out.append({
                "date": row.get("day"),
                "video_id": row.get("video"),
                "playback_location_type": str(row.get("playbackLocationType")),
                "views": int(row.get("views", 0)),
            })
        if out:
            bq.insert_rows_json(TABLE_PLAYBACK, out)
            print(f"Inserted {len(out)} rows into {TABLE_PLAYBACK}")
        else:
            print("No playback location rows returned.")
    except HttpError as e:
        print(f"Skipping playback breakdown (not supported on this channel/account): {e}")


def try_ingest_traffic(yta, bq: bigquery.Client, start_date: str, end_date: str):
    try:
        resp = query_report(
            yta,
            start_date,
            end_date,
            metrics="views",
            dimensions="day,video,insightTrafficSourceType",
        )
        headers = [h["name"] for h in resp.get("columnHeaders", [])]
        rows = resp.get("rows", [])
        out: List[Dict] = []
        for r in rows:
            row = dict(zip(headers, r))
            out.append({
                "date": row.get("day"),
                "video_id": row.get("video"),
                "traffic_source_type": str(row.get("insightTrafficSourceType")),
                "views": int(row.get("views", 0)),
            })
        if out:
            bq.insert_rows_json(TABLE_TRAFFIC, out)
            print(f"Inserted {len(out)} rows into {TABLE_TRAFFIC}")
        else:
            print("No traffic source rows returned.")
    except HttpError as e:
        print(f"Skipping traffic breakdown (not supported on this channel/account): {e}")


def main():
    creds = get_credentials()
    yta = build("youtubeAnalytics", "v2", credentials=creds)
    bq = bigquery.Client(project=PROJECT_ID)

    ensure_tables()

    end = dt.date.today() - dt.timedelta(days=1)
    start = end - dt.timedelta(days=365)
    start_s = to_date_string(start)
    end_s = to_date_string(end)

    ingest_daily(yta, bq, start_s, end_s)
    try_ingest_playback(yta, bq, start_s, end_s)
    try_ingest_traffic(yta, bq, start_s, end_s)


if __name__ == "__main__":
    main()
