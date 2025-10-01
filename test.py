import os
import uuid
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from email.utils import parsedate_to_datetime
import pytz
import boto3
import psycopg2
from io import StringIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment configs
load_dotenv()

# === Configs from .env ===
NSE_RSS_URL = os.getenv("NSE_RSS_URL")
NSE_HOME_URL = os.getenv("NSE_HOME_URL")
BSE_RSS_URL = os.getenv("BSE_RSS_URL")
BSE_HOME_URL = os.getenv("BSE_HOME_URL")
SECURITY_MASTER_API = os.getenv("SECURITY_MASTER_API")
S3_BUCKET = os.getenv("S3_BUCKET")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "120"))


class BaseRssIngestor:
    """Base class with common logic for NSE & BSE ingestion"""

    def __init__(self, feed_url, home_url, source_name):
        self.feed_url = feed_url
        self.home_url = home_url
        self.source_name = source_name
        self.security_master_url = SECURITY_MASTER_API
        self.poll_interval = POLL_INTERVAL
        self.ist = pytz.timezone("Asia/Kolkata")

        # AWS S3 Client
        self.s3 = boto3.client("s3")
        self.bucket = S3_BUCKET

        # PostgreSQL
        self.conn = psycopg2.connect(
            dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
            host=PG_HOST, port=PG_PORT
        )

        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        # Logging
        logging.basicConfig(
            filename=f"{self.source_name}_ingestor.log",
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s"
        )

        # HTTP session with retry
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        retry_strategy = Retry(total=3, backoff_factor=1,
                               status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    def authenticate(self):
        try:
            self.session.get(self.home_url, timeout=5)
        except Exception:
            logging.error("Authentication error", exc_info=True)

    def get_security_info(self, company_name: str) -> dict:
        """Fetch Security ID + Market IDs from Security Master"""
        try:
            resp = self.session.get(self.security_master_url,
                                    params={"company": company_name}, timeout=5)
            if resp.status_code == 200:
                return resp.json()  # { "security_id": "123", "market_ids": [1,2] }
        except Exception:
            logging.error(f"Error fetching Security Info for {company_name}", exc_info=True)
        return {"security_id": "0", "market_ids": []}

    def upload_attachment_to_s3(self, link, security_id, uid, company_name=None):
        """Upload attachments into S3 and return S3 location + status"""
        # Fallback folder name if security_id is missing
        folder_name = security_id if security_id != "0" else (company_name.replace(" ", "_") if company_name else "Unknown")
        folder_prefix = f"Company Data/{folder_name}/RSS_Feeds/"

        if link and link.endswith(".pdf"):
            try:
                res = self.session.get(link, timeout=10)
                if res.status_code == 200:
                    key = f"{folder_prefix}{uid}.pdf"
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=res.content)
                    return f"s3://{self.bucket}/{key}", "Completed"
            except Exception:
                logging.error("PDF upload error", exc_info=True)
                return "", "Error"
        else:
            try:
                key = f"{folder_prefix}{uid}.txt"
                self.s3.put_object(Bucket=self.bucket, Key=key,
                                   Body="No PDF available.".encode("utf-8"))
                return f"s3://{self.bucket}/{key}", "Completed"
            except Exception:
                logging.error("TXT upload error", exc_info=True)
                return "", "Error"

    def insert_metadata_pg(self, row):
        """Insert metadata into PostgreSQL"""
        try:
            self.cur.execute(
                """
                INSERT INTO rss_announcements
                (security_id, source, title, subject, attachment, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    int(row["security_id"]) if row["security_id"].isdigit() else None,
                    f"{self.source_name}_RSS_Feed",
                    row["title"], row["subject"], row["attachment"], row["status"]
                )
            )
        except Exception:
            logging.error("Error inserting into PostgreSQL", exc_info=True)

    def fetch_and_process(self):
        raise NotImplementedError("Must be implemented by child classes")


class NseRssIngestor(BaseRssIngestor):
    def __init__(self):
        super().__init__(NSE_RSS_URL, NSE_HOME_URL, "NSE")

    def fetch_and_process(self):
        self.authenticate()
        try:
            res = self.session.get(self.feed_url, timeout=10)
            res.raise_for_status()
            soup = BeautifulSoup(res.content, "xml")
            items = soup.find_all("item")

            today_date = datetime.now(self.ist).strftime("%d%m%Y")
            raw_key = f"Raw data/NSE_RSS_Feed/NSERawData_{today_date}.csv"

            df_existing = pd.DataFrame()
            try:
                csv_obj = self.s3.get_object(Bucket=self.bucket, Key=raw_key)
                df_existing = pd.read_csv(csv_obj["Body"])
            except self.s3.exceptions.NoSuchKey:
                pass

            existing_links = set(df_existing["link"].values) if not df_existing.empty else set()
            new_rows = []

            for item in items:
                link = item.link.text if item.link else "NA"
                if link in existing_links:
                    continue

                uid = str(uuid.uuid4())
                pub_date = parsedate_to_datetime(item.pubDate.text).astimezone(self.ist) if item.pubDate else None
                company_name = item.title.text.split("-")[0].strip() if item.title else "Unknown"

                # Security Master check
                sec_info = self.get_security_info(company_name)
                security_id = sec_info.get("security_id", "0")
                market_ids = sec_info.get("market_ids", [])

                # ✅ Only ingest NSE if Market ID 1 is present
                if 1 not in market_ids:
                    continue

                # Upload attachment with fallback
                s3_location, status = self.upload_attachment_to_s3(link, security_id, uid, company_name=company_name)

                new_row = {
                    "id": uid,
                    "security_id": security_id,
                    "title": item.title.text if item.title else "",
                    "subject": item.description.text if item.description else "",
                    "attachment": s3_location,
                    "status": status,
                    "link": link,
                    "date": pub_date.strftime("%Y-%m-%d %H:%M:%S") if pub_date else ""
                }
                new_rows.append(new_row)
                self.insert_metadata_pg(new_row)

            if new_rows:
                df_existing = pd.concat([df_existing, pd.DataFrame(new_rows)], ignore_index=True)
                csv_buffer = StringIO()
                df_existing.to_csv(csv_buffer, index=False)
                self.s3.put_object(Bucket=self.bucket, Key=raw_key, Body=csv_buffer.getvalue())
                logging.info(f"NSE feed updated with {len(new_rows)} new records.")

        except Exception:
            logging.error("Error fetching NSE feed", exc_info=True)


class BseRssIngestor(BaseRssIngestor):
    def __init__(self):
        super().__init__(BSE_RSS_URL, BSE_HOME_URL, "BSE")

    def fetch_and_process(self):
        self.authenticate()
        try:
            res = self.session.get(self.feed_url, timeout=10)
            res.raise_for_status()
            xml_text = res.content.decode("utf-8", errors="ignore")
            root = BeautifulSoup(xml_text, "xml")
            items = root.find_all("item")

            today_date = datetime.now(self.ist).strftime("%d%m%Y")
            raw_key = f"Raw data/BSE_RSS_Feed/BSERawData_{today_date}.csv"

            df_existing = pd.DataFrame()
            try:
                csv_obj = self.s3.get_object(Bucket=self.bucket, Key=raw_key)
                df_existing = pd.read_csv(csv_obj["Body"])
            except self.s3.exceptions.NoSuchKey:
                pass

            existing_links = set(df_existing["link"].values) if not df_existing.empty else set()
            new_rows = []

            for item in items:
                title = (item.title.text if item.title else "").strip()
                link = (item.link.text if item.link else "").strip()
                pub_date_str = (item.pubDate.text if item.pubDate else "").strip()
                description = (item.description.text if item.description else "").strip() or title

                pub_date = ""
                if pub_date_str:
                    try:
                        pub_date = parsedate_to_datetime(pub_date_str).astimezone(self.ist)
                        pub_date = pub_date.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        pub_date = pub_date_str

                company_name = "Unknown"
                if "of " in title and " on " in title:
                    company_name = title.split("of ")[1].split(" on ")[0].strip()

                uid = str(uuid.uuid4())

                # Security Master check
                sec_info = self.get_security_info(company_name)
                security_id = sec_info.get("security_id", "0")
                market_ids = sec_info.get("market_ids", [])

                # ✅ Only ingest BSE if Market ID 2 exists AND Market ID 1 does NOT
                if 2 not in market_ids or 1 in market_ids:
                    continue

                # Upload attachment with fallback
                s3_location, status = self.upload_attachment_to_s3(link, security_id, uid, company_name=company_name)

                new_row = {
                    "id": uid,
                    "security_id": security_id,
                    "title": title,
                    "subject": description,
                    "attachment": s3_location,
                    "status": status,
                    "link": link,
                    "date": pub_date
                }
                new_rows.append(new_row)
                self.insert_metadata_pg(new_row)

            if new_rows:
                df_existing = pd.concat([df_existing, pd.DataFrame(new_rows)], ignore_index=True)
                csv_buffer = StringIO()
                df_existing.to_csv(csv_buffer, index=False)
                self.s3.put_object(Bucket=self.bucket, Key=raw_key, Body=csv_buffer.getvalue())
                logging.info(f"BSE feed updated with {len(new_rows)} new records.")

        except Exception:
            logging.error("Error fetching BSE feed", exc_info=True)


if __name__ == "__main__":
    bse_ingestor = BseRssIngestor()
    nse_ingestor = NseRssIngestor()
    

    nse_ingestor.fetch_and_process()
    bse_ingestor.fetch_and_process()