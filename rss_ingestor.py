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
from botocore.exceptions import ClientError
from typing import Optional
import re
try:
    from weasyprint import HTML
    WEASYPRINT_AVAILABLE = True
except Exception:
    WEASYPRINT_AVAILABLE = False

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

        # Per-instance logger so NSE and BSE logs go to separate files
        self.logger = logging.getLogger(self.source_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self.source_name}_ingestor.log")
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            fh.setFormatter(fmt)
            self.logger.addHandler(fh)

        # AWS S3 Client (use explicit creds from .env if provided, otherwise let boto3 resolve)
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if aws_key and aws_secret:
            client_kwargs = {
                "aws_access_key_id": aws_key,
                "aws_secret_access_key": aws_secret,
            }
            if aws_region:
                client_kwargs["region_name"] = aws_region
            self.s3 = boto3.client("s3", **client_kwargs)
        else:
            self.s3 = boto3.client("s3")
        self.bucket = S3_BUCKET

        # PostgreSQL (optional) - don't crash if DB is unavailable
        self.conn = None
        self.cur = None
        try:
            self.conn = psycopg2.connect(
                dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
                host=PG_HOST, port=PG_PORT
            )
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
        except Exception:
            # Log and continue without DB
            self.logger.warning("PostgreSQL not available; continuing without DB", exc_info=True)

        # If DB connected, verify required table exists; optionally create it
        self.db_enabled = bool(self.cur)
        self._suppress_db_errors = False
        if self.db_enabled:
            try:
                # Use information_schema to check for table existence
                self.cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='rss_announcements')")
                exists = self.cur.fetchone()[0]
                if not exists:
                    # Optionally create the table if environment variable set
                    if os.getenv("CREATE_RSS_TABLE", "false").lower() in ("1", "true", "yes"):
                        self.logger.info("Creating missing table rss_announcements as requested by CREATE_RSS_TABLE")
                        self.cur.execute(
                            """
                            CREATE TABLE IF NOT EXISTS rss_announcements (
                                id serial PRIMARY KEY,
                                security_id bigint NULL,
                                source text,
                                title text,
                                subject text,
                                attachment text,
                                status text,
                                created_at timestamptz DEFAULT now()
                            )
                            """
                        )
                        self.conn.commit()
                    else:
                        self.logger.warning("Postgres table 'rss_announcements' not found; DB inserts will be disabled. Set CREATE_RSS_TABLE=true to auto-create.")
                        self.db_enabled = False
            except Exception:
                # If any error checking table, disable DB to avoid noisy logs
                self.logger.warning("Error while checking/creating rss_announcements table; disabling DB inserts", exc_info=True)
                self.db_enabled = False

        # Per-instance logger so NSE and BSE logs go to separate files
        self.logger = logging.getLogger(self.source_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            fh = logging.FileHandler(f"{self.source_name}_ingestor.log")
            fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            fh.setFormatter(fmt)
            self.logger.addHandler(fh)

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
            self.logger.error("Authentication error", exc_info=True)

    def parse_pub_date(self, date_str: str) -> Optional[datetime]:
        """Try multiple ways to parse pubDate into an aware datetime in IST.

        Returns None if parsing fails.
        """
        if not date_str:
            return None
        # 1) Try RFC parsedate
        try:
            dt = parsedate_to_datetime(date_str)
            if dt is None:
                raise ValueError("parsedate returned None")
            if dt.tzinfo is None:
                dt = self.ist.localize(dt)
            else:
                dt = dt.astimezone(self.ist)
            return dt
        except Exception:
            # 2) Try common formats
            fmts = ["%d-%b-%Y %H:%M:%S", "%d %b %Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S"]
            for f in fmts:
                try:
                    dt = datetime.strptime(date_str, f)
                    dt = self.ist.localize(dt)
                    return dt
                except Exception:
                    continue
        # Could not parse
        return None

    def get_security_info(self, company_name: str) -> dict:
        # Security Master API is not used in this mode. Return default placeholders.
        return {"security_id": "0", "market_ids": []}

    def upload_attachment_to_s3(self, link, security_id, uid):
        # Deprecated in this mode. Keep for backward compatibility but not used.
        folder_prefix = f"Company Data/{security_id}/RSS_Feeds/"
        try:
            if link and link.endswith(".pdf"):
                res = self.session.get(link, timeout=10)
                if res.status_code == 200:
                    key = f"{folder_prefix}{uid}.pdf"
                    self.s3.put_object(Bucket=self.bucket, Key=key, Body=res.content)
                    return f"s3://{self.bucket}/{key}", "Completed"
            key = f"{folder_prefix}{uid}.txt"
            self.s3.put_object(Bucket=self.bucket, Key=key, Body="No PDF available.".encode("utf-8"))
            return f"s3://{self.bucket}/{key}", "Completed"
        except Exception:
            logging.error("Attachment upload error", exc_info=True)
            return "", "Error"

    def upload_pdf_or_text_to_s3(self, link: str, description: str, company_name: str, uid: str):
        """Download and upload the PDF if available; otherwise upload a text file with the description.

        Returns (s3_path, status).
        """
        folder_name = company_name.replace(" ", "_") if company_name else "Unknown"
        folder_prefix = f"Company Data/{folder_name}/RSS_Feeds/"

        # Helper to upload bytes to S3 under given extension
        def _upload_bytes(body_bytes: bytes, ext: str, content_type: Optional[str] = None):
            key = f"{folder_prefix}{uid}.{ext}"
            extra = {}
            if content_type:
                extra['ContentType'] = content_type
            try:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=body_bytes, **extra)
                self.logger.info("Uploaded attachment to s3://%s/%s", self.bucket, key)
                return f"s3://{self.bucket}/{key}", "Completed"
            except Exception:
                self.logger.error("Error uploading attachment to S3", exc_info=True)
                return "", "Error"

        # If no link provided, upload description as text
        if not link:
            return _upload_bytes(description.encode("utf-8"), "txt", content_type="text/plain")

        # If the link looks like a direct PDF, try to download it
        try:
            # First a quick heuristic by file name
            if link.lower().endswith('.pdf'):
                r = self.session.get(link, timeout=15)
                if r.status_code == 200 and r.content:
                    # Accept if headers say pdf or the URL endswith .pdf
                    ctype = r.headers.get('Content-Type', '')
                    return _upload_bytes(r.content, 'pdf', content_type=ctype or 'application/pdf')

            # Otherwise, try a GET and inspect headers/body for a PDF link or PDF content
            r = self.session.get(link, timeout=15)
            # If we didn't get a good response, fall back to text
            if r.status_code != 200:
                self.logger.warning("Attachment link returned non-200 status %s: %s", r.status_code, link)
                return _upload_bytes((description or '').encode('utf-8'), 'txt', content_type='text/plain')

            ctype = r.headers.get('Content-Type', '').lower()
            body = r.content
            # If the response itself is a PDF, upload it
            if 'application/pdf' in ctype or link.lower().endswith('.pdf'):
                return _upload_bytes(body, 'pdf', content_type=ctype or 'application/pdf')

            # If the response looks like XML/HTML, try to parse and find an embedded PDF link
            text = None
            try:
                text = r.content.decode('utf-8', errors='ignore')
            except Exception:
                text = None

            if text:
                # Look for any href or url that ends with .pdf
                # Quick regex to find urls
                pdf_urls = re.findall(r"https?://[^\s'\"<>]+\.pdf", text, flags=re.IGNORECASE)
                if pdf_urls:
                    pdf_url = pdf_urls[0]
                    self.logger.info("Discovered PDF inside XML/HTML link: %s", pdf_url)
                    try:
                        r2 = self.session.get(pdf_url, timeout=15)
                        if r2.status_code == 200 and r2.content:
                            ctype2 = r2.headers.get('Content-Type', '')
                            return _upload_bytes(r2.content, 'pdf', content_type=ctype2 or 'application/pdf')
                        else:
                            self.logger.warning("Discovered PDF URL returned non-200: %s %s", r2.status_code, pdf_url)
                    except Exception:
                        self.logger.warning("Error downloading discovered PDF URL", exc_info=True)

            # No PDF found; upload description text as fallback
            return _upload_bytes((description or '').encode('utf-8'), 'txt', content_type='text/plain')

        except Exception:
            self.logger.warning("Failed to fetch attachment link; uploading description as text", exc_info=True)
            return _upload_bytes((description or '').encode('utf-8'), 'txt', content_type='text/plain')

    def upload_bytes_to_s3(self, body_bytes: bytes, company_name: str, uid: str, ext: str = 'pdf', content_type: Optional[str] = None):
        """Upload raw bytes to S3 under company folder and return (s3_uri, status)."""
        folder_name = company_name.replace(" ", "_") if company_name else "Unknown"
        key = f"Company Data/{folder_name}/RSS_Feeds/{uid}.{ext}"
        extra = {}
        if content_type:
            extra['ContentType'] = content_type
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body_bytes, **extra)
            self.logger.info("Uploaded attachment to s3://%s/%s", self.bucket, key)
            return f"s3://{self.bucket}/{key}", "Completed"
        except Exception:
            self.logger.error("Error uploading bytes to S3", exc_info=True)
            return "", "Error"

    def fetch_and_render_page_to_pdf(self, link: str) -> Optional[bytes]:
        """Fetch the HTML page at link and render it to PDF bytes using WeasyPrint.

        Returns PDF bytes on success, or None on failure or if weasyprint not available.
        """
        if not link:
            return None
        if not WEASYPRINT_AVAILABLE:
            self.logger.debug("WeasyPrint not available; skipping HTML->PDF rendering for %s", link)
            return None

        try:
            r = self.session.get(link, timeout=15)
            if r.status_code != 200:
                self.logger.warning("Failed to fetch page for rendering (status %s): %s", r.status_code, link)
                return None
            # Use the response text as HTML source and use the link as base_url for relative resources
            html_text = r.content.decode('utf-8', errors='ignore')
            pdf_bytes = HTML(string=html_text, base_url=link).write_pdf()
            return pdf_bytes
        except Exception:
            self.logger.exception("Error rendering HTML to PDF for link: %s", link)
            return None

    # Note: internal XML uploads removed. Only PDF (.pdf) or text (.txt) attachments are uploaded to company folders.

    def insert_metadata_pg(self, row):
        """Insert metadata into PostgreSQL"""
        if not self.db_enabled or not self.cur:
            # DB disabled; skip insert
            return

        if self._suppress_db_errors:
            # If we've decided to suppress DB errors, don't attempt insert
            return

        try:
            self.cur.execute(
                """
                INSERT INTO rss_announcements
                (security_id, source, title, subject, attachment, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    int(str(row.get("security_id", "")).strip()) if str(row.get("security_id", "")).strip().isdigit() else None,
                    f"{self.source_name}_RSS_Feed",
                    row.get("title"), row.get("subject"), row.get("attachment"), row.get("status")
                )
            )
        except Exception as e:
            # If table missing or other persistent error, log once and suppress further DB attempts
            self.logger.error("Error inserting into PostgreSQL (disabling further DB inserts): %s", e)
            self.logger.debug("Row that failed: %s", row)
            self._suppress_db_errors = True

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
            except ClientError as e:
                err_code = e.response.get("Error", {}).get("Code")
                if err_code in ("NoSuchKey", "404"):
                    df_existing = pd.DataFrame()
                else:
                    self.logger.error("S3 ClientError while reading existing CSV", exc_info=True)
                    df_existing = pd.DataFrame()

            existing_links = set(df_existing.get("link", pd.Series(dtype=str)).values) if not df_existing.empty else set()
            new_rows = []

            for item in items:
                link = item.link.text if item.link else "NA"
                if link in existing_links:
                    continue

                uid = str(uuid.uuid4())
                pub_date = None
                if item.pubDate:
                    pub_date = self.parse_pub_date(item.pubDate.text)
                
                company_name = item.title.text.split("-")[0].strip() if item.title else "Unknown"

                # Security Master is not used: ingest all items
                try:
                    item_xml = str(item)
                except Exception:
                    item_xml = ""

                # Upload PDF if available, otherwise upload a .txt containing the description
                attachment_s3, status = self.upload_pdf_or_text_to_s3(link if link else None,
                                                                     item.description.text if item.description else "",
                                                                     company_name, uid)

                new_row = {
                    "id": uid,
                    "security_id": "0",
                    "title": item.title.text if item.title else "",
                    "subject": item.description.text if item.description else "",
                    "attachment": attachment_s3,
                    "status": status,
                    # Keep original link column pointing to the original feed link (may be xml or pdf)
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
                self.logger.info(f"NSE feed updated with {len(new_rows)} new records.")

        except Exception:
            logging.error("Error fetching NSE feed", exc_info=True)


class BseRssIngestor(BaseRssIngestor):
    def __init__(self):
        super().__init__(BSE_RSS_URL, BSE_HOME_URL, "BSE")

    def extract_company_name_from_title(self, title: str) -> str:
        """Extract company name from BSE title.

        Returns extracted company name or 'Unknown' if extraction fails.
        """
        limited_keywords = ["Ltd", "LTD", "Limited", "LIMITED"]
        split_keywords = [" of ", " for ", " in ", " from ", " by ", " to "]

        if not title:
            return "Unknown"

        text = title.strip()
        company_part = None

        for key in split_keywords:
            if key in text:
                # Take part after the last occurrence of the split keyword
                company_part = text.rsplit(key, 1)[-1].strip()
                break

        if not company_part:
            return "Unknown"

        # Build regex to find a company ending with Limited/Ltd etc.
        pattern = r"(.+?\b(?:{}))\b".format("|".join(limited_keywords))
        match = re.search(pattern, company_part)
        if not match:
            return "Unknown"

        return match.group(1).strip()

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
            except ClientError as e:
                err_code = e.response.get("Error", {}).get("Code")
                if err_code in ("NoSuchKey", "404"):
                    df_existing = pd.DataFrame()
                else:
                    self.logger.error("S3 ClientError while reading existing CSV (BSE)", exc_info=True)
                    df_existing = pd.DataFrame()

            existing_links = set(df_existing.get("link", pd.Series(dtype=str)).values) if not df_existing.empty else set()
            new_rows = []

            for item in items:
                title = (item.title.text if item.title else "").strip()
                link = (item.link.text if item.link else "").strip()
                pub_date_str = (item.pubDate.text if item.pubDate else "").strip()
                description = (item.description.text if item.description else "").strip() or title

                pub_date = ""
                if pub_date_str:
                    dt = self.parse_pub_date(pub_date_str)
                    if dt:
                        pub_date = dt.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        pub_date = pub_date_str

                # Extract company name from title using dedicated extractor
                company_name = self.extract_company_name_from_title(title)

                # If extraction failed, skip this item entirely (do not upload or add to CSV)
                if not company_name or company_name == "Unknown":
                    self.logger.info("Skipping BSE item because company extraction failed: %s", title)
                    continue

                uid = str(uuid.uuid4())

                # Security Master not used; ingest all BSE items
                try:
                    item_xml = str(item)
                except Exception:
                    item_xml = ""

                # BSE: Try to render the linked page to PDF (WeasyPrint) first
                pdf_bytes = self.fetch_and_render_page_to_pdf(link)
                if pdf_bytes:
                    attachment_s3, status = self.upload_bytes_to_s3(pdf_bytes, company_name, uid, ext='pdf', content_type='application/pdf')
                else:
                    # If rendering failed, fall back to the previous logic (direct PDF download or find embedded pdf in page), else text
                    attachment_s3, status = self.upload_pdf_or_text_to_s3(link if link else None, description, company_name, uid)

                new_row = {
                    "id": uid,
                    "security_id": "0",
                    "title": title,
                    "subject": description,
                    "attachment": attachment_s3,
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
                self.logger.info(f"BSE feed updated with {len(new_rows)} new records.")

        except Exception:
            self.logger.error("Error fetching BSE feed", exc_info=True)


if __name__ == "__main__":
    nse_ingestor = NseRssIngestor()
    bse_ingestor = BseRssIngestor()

    nse_ingestor.fetch_and_process()
    bse_ingestor.fetch_and_process()
