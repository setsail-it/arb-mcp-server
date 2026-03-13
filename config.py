import os
import base64
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# --- Database ---
DATABASE_URL = os.getenv("DATABASE_URL")

# --- Google Gemini API ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent"

# --- AWS S3 ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

s3_client = None
if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
    import boto3
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

# --- DataForSEO ---
DEFAULT_LOCATION_CODE = 2840
DATAFORSEO_LABS_BASE = "https://api.dataforseo.com/v3/dataforseo_labs/google"
DATAFORSEO_KEYWORDS_BASE = "https://api.dataforseo.com/v3/keywords_data/google_ads"

# --- Webflow ---
WEBFLOW_ACCESS_TOKEN = os.getenv("WEBFLOW_ACCESS_TOKEN")
WEBFLOW_API_BASE = "https://api.webflow.com/v2"

# --- Caps / magic numbers ---
MAX_KEYWORDS_FOR_SITE_RETURN = 50
MAX_KEYWORDS_FOR_KEYWORDS_RETURN = 100
MAX_OPPORTUNITIES_COMPETITORS = 3
MAX_OPPORTUNITIES_KEYWORDS_PER_COMPETITOR = 250
MIN_OPPORTUNITIES_VOLUME = 10


# --- DataForSEO auth helpers ---
def _dataforseo_basic_auth() -> str:
    """DataForSEO Basic auth: USERNAME + API_SECRET, or API_KEY + API_SECRET, or API_KEY as raw Base64."""
    username = os.getenv("DATAFORSEO_USERNAME") or os.getenv("DATAFORSEO_API_KEY")
    secret = os.getenv("DATAFORSEO_API_SECRET")
    if username and secret:
        return base64.b64encode(f"{username}:{secret}".encode()).decode()
    if username:
        return username  # assume already Base64
    return ""


def _normalize_target_domain(site_url: str) -> str:
    """Extract target domain for DataForSEO (no scheme, no www). E.g. setsail.ca."""
    p = urlparse(site_url)
    netloc = (p.netloc or "").strip().lower()
    if not netloc:
        return ""
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return netloc
