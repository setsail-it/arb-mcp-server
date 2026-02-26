import logging
import os
import base64
import json
import time
import secrets
import requests
from typing import Optional
from urllib.parse import urlparse
from fastmcp import FastMCP

logger = logging.getLogger(__name__)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

mcp = FastMCP("Keyword MCP Server")

# Database setup - create engine once
_database_url = os.getenv("DATABASE_URL")
_engine = None
_SessionLocal = None

if _database_url:
    _engine = create_engine(_database_url)
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

# Google Gemini API setup
_google_api_key = os.getenv("GOOGLE_API_KEY")
_google_api_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent"

# AWS S3 setup
_aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
_aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
_aws_region = os.getenv("AWS_REGION", "us-east-2")
_aws_s3_bucket = os.getenv("AWS_S3_BUCKET", "arb-imgs")
_s3_client = None

if _aws_access_key_id and _aws_secret_access_key:
    import boto3
    _s3_client = boto3.client(
        's3',
        aws_access_key_id=_aws_access_key_id,
        aws_secret_access_key=_aws_secret_access_key,
        region_name=_aws_region
    )

def get_db_session() -> Session:
    """Get a database session."""
    if not _SessionLocal:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return _SessionLocal()


# --- DataForSEO shared auth and helpers (Labs + Keywords Data; sitemap/on-page moved to arb-v1) ---
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


@mcp.tool
def get_search_volume(keyword: str, location_code: int = 2840, language_code: str = "en") -> dict:
    """
    Fetches keyword overview data for a given keyword using DataForSEO API.

    Args:
        keyword (str): The keyword to fetch data for.
        location_code (int): The location code (default is 2840 for the United States).
        language_code (str): The language code (default is "en" for English).

    Returns:
        dict: A dictionary containing keyword data, including search volume, keyword difficulty, and main intent.
    """
    # DataForSEO API endpoint
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_overview/live"

    # Retrieve DataForSEO Base64 authorization key from environment variable
    api_key_base64 = os.getenv("DATAFORSEO_API_KEY")  # Base64 encoded "username:password"

    if not api_key_base64:
        raise ValueError("DATAFORSEO_API_KEY environment variable is not set.")

    # Prepare the payload
    payload = [
        {
            "language_code": language_code,
            "location_code": location_code,
            "include_clickstream_data": True,
            "include_serp_info": True,
            "keywords": [keyword]
        }
    ]

    # Make the POST request with Base64 Authorization header
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {api_key_base64}"
    }
    response = requests.post(url, headers=headers, json=payload)

    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"DataForSEO API request failed with status code {response.status_code}: {response.text}")

    # Parse the response
    data = response.json()

    # Extract relevant data from tasks[0].result[0].items[0]
    try:
        if 'tasks' not in data or not data['tasks']:
            raise Exception("No tasks in DataForSEO API response")
        
        task = data['tasks'][0]
        if 'result' not in task or not task['result']:
            status_code = task.get('status_code')
            status_message = task.get('status_message', 'Unknown error')
            raise Exception(f"DataForSEO API returned no result. Status: {status_code} - {status_message}")
        
        if not isinstance(task['result'], list) or not task['result']:
            raise Exception("DataForSEO API result is not a list or is empty")
        
        result = task['result'][0]
        if 'items' not in result or not result['items']:
            # Return a graceful response when no data is available
            return {
                "keyword": keyword,
                "search_volume": None,
                "keyword_difficulty": None,
                "main_intent": None,
                "error": "No data available for this keyword"
            }
        
        item = result['items'][0]
        
        # Extract search_volume from keyword_info
        keyword_info = item.get('keyword_info', {})
        search_volume = keyword_info.get('search_volume')
        
        # Extract keyword_difficulty from keyword_properties
        keyword_properties = item.get('keyword_properties', {})
        keyword_difficulty = keyword_properties.get('keyword_difficulty')
        
        # Extract main_intent from search_intent_info
        search_intent_info = item.get('search_intent_info', {})
        main_intent = search_intent_info.get('main_intent')
        
        keyword_data = {
            "keyword": item.get('keyword'),
            "search_volume": search_volume,
            "keyword_difficulty": keyword_difficulty,
            "main_intent": main_intent
        }
        return keyword_data
    except (KeyError, IndexError, TypeError) as e:
        raise Exception(f"Unexpected response structure from DataForSEO API: {e}. Response: {data}")


@mcp.tool
def getKeywordIdeas(
    keywords: list[str],
    location_code: int = 2840,
    language_code: str = "en",
    limit: int = 100,
    include_serp_info: bool = True,
    include_clickstream_data: bool = False,
    filters: Optional[list] = None,
    order_by: Optional[list] = None
) -> dict:
    """
    Fetches keyword ideas based on seed keywords using DataForSEO Keyword Ideas API.
    
    This endpoint provides keyword ideas based on the specified seed keywords.
    Results include search volume, competition, CPC, and related metrics.
    
    API Docs: https://docs.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live/
    
    Args:
        keywords (list[str]): Array of seed keywords (max 20 keywords).
        location_code (int): Location code (default 2840 for United States).
        language_code (str): Language code (default "en" for English).
        limit (int): Maximum number of keyword ideas to return (default 100, max 1000).
        include_serp_info (bool): Include SERP info in results (default True).
        include_clickstream_data (bool): Include clickstream data (default False).
        filters (list, optional): Array of filter conditions. Example:
            [["keyword_info.search_volume", ">", 100], "and", ["keyword_info.competition_level", "=", "LOW"]]
        order_by (list, optional): Array of order conditions. Example:
            ["keyword_info.search_volume,desc"]
    
    Returns:
        dict: Contains 'total_count', 'items_count', and 'items' array with keyword ideas.
              Each item includes keyword, keyword_info (search_volume, competition, cpc, etc.),
              keyword_properties (keyword_difficulty), and search_intent_info.
    
    Example filters:
        - Filter by search volume > 100: [["keyword_info.search_volume", ">", 100]]
        - Filter by low competition: [["keyword_info.competition_level", "=", "LOW"]]
        - Combined: [["keyword_info.search_volume", ">", 100], "and", ["keyword_info.competition_level", "=", "LOW"]]
    
    Example order_by:
        - Order by search volume descending: ["keyword_info.search_volume,desc"]
        - Order by CPC ascending: ["keyword_info.cpc,asc"]
    """
    # DataForSEO API endpoint for Keyword Ideas
    url = "https://api.dataforseo.com/v3/dataforseo_labs/google/keyword_ideas/live"
    
    # Retrieve DataForSEO Base64 authorization key from environment variable
    api_key_base64 = os.getenv("DATAFORSEO_API_KEY")
    
    if not api_key_base64:
        raise ValueError("DATAFORSEO_API_KEY environment variable is not set.")
    
    # Validate inputs
    if not keywords or len(keywords) == 0:
        raise ValueError("At least one seed keyword is required.")
    
    if len(keywords) > 20:
        raise ValueError("Maximum 20 seed keywords allowed.")
    
    if limit > 1000:
        limit = 1000  # Cap at API maximum
    
    # Prepare the payload
    payload_item = {
        "keywords": keywords,
        "location_code": location_code,
        "language_code": language_code,
        "limit": limit,
        "include_serp_info": include_serp_info,
        "include_clickstream_data": include_clickstream_data,
    }
    
    # Add optional filters if provided
    if filters:
        payload_item["filters"] = filters
    
    # Add optional order_by if provided
    if order_by:
        payload_item["order_by"] = order_by
    
    payload = [payload_item]
    
    # Make the POST request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {api_key_base64}"
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"DataForSEO API request failed with status code {response.status_code}: {response.text}")
    
    # Parse the response
    data = response.json()
    
    try:
        if 'tasks' not in data or not data['tasks']:
            raise Exception("No tasks in DataForSEO API response")
        
        task = data['tasks'][0]
        
        if task.get('status_code') != 20000:
            status_code = task.get('status_code')
            status_message = task.get('status_message', 'Unknown error')
            raise Exception(f"DataForSEO API error: {status_code} - {status_message}")
        
        if 'result' not in task or not task['result']:
            return {
                "total_count": 0,
                "items_count": 0,
                "items": [],
                "seed_keywords": keywords
            }
        
        result = task['result'][0]
        
        # Extract and format items
        items = result.get('items', [])
        formatted_items = []
        
        for item in items:
            keyword_info = item.get('keyword_info', {})
            keyword_properties = item.get('keyword_properties', {})
            search_intent_info = item.get('search_intent_info', {})
            serp_info = item.get('serp_info', {})
            
            formatted_item = {
                "keyword": item.get('keyword'),
                "search_volume": keyword_info.get('search_volume'),
                "competition": keyword_info.get('competition'),
                "competition_level": keyword_info.get('competition_level'),
                "cpc": keyword_info.get('cpc'),
                "low_top_of_page_bid": keyword_info.get('low_top_of_page_bid'),
                "high_top_of_page_bid": keyword_info.get('high_top_of_page_bid'),
                "keyword_difficulty": keyword_properties.get('keyword_difficulty'),
                "main_intent": search_intent_info.get('main_intent'),
                "monthly_searches": keyword_info.get('monthly_searches', [])[:6],  # Last 6 months
            }
            
            # Include SERP info if available
            if include_serp_info and serp_info:
                formatted_item["serp_item_types"] = serp_info.get('serp_item_types', [])
                formatted_item["se_results_count"] = serp_info.get('se_results_count')
            
            formatted_items.append(formatted_item)
        
        return {
            "total_count": result.get('total_count', 0),
            "items_count": result.get('items_count', 0),
            "items": formatted_items,
            "seed_keywords": keywords,
            "location_code": location_code,
            "language_code": language_code
        }
        
    except (KeyError, IndexError, TypeError) as e:
        raise Exception(f"Unexpected response structure from DataForSEO API: {e}. Response: {data}")


@mcp.tool
def google_ads_keyword_planner(
    keywords: list[str],
    location_code: int = 2840,
    language_code: str = "en",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None
) -> dict:
    """
    Fetches Google Ads keyword planner data including CPC, impressions, clicks, cost, and position data.
    
    This tool uses the DataForSEO Google Ads Ad Traffic By Keywords API to get comprehensive
    keyword metrics including CPC ranges, ad positions, impressions, clicks, and cost data.
    
    API Docs: https://docs.dataforseo.com/v3/keywords_data/google_ads/ad_traffic_by_keywords/
    
    Args:
        keywords (list[str]): Array of keywords to analyze (max 1000 keywords per request).
        location_code (int): Location code (default 2840 for United States).
        language_code (str): Language code (default "en" for English).
        date_from (str, optional): Start date in YYYY-MM-DD format. If not provided, uses default period.
        date_to (str, optional): End date in YYYY-MM-DD format. If not provided, uses default period.
    
    Returns:
        dict: Contains 'total_count', 'items_count', and 'items' array with keyword data.
              Each item includes:
              - keyword: The keyword text
              - search_volume: Monthly search volume
              - cpc: Average CPC (cost per click)
              - cpc_min: Minimum CPC
              - cpc_max: Maximum CPC
              - competition: Competition level (LOW, MEDIUM, HIGH)
              - competition_index: Competition index (0-1)
              - low_top_of_page_bid: Low top of page bid estimate
              - high_top_of_page_bid: High top of page bid estimate
              - ad_position: Average ad position (if available)
              - impressions: Impressions for the period
              - clicks: Clicks for the period
              - cost: Cost for the period (in micros, divide by 1,000,000 for dollars)
    
    Example:
        google_ads_keyword_planner(
            keywords=["digital marketing", "seo services"],
            location_code=2840,
            language_code="en"
        )
    """
    # DataForSEO API endpoint for Google Ads Ad Traffic By Keywords
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/ad_traffic_by_keywords/live"
    
    # Retrieve DataForSEO Base64 authorization key from environment variable
    api_key_base64 = os.getenv("DATAFORSEO_API_KEY")
    
    if not api_key_base64:
        raise ValueError("DATAFORSEO_API_KEY environment variable is not set.")
    
    # Validate inputs
    if not keywords or len(keywords) == 0:
        raise ValueError("At least one keyword is required.")
    
    if len(keywords) > 1000:
        raise ValueError("Maximum 1000 keywords allowed per request.")
    
    # Prepare the payload - ad_traffic_by_keywords endpoint structure
    # Note: According to DataForSEO docs, use a high bid value to level other factors
    payload_item = {
        "keywords": keywords,
        "location_code": location_code,
        "language_code": language_code,
        "bid": 1.0,  # Required field - use high value for accurate forecasting
        "match": "broad",  # Required field - keyword match type: exact, phrase, or broad
    }
    
    # Add optional date range if provided
    if date_from:
        payload_item["date_from"] = date_from
    if date_to:
        payload_item["date_to"] = date_to
    
    payload = [payload_item]
    
    # Make the POST request
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {api_key_base64}"
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"DataForSEO API request failed with status code {response.status_code}: {response.text}")
    
    # Parse the response
    data = response.json()
    
    try:
        if 'tasks' not in data or not data['tasks']:
            raise Exception("No tasks in DataForSEO API response")
        
        task = data['tasks'][0]
        
        if task.get('status_code') != 20000:
            status_code = task.get('status_code')
            status_message = task.get('status_message', 'Unknown error')
            raise Exception(f"DataForSEO API error: {status_code} - {status_message}")
        
        if 'result' not in task or not task['result']:
            return {
                "total_count": 0,
                "items_count": 0,
                "items": [],
                "keywords": keywords,
                "location_code": location_code,
                "language_code": language_code
            }
        
        # The ad_traffic_by_keywords endpoint returns aggregated data directly in result
        # Structure: result contains data at top level, not in items array
        result = task['result'][0]
        
        # Check if result has items array (individual keyword data) or aggregated data
        if result.get('items') and len(result.get('items', [])) > 0:
            # Individual keyword data structure
            items = result.get('items', [])
            formatted_items = []
            
            for item in items:
                keyword_info = item.get('keyword_info', {})
                ad_traffic = item.get('ad_traffic', {})
                
                formatted_item = {
                    "keyword": item.get('keyword'),
                    "search_volume": keyword_info.get('search_volume'),
                    "competition": keyword_info.get('competition'),
                    "competition_level": keyword_info.get('competition_level'),
                    "competition_index": keyword_info.get('competition_index'),
                    "cpc": keyword_info.get('cpc'),
                    "cpc_min": keyword_info.get('cpc_min') or ad_traffic.get('cpc_min'),
                    "cpc_max": keyword_info.get('cpc_max') or ad_traffic.get('cpc_max'),
                    "low_top_of_page_bid": keyword_info.get('low_top_of_page_bid'),
                    "high_top_of_page_bid": keyword_info.get('high_top_of_page_bid'),
                    "ad_position": ad_traffic.get('ad_position_average') or ad_traffic.get('ad_position'),
                    "impressions": ad_traffic.get('impressions') or ad_traffic.get('daily_impressions_average'),
                    "clicks": ad_traffic.get('clicks') or ad_traffic.get('daily_clicks_average'),
                }
                
                cost_micros = ad_traffic.get('cost_micros') or ad_traffic.get('daily_cost_average')
                if cost_micros:
                    formatted_item["cost_micros"] = cost_micros
                    formatted_item["cost_usd"] = cost_micros / 1_000_000 if isinstance(cost_micros, (int, float)) else None
                else:
                    formatted_item["cost_micros"] = None
                    formatted_item["cost_usd"] = None
                
                formatted_items.append(formatted_item)
        else:
            # Aggregated data structure (all keywords combined)
            # Create one item per keyword with aggregated metrics
            formatted_items = []
            
            for keyword in keywords:
                formatted_item = {
                    "keyword": keyword,
                    "search_volume": None,  # Not available in aggregated response
                    "competition": None,
                    "competition_level": None,
                    "competition_index": None,
                    "cpc": result.get('average_cpc'),
                    "cpc_min": None,  # Not available in aggregated response
                    "cpc_max": None,  # Not available in aggregated response
                    "low_top_of_page_bid": None,
                    "high_top_of_page_bid": None,
                    "ad_position": None,  # Not available in aggregated response
                    "impressions": result.get('impressions'),
                    "clicks": result.get('clicks'),
                    "ctr": result.get('ctr'),  # Click-through rate
                }
                
                # Cost is in dollars, convert to micros for consistency
                cost_usd = result.get('cost')
                if cost_usd:
                    formatted_item["cost_usd"] = cost_usd
                    formatted_item["cost_micros"] = int(cost_usd * 1_000_000) if isinstance(cost_usd, (int, float)) else None
                else:
                    formatted_item["cost_usd"] = None
                    formatted_item["cost_micros"] = None
                
                formatted_items.append(formatted_item)
        
        return {
            "total_count": len(formatted_items),
            "items_count": len(formatted_items),
            "items": formatted_items,
            "keywords": keywords,
            "location_code": location_code,
            "language_code": language_code,
            "date_from": date_from,
            "date_to": date_to,
            "date_interval": result.get('date_interval'),
            "match": result.get('match'),
            "bid": result.get('bid'),
        }
        
    except (KeyError, IndexError, TypeError) as e:
        raise Exception(f"Unexpected response structure from DataForSEO API: {e}. Response: {data}")


def _parse_keywords_data_result(data: dict) -> list[dict]:
    """Extract list of { keyword, search_volume?, cpc?, competition? } from DataForSEO keywords_data task result."""
    tasks = data.get("tasks") or []
    if not tasks:
        raise ValueError("DataForSEO API returned no tasks")
    task = tasks[0]
    if task.get("status_code") != 20000:
        raise ValueError(
            f"DataForSEO API error: {task.get('status_code')} - {task.get('status_message', 'Unknown')}"
        )
    result_list = task.get("result") or []
    out = []
    for item in result_list:
        out.append({
            "keyword": item.get("keyword") or "",
            "search_volume": item.get("search_volume"),
            "cpc": item.get("cpc"),
            "competition": item.get("competition"),
        })
    return out


@mcp.tool
def keywords_for_site(
    url: str,
    location_code: int,
    language_code: str = "en",
    limit: Optional[int] = 50,
) -> dict:
    """
    Get keywords relevant to a site or page using DataForSEO Google Ads Keywords For Site API.
    Returns seed keywords with search_volume, cpc, competition; raw response included for debugging.

    Args:
        url: Target URL or domain (e.g. https://example.com or example.com).
        location_code: DataForSEO location code (e.g. 2840 US, 2124 Canada).
        language_code: Language code (default en).
        limit: Max number of keywords to return (default 50). API returns up to 2000; we slice to this.

    Returns:
        { seeds: Array<{ keyword, search_volume?, cpc?, competition? }>, raw: full API response }
    """
    if not url or not url.strip():
        return {"seeds": [], "raw": None}
    target = url.strip()
    payload = [{
        "target": target,
        "location_code": location_code,
        "language_code": language_code,
    }]
    data = _dataforseo_keywords_post("keywords_for_site/live", payload)
    try:
        items = _parse_keywords_data_result(data)
    except ValueError:
        return {"seeds": [], "raw": data}
    limit = limit if limit is not None else 50
    seeds = items[: max(1, limit)]
    return {"seeds": seeds, "raw": data}


@mcp.tool
def keywords_for_keywords(
    keywords: list[str],
    location_code: int,
    language_code: str = "en",
    limit: Optional[int] = 200,
) -> dict:
    """
    Get keyword suggestions for given seed keywords using DataForSEO Google Ads Keywords For Keywords API.
    Returns suggestions with search_volume, cpc, competition; raw response included for debugging.

    Args:
        keywords: Seed keywords (max 20 per API).
        location_code: DataForSEO location code (e.g. 2840 US, 2124 Canada).
        language_code: Language code (default en).
        limit: Max number of suggestions to return (default 200). API returns up to 20000; we slice to this.

    Returns:
        { suggestions: Array<{ keyword, search_volume?, cpc?, competition? }>, raw: full API response }
    """
    if not keywords:
        return {"suggestions": [], "raw": None}
    payload = [{
        "keywords": keywords[:20],
        "location_code": location_code,
        "language_code": language_code,
    }]
    data = _dataforseo_keywords_post("keywords_for_keywords/live", payload)
    try:
        items = _parse_keywords_data_result(data)
    except ValueError:
        return {"suggestions": [], "raw": data}
    limit_val = limit if limit is not None else 200
    suggestions = items[: max(1, limit_val)]
    return {"suggestions": suggestions, "raw": data}


@mcp.tool
def addKeyword(client_id: int, keyword: str, search_volume: Optional[int] = None, keyword_difficulty: Optional[int] = None) -> dict:
    """
    Adds a keyword to a client's keyword list.
    
    This tool allows The Brute or other agents to add keywords incrementally
    while generating keyword ideas, preventing timeouts.
    
    Args:
        client_id (int): The client ID.
        keyword (str): The keyword to add.
        search_volume (int, optional): Search volume for the keyword.
        keyword_difficulty (int, optional): Keyword difficulty score.
    
    Returns:
        dict: A dictionary containing status, client_id, keyword, and the created keyword_id.
    """
    db = get_db_session()
    try:
        # Validate keyword
        keyword = keyword.strip()
        if not keyword:
            raise ValueError("Keyword cannot be empty")
        
        # Check if keyword already exists for this client
        existing = db.execute(
            text("""
                SELECT id FROM keyword_ideas
                WHERE client_id = :client_id AND LOWER(keyword) = LOWER(:keyword)
            """),
            {"client_id": client_id, "keyword": keyword}
        ).fetchone()
        
        if existing:
            return {
                "status": "already_exists",
                "client_id": client_id,
                "keyword": keyword,
                "keyword_id": existing[0],
                "message": f"Keyword '{keyword}' already exists for this client"
            }
        
        # Insert new keyword
        result = db.execute(
            text("""
                INSERT INTO keyword_ideas (client_id, keyword, source, search_volume, keyword_difficulty, created_at, updated_at)
                VALUES (:client_id, :keyword, 'ai', :search_volume, :keyword_difficulty, NOW(), NOW())
                RETURNING id
            """),
            {
                "client_id": client_id,
                "keyword": keyword,
                "search_volume": search_volume,
                "keyword_difficulty": keyword_difficulty
            }
        )
        
        keyword_id = result.fetchone()[0]
        db.commit()
        
        return {
            "status": "success",
            "client_id": client_id,
            "keyword": keyword,
            "keyword_id": keyword_id,
            "search_volume": search_volume,
            "keyword_difficulty": keyword_difficulty,
            "message": f"Successfully added keyword '{keyword}' to client {client_id}"
        }
    except Exception as e:
        db.rollback()
        raise Exception(f"Failed to add keyword: {str(e)}")
    finally:
        db.close()


@mcp.tool
def readHTML(client_id: int, blog_id: int, version_number: int) -> dict:
    """
    Fetches a specific HTML version from the database.
    
    Args:
        client_id (int): The client ID.
        blog_id (int): The blog idea ID.
        version_number (int): The version number to fetch.
    
    Returns:
        dict: A dictionary containing client_id, blog_id, version_number, and html.
    """
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                SELECT client_id, blog_idea_id, version_number, html
                FROM html_artifacts
                WHERE client_id = :client_id
                  AND blog_idea_id = :blog_id
                  AND version_number = :version_number
                LIMIT 1
            """),
            {"client_id": client_id, "blog_id": blog_id, "version_number": version_number}
        ).fetchone()
        
        if not result:
            raise ValueError(f"HTML artifact not found for client_id={client_id}, blog_id={blog_id}, version_number={version_number}")
        
        return {
            "client_id": result[0],
            "blog_id": result[1],
            "version_number": result[2],
            "html": result[3]
        }
    finally:
        db.close()


@mcp.tool
def writeHTML(client_id: int, blog_id: int, version_number: int, html: str) -> dict:
    """
    Writes HTML to the database. The version number must be one higher than the current maximum version.
    
    Args:
        client_id (int): The client ID.
        blog_id (int): The blog idea ID.
        version_number (int): The new version number (must be current_max + 1).
        html (str): The HTML content to store.
    
    Returns:
        dict: A dictionary containing client_id, blog_id, and the updated version_number.
    """
    db = get_db_session()
    try:
        # Get the current maximum version number
        max_version_result = db.execute(
            text("""
                SELECT COALESCE(MAX(version_number), 0)
                FROM html_artifacts
                WHERE client_id = :client_id AND blog_idea_id = :blog_id
            """),
            {"client_id": client_id, "blog_id": blog_id}
        ).fetchone()
        
        current_max_version = max_version_result[0] if max_version_result else 0
        expected_version = current_max_version + 1
        
        if version_number != expected_version:
            raise ValueError(
                f"Version number mismatch. Expected version {expected_version} "
                f"(current max is {current_max_version}), but got {version_number}"
            )
        
        # Insert the new HTML artifact
        db.execute(
            text("""
                INSERT INTO html_artifacts (client_id, blog_idea_id, version_number, html)
                VALUES (:client_id, :blog_id, :version_number, :html)
            """),
            {"client_id": client_id, "blog_id": blog_id, "version_number": version_number, "html": html}
        )
        db.commit()
        
        return {
            "client_id": client_id,
            "blog_id": blog_id,
            "version_number": version_number
        }
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()


@mcp.tool
def getClientOverview(client_id: int) -> dict:
    """
    Fetches client overview data from the general_contexts table (formerly client_contexts).
    
    Args:
        client_id (int): The client ID.
    
    Returns:
        dict: A dictionary containing domain, call_to_action, about, competitors, 
              ideal_target_market, company_details, and social_links.
    """
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                SELECT domain, call_to_action, about, competitors, ideal_target_market, 
                       company_details, social_links
                FROM general_contexts
                WHERE client_id = :client_id
            """),
            {"client_id": client_id}
        ).fetchone()
        
        if not result:
            raise ValueError(f"Client context not found for client_id={client_id}")
        
        return {
            "domain": result[0],
            "call_to_action": result[1],
            "about": result[2],
            "competitors": result[3],
            "ideal_target_market": result[4],
            "company_details": result[5],
            "social_links": result[6]
        }
    finally:
        db.close()


@mcp.tool
def getClientWritingRules(client_id: int) -> dict:
    """
    Fetches client writing rules from the general_contexts table (formerly client_contexts).
    
    Args:
        client_id (int): The client ID.
    
    Returns:
        dict: A dictionary containing brand_pov, brand_safety, questionnaire, 
              author_tone, and author_rules.
    """
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                SELECT brand_pov, brand_safety, questionnaire, author_tone, author_rules
                FROM general_contexts
                WHERE client_id = :client_id
            """),
            {"client_id": client_id}
        ).fetchone()
        
        if not result:
            raise ValueError(f"Client context not found for client_id={client_id}")
        
        return {
            "brand_pov": result[0],
            "brand_safety": result[1],
            "questionnaire": result[2],
            "author_tone": result[3],
            "author_rules": result[4]
        }
    finally:
        db.close()


@mcp.tool
def generate_image(prompt: str, filename: str = "generated_image.png") -> dict:
    """
    Generates an image using Google Gemini's image generation API, decodes base64 if needed,
    and uploads it to AWS S3 for hosting.
    
    Args:
        prompt (str): The text prompt describing the image to generate.
        filename (str): The filename to use when saving the image (default: "generated_image.png").
    
    Returns:
        dict: A dictionary containing the hosted image URL and other metadata.
    """
    if not _google_api_key:
        raise ValueError("GOOGLE_API_KEY environment variable is not set.")
    
    if not _s3_client:
        raise ValueError("AWS credentials not configured. Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_S3_BUCKET.")
    
    if not _aws_s3_bucket:
        raise ValueError("AWS_S3_BUCKET environment variable is not set.")
    
    # Call Google Gemini API with exponential backoff retry logic
    headers = {
        "x-goog-api-key": _google_api_key,
        "Content-Type": "application/json"
    }
    
    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ]
    }
    
    # Exponential backoff: 3 retries with delays of 1s, 2s, 4s
    max_retries = 3
    base_delay = 1.0  # Start with 1 second
    
    result = None
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            response = requests.post(_google_api_url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            result = response.json()
            break  # Success, exit retry loop
            
        except requests.exceptions.RequestException as e:
            last_exception = e
            if attempt < max_retries - 1:  # Don't sleep on last attempt
                delay = base_delay * (2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                time.sleep(delay)
                continue
            else:
                # Last attempt failed, raise the exception
                raise Exception(f"Google Gemini API request failed after {max_retries} attempts: {str(e)}")
    
    if result is None:
        raise Exception(f"Google Gemini API request failed after {max_retries} attempts: {str(last_exception)}")
    
    # Extract image data from response
    # According to docs: response.candidates[0].content.parts - iterate through parts to find inlineData
    # Parts can contain both text and inlineData, so we need to find the image part
    try:
        if 'candidates' not in result or not result['candidates']:
            import json
            raise Exception(f"No candidates in Google Gemini API response. Full response: {json.dumps(result, indent=2)}")
        
        candidate = result['candidates'][0]
        if 'content' not in candidate or 'parts' not in candidate['content']:
            import json
            raise Exception(f"No content/parts in Google Gemini API response. Candidate: {json.dumps(candidate, indent=2)}")
        
        parts = candidate['content']['parts']
        if not parts:
            raise Exception("Parts list is empty in Google Gemini API response")
        
        # Iterate through parts to find the one with inlineData
        # The first part might be text, so we need to check all parts
        image_base64 = None
        mime_type = 'image/png'
        
        for part in parts:
            if 'inlineData' in part:
                inline_data = part['inlineData']
                image_base64 = inline_data.get('data')
                mime_type = inline_data.get('mimeType', 'image/png')
                break
        
        if not image_base64:
            import json
            # Log what parts we actually got for debugging
            part_types = [list(p.keys()) for p in parts]
            raise Exception(f"No inlineData found in any part. Part types found: {part_types}. Full response: {json.dumps(result, indent=2)}")
        
        # Decode base64 image
        image_bytes = base64.b64decode(image_base64)
        
    except (KeyError, IndexError, TypeError) as e:
        import json
        raise Exception(f"Unexpected response structure from Google Gemini API: {e}. Response: {json.dumps(result, indent=2)}")
    
    # Use mime_type from API response, or fallback to filename extension
    content_type = mime_type
    if not content_type:
        # Fallback to determining from filename
        if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg'):
            content_type = "image/jpeg"
        elif filename.lower().endswith('.gif'):
            content_type = "image/gif"
        elif filename.lower().endswith('.webp'):
            content_type = "image/webp"
        else:
            content_type = "image/png"
    
    # Upload to S3
    try:
        _s3_client.put_object(
            Bucket=_aws_s3_bucket,
            Key=filename,
            Body=image_bytes,
            ContentType=content_type
            # Note: ACLs are disabled on this bucket. Make bucket public via bucket policy instead.
        )
        
        # Construct the public URL
        # Format: https://bucket-name.s3.region.amazonaws.com/filename
        hosted_url = f"https://{_aws_s3_bucket}.s3.{_aws_region}.amazonaws.com/{filename}"
        
        return {
            "url": hosted_url,
            "filename": filename,
            "prompt": prompt,
            "model": "gemini-2.5-flash-image",
            "mime_type": mime_type,
            "hosting_service": "aws_s3",
            "bucket": _aws_s3_bucket
        }
    except Exception as e:
        raise Exception(f"Failed to upload image to S3: {str(e)}")


@mcp.tool
def update_discovery_document(
    client_id: int,
    # Domain
    domain: Optional[str] = None,
    # Section 0: Meta/Header
    client_name: Optional[str] = None,
    discovery_date: Optional[str] = None,
    contact_name: Optional[str] = None,
    contact_title: Optional[str] = None,
    contact_email: Optional[str] = None,
    contact_phone: Optional[str] = None,
    industry: Optional[str] = None,
    # Section 1: Company Overview & Business Objectives
    primary_business: Optional[str] = None,
    years_in_business: Optional[str] = None,
    annual_revenue: Optional[str] = None,
    num_employees: Optional[int] = None,
    geographic_market: Optional[str] = None,
    primary_goal_12_months: Optional[str] = None,
    target_leads_per_month: Optional[int] = None,
    target_leads_timeframe: Optional[str] = None,
    target_cpl_amount: Optional[str] = None,
    target_cpl_reasoning: Optional[str] = None,
    qualified_lead_definition: Optional[str] = None,
    customer_ltv: Optional[str] = None,
    customer_ltv_calculation: Optional[str] = None,
    sales_cycle_length: Optional[str] = None,
    close_rate_percent: Optional[str] = None,
    close_rate_not_tracked: Optional[bool] = None,
    current_monthly_leads: Optional[int] = None,
    current_lead_generation_method: Optional[str] = None,
    current_sql_percent: Optional[str] = None,
    previous_marketing_efforts: Optional[str] = None,  # JSON string
    what_is_working: Optional[str] = None,
    budget_monthly: Optional[str] = None,
    budget_quarterly: Optional[str] = None,
    budget_annual: Optional[str] = None,
    leadgen_budget_monthly: Optional[str] = None,
    leadgen_budget_quarterly: Optional[str] = None,
    leadgen_budget_annual: Optional[str] = None,
    seasonal_peak_months: Optional[str] = None,
    seasonal_slow_months: Optional[str] = None,
    seasonal_details: Optional[str] = None,
    # Section 2: Target Audience
    ideal_customer_description: Optional[str] = None,
    decision_maker_titles: Optional[str] = None,
    decision_authority_level: Optional[str] = None,
    target_company_size: Optional[str] = None,
    target_industries: Optional[str] = None,
    geographic_focus: Optional[str] = None,
    customer_age_range: Optional[str] = None,
    customer_gender: Optional[str] = None,
    customer_education: Optional[str] = None,
    customer_income_range: Optional[str] = None,
    pain_point_1: Optional[str] = None,
    pain_point_2: Optional[str] = None,
    pain_point_3: Optional[str] = None,
    goal_motivation_1: Optional[str] = None,
    goal_motivation_2: Optional[str] = None,
    goal_motivation_3: Optional[str] = None,
    buying_process: Optional[str] = None,
    secondary_audiences: Optional[str] = None,  # JSON string
    # Section 3: Value Proposition & Messaging
    differentiation: Optional[str] = None,
    value_prop_1: Optional[str] = None,
    value_prop_2: Optional[str] = None,
    value_prop_3: Optional[str] = None,
    why_choose_us: Optional[str] = None,
    market_perception: Optional[str] = None,
    brand_voice_tones: Optional[str] = None,  # JSON array string
    brand_voice_other: Optional[str] = None,
    messaging_theme_1: Optional[str] = None,
    messaging_theme_2: Optional[str] = None,
    messaging_theme_3: Optional[str] = None,
    testimonials_available: Optional[str] = None,
    testimonials_count: Optional[int] = None,
    testimonials_examples: Optional[str] = None,
    proof_customer_stories: Optional[str] = None,
    proof_statistics: Optional[str] = None,
    proof_awards: Optional[str] = None,
    proof_notable_customers: Optional[str] = None,
    # Section 4: Competitive Landscape
    competitor_1: Optional[str] = None,
    competitor_2: Optional[str] = None,
    competitor_3: Optional[str] = None,
    competitor_channels: Optional[str] = None,  # JSON string
    competitor_strengths: Optional[str] = None,
    competitive_advantages: Optional[str] = None,
    # Section 5: SetSail Services Assessment
    services_interested: Optional[str] = None,  # JSON array string
    services_interest_reasons: Optional[str] = None,  # JSON object string
    google_ads_used: Optional[bool] = None,
    google_ads_experience: Optional[str] = None,
    meta_ads_used: Optional[bool] = None,
    meta_ads_experience: Optional[str] = None,
    social_media_used: Optional[bool] = None,
    social_media_experience: Optional[str] = None,
    seo_used: Optional[bool] = None,
    seo_experience: Optional[str] = None,
    website_dev_used: Optional[bool] = None,
    website_dev_experience: Optional[str] = None,
    services_not_wanted: Optional[bool] = None,
    services_not_wanted_details: Optional[str] = None,
    # Section 6: Current Digital Presence
    has_website: Optional[bool] = None,
    website_url: Optional[str] = None,
    website_status: Optional[str] = None,  # JSON array string
    website_status_other: Optional[str] = None,
    website_monthly_visitors: Optional[int] = None,
    website_conversion_rate: Optional[str] = None,
    website_main_issues: Optional[str] = None,
    social_platforms: Optional[str] = None,  # JSON string
    social_strategy: Optional[str] = None,
    # Section 7: Analytics & Tracking
    analytics_tools: Optional[str] = None,  # JSON array string
    analytics_other: Optional[str] = None,
    crm_name: Optional[str] = None,
    crm_features_used: Optional[str] = None,
    lead_data_tracked: Optional[str] = None,
    conversion_tracking_status: Optional[str] = None,
    conversion_tracking_details: Optional[str] = None,
    crm_integration_possible: Optional[str] = None,
    crm_integration_details: Optional[str] = None,
    # Section 8: Current Tech Stack
    tools_used: Optional[str] = None,  # JSON array string
    tools_other: Optional[str] = None,
    # Section 9: Team & Support
    poc_name: Optional[str] = None,
    poc_title: Optional[str] = None,
    poc_email: Optional[str] = None,
    poc_phone: Optional[str] = None,
    poc_availability: Optional[str] = None,
    other_stakeholders: Optional[str] = None,  # JSON string
    final_decision_name: Optional[str] = None,
    final_decision_title: Optional[str] = None,
    decision_timeline: Optional[str] = None,
    resources_available: Optional[str] = None,  # JSON array string
    resources_other: Optional[str] = None,
    has_dev_support: Optional[bool] = None,
    has_marketing_support: Optional[bool] = None,
    has_sales_support: Optional[bool] = None,
    internal_resources_other: Optional[str] = None,
    # Section 10: Timeline & Expectations
    target_launch_date: Optional[str] = None,
    urgency_level: Optional[str] = None,
    first_leads_timeframe: Optional[str] = None,
    ramp_up_timeframe: Optional[str] = None,
    full_results_timeframe: Optional[str] = None,
    success_indicator_1: Optional[str] = None,
    success_indicator_2: Optional[str] = None,
    success_indicator_3: Optional[str] = None,
    exceed_expectations: Optional[str] = None,
    concern_1: Optional[str] = None,
    concern_2: Optional[str] = None,
    concern_3: Optional[str] = None,
    # Section 11: Additional Information
    regulatory_considerations: Optional[str] = None,  # JSON array string
    regulatory_other: Optional[str] = None,
    industry_keywords: Optional[str] = None,
    is_seasonal: Optional[bool] = None,
    seasonality_peak: Optional[str] = None,
    seasonality_slow: Optional[str] = None,
    seasonality_strategy: Optional[str] = None,
    anything_else: Optional[str] = None,
    success_definition: Optional[str] = None,
    case_study_consent: Optional[str] = None,
) -> dict:
    """
    Updates or creates a discovery document for a client. All fields except client_id are optional.
    Only provide fields you have researched and are confident about - leave others as None.
    
    This tool allows LLMs to research a company's domain and fill in discovery document fields
    with information gathered from their website, public records, and other sources.
    
    Args:
        client_id (int): Required. The client ID to update the discovery document for.
        domain (str): The client's domain name (e.g., "acme.com").
        
        --- SECTION 0: META/HEADER ---
        client_name (str): Company/client name.
        discovery_date (str): Date of discovery in YYYY-MM-DD format.
        contact_name (str): Primary contact person's name.
        contact_title (str): Primary contact's job title.
        contact_email (str): Primary contact's email.
        contact_phone (str): Primary contact's phone number.
        industry (str): The industry the company operates in.
        
        --- SECTION 1: COMPANY OVERVIEW & BUSINESS OBJECTIVES ---
        primary_business (str): Company's primary business/service offering description.
        years_in_business (str): How long the company has been in business.
        annual_revenue (str): Current annual revenue or revenue range (e.g., "$1-3M").
        num_employees (int): Number of employees.
        geographic_market (str): Geographic service area/market.
        primary_goal_12_months (str): Primary business goal for the next 12 months.
        target_leads_per_month (int): Target number of leads per month.
        target_leads_timeframe (str): Timeframe for achieving target leads.
        target_cpl_amount (str): Target cost per lead amount.
        target_cpl_reasoning (str): How the target CPL was determined.
        qualified_lead_definition (str): What defines a "qualified lead" for this business.
        customer_ltv (str): Estimated customer lifetime value.
        customer_ltv_calculation (str): How LTV is calculated.
        sales_cycle_length (str): Typical sales cycle length (e.g., "6-8 weeks").
        close_rate_percent (str): Percentage of leads that close.
        close_rate_not_tracked (bool): Whether close rate is currently tracked.
        current_monthly_leads (int): Current number of monthly leads.
        current_lead_generation_method (str): How leads are currently generated.
        current_sql_percent (str): Percentage of leads that are sales-qualified.
        previous_marketing_efforts (str): JSON array of previous marketing efforts with fields: channel_name, timeframe, result, why_worked.
        what_is_working (str): What is currently working in their marketing.
        budget_monthly (str): Total monthly marketing budget.
        budget_quarterly (str): Total quarterly marketing budget.
        budget_annual (str): Total annual marketing budget.
        leadgen_budget_monthly (str): Monthly budget allocated to lead generation.
        leadgen_budget_quarterly (str): Quarterly budget for lead generation.
        leadgen_budget_annual (str): Annual budget for lead generation.
        seasonal_peak_months (str): Peak business months.
        seasonal_slow_months (str): Slow business months.
        seasonal_details (str): Details about seasonality.
        
        --- SECTION 2: TARGET AUDIENCE ---
        ideal_customer_description (str): Description of the ideal customer.
        decision_maker_titles (str): Job titles of decision makers.
        decision_authority_level (str): Decision authority level (C-Suite, Director, Manager, Other).
        target_company_size (str): Target company size (employees or revenue range).
        target_industries (str): List of target industries.
        geographic_focus (str): Geographic focus for customers.
        customer_age_range (str): Target customer age range.
        customer_gender (str): Target gender (All, Specific).
        customer_education (str): Education level (High school, Bachelor's, Advanced, Any).
        customer_income_range (str): Income/budget authority range.
        pain_point_1 (str): Main customer pain point #1.
        pain_point_2 (str): Main customer pain point #2.
        pain_point_3 (str): Main customer pain point #3.
        goal_motivation_1 (str): Customer goal/motivation #1.
        goal_motivation_2 (str): Customer goal/motivation #2.
        goal_motivation_3 (str): Customer goal/motivation #3.
        buying_process (str): Typical buying process description.
        secondary_audiences (str): JSON array of secondary audiences with fields: description, job_titles, why_target.
        
        --- SECTION 3: VALUE PROPOSITION & MESSAGING ---
        differentiation (str): What makes the business different from competitors.
        value_prop_1 (str): Top value proposition #1.
        value_prop_2 (str): Top value proposition #2.
        value_prop_3 (str): Top value proposition #3.
        why_choose_us (str): Why prospects should choose them over competitors.
        market_perception (str): How they want to be perceived in the market.
        brand_voice_tones (str): JSON array of brand voice/tone selections from: "Professional / Corporate", "Casual / Conversational", "Educational / Thought Leadership", "Results-Driven / ROI-Focused", "Innovative / Forward-Thinking", "Supportive / Customer-Centric".
        brand_voice_other (str): Other brand voice description if applicable.
        messaging_theme_1 (str): Key messaging theme #1.
        messaging_theme_2 (str): Key messaging theme #2.
        messaging_theme_3 (str): Key messaging theme #3.
        testimonials_available (str): Whether testimonials are available (Yes, Some, No).
        testimonials_count (int): Number of testimonials/case studies available.
        testimonials_examples (str): Examples or descriptions of testimonials.
        proof_customer_stories (str): Customer success stories.
        proof_statistics (str): Relevant statistics/metrics.
        proof_awards (str): Awards/certifications.
        proof_notable_customers (str): Notable customers.
        
        --- SECTION 4: COMPETITIVE LANDSCAPE ---
        competitor_1 (str): Main competitor #1 name.
        competitor_2 (str): Main competitor #2 name.
        competitor_3 (str): Main competitor #3 name.
        competitor_channels (str): JSON array of competitor channel info with fields: name, google_ads, meta_ads, social_media, seo_content, website_quality, other_channels.
        competitor_strengths (str): What competitors are doing well.
        competitive_advantages (str): Where they have competitive advantages.
        
        --- SECTION 5: SETSAIL SERVICES ASSESSMENT ---
        services_interested (str): JSON array of services interested in: "google_ads", "meta_ads", "social_media", "seo", "website_dev".
        services_interest_reasons (str): JSON object mapping service to reason for interest.
        google_ads_used (bool): Whether Google Ads has been used before.
        google_ads_experience (str): Google Ads experience level (Beginner, Intermediate, Advanced, N/A).
        meta_ads_used (bool): Whether Meta Ads has been used before.
        meta_ads_experience (str): Meta Ads experience level.
        social_media_used (bool): Whether social media marketing has been used.
        social_media_experience (str): Social media experience level.
        seo_used (bool): Whether SEO has been used before.
        seo_experience (str): SEO experience level.
        website_dev_used (bool): Whether website development services were used.
        website_dev_experience (str): Website development experience level.
        services_not_wanted (bool): Whether there are services they specifically don't want.
        services_not_wanted_details (str): Details on services not wanted and why.
        
        --- SECTION 6: CURRENT DIGITAL PRESENCE ---
        has_website (bool): Whether they currently have a website.
        website_url (str): Website URL.
        website_status (str): JSON array of website status selections: "Recently built", "Needs updating / redesign", "Being built".
        website_status_other (str): Other website status description.
        website_monthly_visitors (int): Monthly website visitors.
        website_conversion_rate (str): Website conversion rate.
        website_main_issues (str): Main website issues.
        social_platforms (str): JSON array of social platforms with fields: platform, followers, activity_level, primary_goal.
        social_strategy (str): Current social media strategy description.
        
        --- SECTION 7: ANALYTICS & TRACKING ---
        analytics_tools (str): JSON array of analytics tools used: "Google Analytics 4", "Google Analytics (Universal Analytics)", "None currently".
        analytics_other (str): Other analytics tools.
        crm_name (str): CRM/lead management system name.
        crm_features_used (str): CRM features being used.
        lead_data_tracked (str): What lead data is tracked.
        conversion_tracking_status (str): Conversion tracking status (Yes – Fully set up, Partially set up, No – Needs to be set up).
        conversion_tracking_details (str): Details on conversion tracking setup.
        crm_integration_possible (str): Whether CRM integration is possible (Yes – CRM supports integrations, Unsure, No – Manual lead entry only).
        crm_integration_details (str): Details on CRM integration possibilities.
        
        --- SECTION 8: CURRENT TECH STACK ---
        tools_used (str): JSON array of tools used: "Google Workspace (Gmail, Docs, Sheets)", "Microsoft 365", "Slack", "Monday.com", "Asana", "Salesforce", "HubSpot", "Zapier".
        tools_other (str): Other tools used.
        
        --- SECTION 9: TEAM & SUPPORT ---
        poc_name (str): Primary point of contact name.
        poc_title (str): Primary point of contact title.
        poc_email (str): Primary point of contact email.
        poc_phone (str): Primary point of contact phone.
        poc_availability (str): POC availability (days/hours).
        other_stakeholders (str): JSON array of other stakeholders with fields: name, title, role, email.
        final_decision_name (str): Name of final decision authority.
        final_decision_title (str): Title of final decision authority.
        decision_timeline (str): Typical decision timeline.
        resources_available (str): JSON array of available resources: "Brand guidelines / style guide", "Product / service information documents", etc.
        resources_other (str): Other resources available.
        has_dev_support (bool): Whether developer/IT support is available.
        has_marketing_support (bool): Whether marketing support is available.
        has_sales_support (bool): Whether sales support is available.
        internal_resources_other (str): Other internal resources.
        
        --- SECTION 10: TIMELINE & EXPECTATIONS ---
        target_launch_date (str): Target strategy launch date (YYYY-MM-DD).
        urgency_level (str): How urgent (Very flexible, Moderate, Fast, Urgent).
        first_leads_timeframe (str): Timeframe for first leads.
        ramp_up_timeframe (str): Timeframe for performance ramp-up.
        full_results_timeframe (str): Timeframe for full results.
        success_indicator_1 (str): Success indicator #1 for first 90 days.
        success_indicator_2 (str): Success indicator #2.
        success_indicator_3 (str): Success indicator #3.
        exceed_expectations (str): What would exceed expectations.
        concern_1 (str): Biggest concern #1.
        concern_2 (str): Biggest concern #2.
        concern_3 (str): Biggest concern #3.
        
        --- SECTION 11: ADDITIONAL INFORMATION ---
        regulatory_considerations (str): JSON array of regulatory considerations: "HIPAA (Healthcare)", "GDPR / Privacy regulations", "Financial services regulations", "Advertising restrictions", "None".
        regulatory_other (str): Other regulatory considerations.
        industry_keywords (str): Industry-specific keywords/terminology.
        is_seasonal (bool): Whether the business is seasonal.
        seasonality_peak (str): Peak season months if seasonal.
        seasonality_slow (str): Slow season months if seasonal.
        seasonality_strategy (str): How seasonality should affect strategy.
        anything_else (str): Anything else to know about the business/goals.
        success_definition (str): What would make the engagement successful.
        case_study_consent (str): Consent to use results as case study (Yes, Maybe – ask later, No).
    
    Returns:
        dict: A dictionary containing status, client_id, and the number of fields updated.
    """
    db = get_db_session()
    try:
        # Check if discovery document exists for this client
        existing = db.execute(
            text("SELECT id FROM discovery_documents WHERE client_id = :client_id"),
            {"client_id": client_id}
        ).fetchone()
        
        # Build the update data - only include non-None values
        update_data = {}
        
        # Helper to parse JSON strings for array/object fields
        def parse_json_field(value: Optional[str]):
            if value is None:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value  # Return as-is if not valid JSON
        
        # Map all parameters to their database column names
        field_mappings = {
            "domain": domain,
            "client_name": client_name,
            "discovery_date": discovery_date,
            "contact_name": contact_name,
            "contact_title": contact_title,
            "contact_email": contact_email,
            "contact_phone": contact_phone,
            "industry": industry,
            "primary_business": primary_business,
            "years_in_business": years_in_business,
            "annual_revenue": annual_revenue,
            "num_employees": num_employees,
            "geographic_market": geographic_market,
            "primary_goal_12_months": primary_goal_12_months,
            "target_leads_per_month": target_leads_per_month,
            "target_leads_timeframe": target_leads_timeframe,
            "target_cpl_amount": target_cpl_amount,
            "target_cpl_reasoning": target_cpl_reasoning,
            "qualified_lead_definition": qualified_lead_definition,
            "customer_ltv": customer_ltv,
            "customer_ltv_calculation": customer_ltv_calculation,
            "sales_cycle_length": sales_cycle_length,
            "close_rate_percent": close_rate_percent,
            "close_rate_not_tracked": close_rate_not_tracked,
            "current_monthly_leads": current_monthly_leads,
            "current_lead_generation_method": current_lead_generation_method,
            "current_sql_percent": current_sql_percent,
            "what_is_working": what_is_working,
            "budget_monthly": budget_monthly,
            "budget_quarterly": budget_quarterly,
            "budget_annual": budget_annual,
            "leadgen_budget_monthly": leadgen_budget_monthly,
            "leadgen_budget_quarterly": leadgen_budget_quarterly,
            "leadgen_budget_annual": leadgen_budget_annual,
            "seasonal_peak_months": seasonal_peak_months,
            "seasonal_slow_months": seasonal_slow_months,
            "seasonal_details": seasonal_details,
            "ideal_customer_description": ideal_customer_description,
            "decision_maker_titles": decision_maker_titles,
            "decision_authority_level": decision_authority_level,
            "target_company_size": target_company_size,
            "target_industries": target_industries,
            "geographic_focus": geographic_focus,
            "customer_age_range": customer_age_range,
            "customer_gender": customer_gender,
            "customer_education": customer_education,
            "customer_income_range": customer_income_range,
            "pain_point_1": pain_point_1,
            "pain_point_2": pain_point_2,
            "pain_point_3": pain_point_3,
            "goal_motivation_1": goal_motivation_1,
            "goal_motivation_2": goal_motivation_2,
            "goal_motivation_3": goal_motivation_3,
            "buying_process": buying_process,
            "differentiation": differentiation,
            "value_prop_1": value_prop_1,
            "value_prop_2": value_prop_2,
            "value_prop_3": value_prop_3,
            "why_choose_us": why_choose_us,
            "market_perception": market_perception,
            "brand_voice_other": brand_voice_other,
            "messaging_theme_1": messaging_theme_1,
            "messaging_theme_2": messaging_theme_2,
            "messaging_theme_3": messaging_theme_3,
            "testimonials_available": testimonials_available,
            "testimonials_count": testimonials_count,
            "testimonials_examples": testimonials_examples,
            "proof_customer_stories": proof_customer_stories,
            "proof_statistics": proof_statistics,
            "proof_awards": proof_awards,
            "proof_notable_customers": proof_notable_customers,
            "competitor_1": competitor_1,
            "competitor_2": competitor_2,
            "competitor_3": competitor_3,
            "competitor_strengths": competitor_strengths,
            "competitive_advantages": competitive_advantages,
            "google_ads_used": google_ads_used,
            "google_ads_experience": google_ads_experience,
            "meta_ads_used": meta_ads_used,
            "meta_ads_experience": meta_ads_experience,
            "social_media_used": social_media_used,
            "social_media_experience": social_media_experience,
            "seo_used": seo_used,
            "seo_experience": seo_experience,
            "website_dev_used": website_dev_used,
            "website_dev_experience": website_dev_experience,
            "services_not_wanted": services_not_wanted,
            "services_not_wanted_details": services_not_wanted_details,
            "has_website": has_website,
            "website_url": website_url,
            "website_status_other": website_status_other,
            "website_monthly_visitors": website_monthly_visitors,
            "website_conversion_rate": website_conversion_rate,
            "website_main_issues": website_main_issues,
            "social_strategy": social_strategy,
            "analytics_other": analytics_other,
            "crm_name": crm_name,
            "crm_features_used": crm_features_used,
            "lead_data_tracked": lead_data_tracked,
            "conversion_tracking_status": conversion_tracking_status,
            "conversion_tracking_details": conversion_tracking_details,
            "crm_integration_possible": crm_integration_possible,
            "crm_integration_details": crm_integration_details,
            "tools_other": tools_other,
            "poc_name": poc_name,
            "poc_title": poc_title,
            "poc_email": poc_email,
            "poc_phone": poc_phone,
            "poc_availability": poc_availability,
            "final_decision_name": final_decision_name,
            "final_decision_title": final_decision_title,
            "decision_timeline": decision_timeline,
            "resources_other": resources_other,
            "has_dev_support": has_dev_support,
            "has_marketing_support": has_marketing_support,
            "has_sales_support": has_sales_support,
            "internal_resources_other": internal_resources_other,
            "target_launch_date": target_launch_date,
            "urgency_level": urgency_level,
            "first_leads_timeframe": first_leads_timeframe,
            "ramp_up_timeframe": ramp_up_timeframe,
            "full_results_timeframe": full_results_timeframe,
            "success_indicator_1": success_indicator_1,
            "success_indicator_2": success_indicator_2,
            "success_indicator_3": success_indicator_3,
            "exceed_expectations": exceed_expectations,
            "concern_1": concern_1,
            "concern_2": concern_2,
            "concern_3": concern_3,
            "regulatory_other": regulatory_other,
            "industry_keywords": industry_keywords,
            "is_seasonal": is_seasonal,
            "seasonality_peak": seasonality_peak,
            "seasonality_slow": seasonality_slow,
            "seasonality_strategy": seasonality_strategy,
            "anything_else": anything_else,
            "success_definition": success_definition,
            "case_study_consent": case_study_consent,
        }
        
        # JSON fields that need parsing
        json_field_mappings = {
            "previous_marketing_efforts": previous_marketing_efforts,
            "secondary_audiences": secondary_audiences,
            "brand_voice_tones": brand_voice_tones,
            "competitor_channels": competitor_channels,
            "services_interested": services_interested,
            "services_interest_reasons": services_interest_reasons,
            "website_status": website_status,
            "social_platforms": social_platforms,
            "analytics_tools": analytics_tools,
            "tools_used": tools_used,
            "other_stakeholders": other_stakeholders,
            "resources_available": resources_available,
            "regulatory_considerations": regulatory_considerations,
        }
        
        # Add non-None simple fields
        for field, value in field_mappings.items():
            if value is not None:
                update_data[field] = value
        
        # Add non-None JSON fields (parse them first)
        for field, value in json_field_mappings.items():
            if value is not None:
                parsed = parse_json_field(value)
                if parsed is not None:
                    # If parsing failed and we got a plain string back,
                    # wrap it in an array since these fields expect arrays
                    if isinstance(parsed, str):
                        parsed = [parsed]
                    update_data[field] = json.dumps(parsed)
        
        if not update_data:
            return {
                "status": "no_changes",
                "client_id": client_id,
                "message": "No fields provided to update"
            }
        
        if existing:
            # Update existing document
            set_clauses = ", ".join([f"{k} = :{k}" for k in update_data.keys()])
            update_data["client_id"] = client_id
            db.execute(
                text(f"UPDATE discovery_documents SET {set_clauses}, updated_at = NOW() WHERE client_id = :client_id"),
                update_data
            )
            action = "updated"
        else:
            # Create new document
            update_data["client_id"] = client_id
            update_data["edit_token"] = secrets.token_urlsafe(32)
            columns = ", ".join(update_data.keys())
            placeholders = ", ".join([f":{k}" for k in update_data.keys()])
            db.execute(
                text(f"INSERT INTO discovery_documents ({columns}) VALUES ({placeholders})"),
                update_data
            )
            action = "created"
        
        db.commit()
        
        return {
            "status": "success",
            "action": action,
            "client_id": client_id,
            "fields_updated": len(update_data) - (2 if action == "created" else 1),  # Exclude client_id and edit_token from count
            "field_names": list(k for k in update_data.keys() if k not in ("client_id", "edit_token"))
        }
        
    except Exception as e:
        db.rollback()
        raise Exception(f"Failed to update discovery document: {str(e)}")
    finally:
        db.close()


@mcp.tool
def get_discovery_document(client_id: int) -> dict:
    """
    Fetches the discovery document for a client.
    
    Args:
        client_id (int): The client ID.
    
    Returns:
        dict: The discovery document data including all fields.
    """
    db = get_db_session()
    try:
        result = db.execute(
            text("SELECT * FROM discovery_documents WHERE client_id = :client_id"),
            {"client_id": client_id}
        ).fetchone()
        
        if not result:
            raise ValueError(f"Discovery document not found for client_id={client_id}")
        
        # Convert row to dictionary
        columns = result._mapping.keys()
        return {col: result._mapping[col] for col in columns}
    finally:
        db.close()


# =============================================================================
# STRATEGY SECTION MANAGEMENT
# =============================================================================

# Strategy section metadata - maps section keys to names
STRATEGY_SECTIONS = {
    "executive_summary": "Executive Summary",
    "section_1": "Strategic Foundation",
    "section_2": "Market & Competitive Analysis",
    "section_3": "Audience Intelligence",
    "section_4": "Value Proposition & Messaging",
    "section_5": "Setsail Services Overview",
    "section_6": "Google Ads Management",
    "section_7": "Social Media Management",
    "section_8": "SEO Services",
    "section_9": "Overall Performance Targets",
    "section_10": "Execution Timeline",
    "section_11": "Budget & Investment",
    "section_12": "Communication & Support",
    "section_13": "Success Indicators",
    "section_14": "Next Steps & Kickoff",
    "appendix_a": "Glossary of Terms",
    "appendix_b": "Key Contacts",
}

# SQL to create strategies table if it doesn't exist
STRATEGIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategies (
    id SERIAL PRIMARY KEY,
    client_id INTEGER NOT NULL,
    version_number INTEGER NOT NULL,
    executive_summary TEXT,
    section_1 TEXT,
    section_2 TEXT,
    section_3 TEXT,
    section_4 TEXT,
    section_5 TEXT,
    section_6 TEXT,
    section_7 TEXT,
    section_8 TEXT,
    section_9 TEXT,
    section_10 TEXT,
    section_11 TEXT,
    section_12 TEXT,
    section_13 TEXT,
    section_14 TEXT,
    appendix_a TEXT,
    appendix_b TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(client_id, version_number)
);
"""

def _ensure_strategies_table():
    """Ensure the strategies table exists."""
    if not _engine:
        return
    db = get_db_session()
    try:
        db.execute(text(STRATEGIES_TABLE_SQL))
        db.commit()
    except Exception as e:
        db.rollback()
        # Table might already exist, that's fine
    finally:
        db.close()

# Ensure table exists on module load
if _engine:
    _ensure_strategies_table()


def _get_strategy(client_id: int, version_number: int) -> dict | None:
    """Fetch a strategy by client_id and version_number."""
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                SELECT * FROM strategies 
                WHERE client_id = :client_id AND version_number = :version_number
            """),
            {"client_id": client_id, "version_number": version_number}
        ).fetchone()
        
        if not result:
            return None
        
        columns = result._mapping.keys()
        return {col: result._mapping[col] for col in columns}
    finally:
        db.close()


def _get_latest_strategy_version(client_id: int) -> int:
    """Get the latest version number for a client's strategy."""
    db = get_db_session()
    try:
        result = db.execute(
            text("""
                SELECT COALESCE(MAX(version_number), 0) as max_version
                FROM strategies WHERE client_id = :client_id
            """),
            {"client_id": client_id}
        ).fetchone()
        return result[0] if result else 0
    finally:
        db.close()


def _update_strategy_section(client_id: int, version_number: int, section_key: str, content: str) -> dict:
    """Update a single section of a strategy."""
    if section_key not in STRATEGY_SECTIONS:
        raise ValueError(f"Invalid section key: {section_key}. Valid keys: {list(STRATEGY_SECTIONS.keys())}")
    
    db = get_db_session()
    try:
        # Check if strategy exists
        existing = _get_strategy(client_id, version_number)
        if not existing:
            raise ValueError(f"Strategy not found for client_id={client_id}, version_number={version_number}")
        
        # Update the section
        db.execute(
            text(f"""
                UPDATE strategies 
                SET {section_key} = :content, updated_at = NOW()
                WHERE client_id = :client_id AND version_number = :version_number
            """),
            {"content": content, "client_id": client_id, "version_number": version_number}
        )
        db.commit()
        
        return {
            "status": "success",
            "client_id": client_id,
            "version_number": version_number,
            "section_key": section_key,
            "section_name": STRATEGY_SECTIONS[section_key],
            "content_length": len(content)
        }
    except Exception as e:
        db.rollback()
        raise
    finally:
        db.close()




def _assemble_full_strategy(strategy: dict) -> str:
    """Assemble all sections into a full strategy document."""
    parts = []
    
    # Add executive summary first
    if strategy.get("executive_summary"):
        parts.append(strategy["executive_summary"])
        parts.append("\n\n---\n\n")
    
    # Add numbered sections 1-14
    for i in range(1, 15):
        section_key = f"section_{i}"
        if strategy.get(section_key):
            parts.append(strategy[section_key])
            parts.append("\n\n---\n\n")
    
    # Add appendices
    if strategy.get("appendix_a"):
        parts.append(strategy["appendix_a"])
        parts.append("\n\n---\n\n")
    
    if strategy.get("appendix_b"):
        parts.append(strategy["appendix_b"])
    
    return "".join(parts).strip()


# =============================================================================
# STRATEGY MCP TOOLS
# =============================================================================

@mcp.tool
def readStrategy(client_id: int, version_number: int) -> dict:
    """
    Reads a complete strategy document for a client.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version number to read.
    
    Returns:
        dict: A dictionary containing the full assembled strategy and individual sections.
    """
    strategy = _get_strategy(client_id, version_number)
    
    if not strategy:
        raise ValueError(f"Strategy not found for client_id={client_id}, version_number={version_number}")
    
    # Assemble the full document
    full_document = _assemble_full_strategy(strategy)
    
    # Build sections dict with names
    sections = {}
    for key, name in STRATEGY_SECTIONS.items():
        sections[key] = {
            "name": name,
            "content": strategy.get(key) or ""
        }
    
    return {
        "client_id": client_id,
        "version_number": version_number,
        "created_at": str(strategy.get("created_at", "")),
        "updated_at": str(strategy.get("updated_at", "")),
        "full_document": full_document,
        "sections": sections
    }


@mcp.tool
def listStrategyVersions(client_id: int) -> dict:
    """
    Lists all strategy versions for a client.
    
    Args:
        client_id (int): The client ID.
    
    Returns:
        dict: A dictionary containing a list of versions with their metadata.
    """
    db = get_db_session()
    try:
        results = db.execute(
            text("""
                SELECT version_number, created_at, updated_at
                FROM strategies
                WHERE client_id = :client_id
                ORDER BY version_number DESC
            """),
            {"client_id": client_id}
        ).fetchall()
        
        versions = []
        for row in results:
            versions.append({
                "version_number": row[0],
                "created_at": str(row[1]) if row[1] else None,
                "updated_at": str(row[2]) if row[2] else None,
            })
        
        return {
            "client_id": client_id,
            "total_versions": len(versions),
            "versions": versions
        }
    finally:
        db.close()


@mcp.tool
def createStrategy(client_id: int, copy_from_version: Optional[int] = None) -> dict:
    """
    Creates a new strategy version for a client.
    
    Args:
        client_id (int): The client ID.
        copy_from_version (int, optional): If provided, copies content from this version.
            If not provided, creates a blank strategy.
    
    Returns:
        dict: A dictionary containing the new version number and status.
    """
    db = get_db_session()
    try:
        # Get the next version number
        current_max = _get_latest_strategy_version(client_id)
        new_version = current_max + 1
        
        # Prepare insert data
        insert_data = {
            "client_id": client_id,
            "version_number": new_version,
        }
        
        # If copying from an existing version, get that data
        if copy_from_version is not None:
            source = _get_strategy(client_id, copy_from_version)
            if not source:
                raise ValueError(f"Source strategy not found: client_id={client_id}, version={copy_from_version}")
            
            # Copy all section content
            for key in STRATEGY_SECTIONS.keys():
                insert_data[key] = source.get(key)
        
        # Build and execute insert
        columns = ", ".join(insert_data.keys())
        placeholders = ", ".join([f":{k}" for k in insert_data.keys()])
        
        db.execute(
            text(f"INSERT INTO strategies ({columns}) VALUES ({placeholders})"),
            insert_data
        )
        db.commit()
        
        return {
            "status": "success",
            "client_id": client_id,
            "version_number": new_version,
            "copied_from": copy_from_version,
            "message": f"Created strategy version {new_version}" + (f" (copied from v{copy_from_version})" if copy_from_version else " (blank)")
        }
    except Exception as e:
        db.rollback()
        raise Exception(f"Failed to create strategy: {str(e)}")
    finally:
        db.close()


# --- Edit tools for each section ---
# These tools take the content directly and write it to the database.
# The AI agent (The Cook) generates the content and calls these tools.

@mcp.tool
def editStrategyExecutiveSummary(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates the Executive Summary section of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "executive_summary", content)


@mcp.tool
def editStrategySection1(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 1: Strategic Foundation of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_1", content)


@mcp.tool
def editStrategySection2(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 2: Market & Competitive Analysis of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_2", content)


@mcp.tool
def editStrategySection3(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 3: Audience Intelligence of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_3", content)


@mcp.tool
def editStrategySection4(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 4: Value Proposition & Messaging of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_4", content)


@mcp.tool
def editStrategySection5(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 5: Setsail Services Overview of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_5", content)


@mcp.tool
def editStrategySection6(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 6: Google Ads Management of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_6", content)


@mcp.tool
def editStrategySection7(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 7: Social Media Management of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_7", content)


@mcp.tool
def editStrategySection8(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 8: SEO Services of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_8", content)


@mcp.tool
def editStrategySection9(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 9: Overall Performance Targets of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_9", content)


@mcp.tool
def editStrategySection10(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 10: Execution Timeline of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_10", content)


@mcp.tool
def editStrategySection11(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 11: Budget & Investment of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_11", content)


@mcp.tool
def editStrategySection12(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 12: Communication & Support of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_12", content)


@mcp.tool
def editStrategySection13(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 13: Success Indicators of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_13", content)


@mcp.tool
def editStrategySection14(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Section 14: Next Steps & Kickoff of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "section_14", content)


@mcp.tool
def editStrategyAppendixA(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Appendix A: Glossary of Terms of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "appendix_a", content)


@mcp.tool
def editStrategyAppendixB(client_id: int, version_number: int, content: str) -> dict:
    """
    Updates Appendix B: Key Contacts of a strategy.
    
    Args:
        client_id (int): The client ID.
        version_number (int): The strategy version to edit.
        content (str): The new content for this section (markdown format).
    
    Returns:
        dict: Status and the updated section content.
    """
    return _update_strategy_section(client_id, version_number, "appendix_b", content)


# --- DataForSEO Keywords Data (Google Ads) ---
KEYWORDS_DATA_BASE = "https://api.dataforseo.com/v3/keywords_data/google_ads"


def _dataforseo_keywords_post(endpoint: str, payload: list) -> dict:
    """POST to DataForSEO keywords_data/google_ads endpoint. Returns full JSON."""
    auth = _dataforseo_basic_auth()
    if not auth:
        raise ValueError("DATAFORSEO_USERNAME + DATAFORSEO_API_SECRET (or API_KEY) not set.")
    url = f"{KEYWORDS_DATA_BASE}/{endpoint}"
    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}
    r = requests.post(url, headers=headers, json=payload, timeout=120)
    r.raise_for_status()
    return r.json()


# --- DataForSEO Labs: Keyword Opportunities (competitors + gap keywords) ---
LABS_BASE = "https://api.dataforseo.com/v3/dataforseo_labs/google"
MAX_OPPORTUNITIES_COMPETITORS = 3
MAX_OPPORTUNITIES_KEYWORDS_PER_COMPETITOR = 250
MIN_OPPORTUNITIES_VOLUME = 10


def _dataforseo_labs_post(endpoint: str, payload: list) -> dict:
    auth = _dataforseo_basic_auth()
    if not auth:
        raise ValueError("DATAFORSEO_USERNAME + DATAFORSEO_API_SECRET (or API_KEY) not set.")
    url = f"{LABS_BASE}/{endpoint}"
    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()


def _fetch_competitors_domain(target: str, location_code: int = 2124, language_code: str = "en", limit: int = 50) -> list[str]:
    """Fetch organic competitors for target. Returns list of competitor domains (max 50 from API)."""
    payload = [{
        "target": _normalize_target_domain(target) or target,
        "location_code": location_code,
        "language_code": language_code,
        "limit": limit,
    }]
    data = _dataforseo_labs_post("competitors_domain/live", payload)
    domains = []
    for task in data.get("tasks") or []:
        for res in task.get("result") or []:
            items = res.get("items") or res.get("items_data") or (res.get("items_data") or {}).get("items") or []
            if isinstance(items, list):
                for it in items:
                    d = it.get("domain") or it.get("competitor")
                    if d and isinstance(d, str):
                        domains.append(d.strip().lower())
    # Dedupe, exclude target, return list
    target_norm = (_normalize_target_domain(target) or target).lower()
    seen = set()
    out = []
    for d in domains:
        if d == target_norm or d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out


def _fetch_domain_intersection_gap(
    competitor_domain: str,
    target_domain: str,
    location_code: int = 2124,
    language_code: str = "en",
    limit: int = 250,
    min_vol: int = 10,
) -> list[dict]:
    """Keywords competitor ranks for that target doesn't (intersections=false). Returns list of item dicts."""
    payload = [{
        "target1": competitor_domain,
        "target2": target_domain,
        "location_code": location_code,
        "language_code": language_code,
        "intersections": False,
        "item_types": ["organic"],
        "limit": limit,
        "filters": [["keyword_data.keyword_info.search_volume", ">", min_vol]],
        "order_by": ["keyword_data.keyword_info.search_volume,desc"],
    }]
    data = _dataforseo_labs_post("domain_intersection/live", payload)
    items = []
    for task in data.get("tasks") or []:
        for res in task.get("result") or []:
            raw = res.get("items") or res.get("items_data") or (res.get("items_data") or {}).get("items") or []
            if isinstance(raw, list):
                items.extend(raw)
    return items


def fetch_keyword_opportunities_impl(
    domain: str,
    location_code: int = 2124,
    language_code: str = "en",
    max_competitors: int = MAX_OPPORTUNITIES_COMPETITORS,
    gap_limit: int = MAX_OPPORTUNITIES_KEYWORDS_PER_COMPETITOR,
    min_vol: int = MIN_OPPORTUNITIES_VOLUME,
) -> dict:
    """
    Ahrefs-style keyword opportunities: get organic competitors, then gap keywords (they rank, we don't).
    Returns { "opportunities": [...], "competitors_used": [...], "error": optional }.
    Each opportunity: keyword, search_volume, cpc, competition, competitor_count, example_competitors, score.
    """
    target = _normalize_target_domain(domain) or domain.replace("https://", "").replace("http://", "").strip().lower()
    if not target:
        return {"opportunities": [], "competitors_used": [], "error": "Invalid or missing domain"}
    if not _dataforseo_basic_auth():
        return {"opportunities": [], "competitors_used": [], "error": "DATAFORSEO credentials not set"}

    max_competitors = min(max_competitors, MAX_OPPORTUNITIES_COMPETITORS)
    gap_limit = min(gap_limit, MAX_OPPORTUNITIES_KEYWORDS_PER_COMPETITOR)

    try:
        logger.info("[Keyword Opportunities] Fetching competitors for target=%s location=%s", target, location_code)
        competitors = _fetch_competitors_domain(target, location_code=location_code, language_code=language_code)
        competitors = [c for c in competitors if c != target][:max_competitors]
        if not competitors:
            logger.warning("[Keyword Opportunities] No competitors found for %s", target)
            return {"opportunities": [], "competitors_used": [], "error": None}

        logger.info("[Keyword Opportunities] Using competitors: %s", competitors)
        # Aggregate by keyword: keyword -> { max_vol, max_cpc, max_comp, competitors[] }
        agg: dict[str, dict] = {}
        for comp in competitors:
            try:
                items = _fetch_domain_intersection_gap(
                    comp, target,
                    location_code=location_code,
                    language_code=language_code,
                    limit=gap_limit,
                    min_vol=min_vol,
                )
            except Exception as e:
                logger.warning("[Keyword Opportunities] Gap fetch failed for %s: %s", comp, e)
                continue
            for it in items:
                kw = (it.get("keyword") or (it.get("keyword_data") or {}).get("keyword") or "").strip()
                if not kw:
                    continue
                ki = (it.get("keyword_data") or {}).get("keyword_info") or {}
                vol = int(ki.get("search_volume") or 0)
                cpc = float(ki.get("cpc") or 0)
                comp_val = float(ki.get("competition") or 0)
                if kw not in agg:
                    agg[kw] = {"search_volume": vol, "cpc": cpc, "competition": comp_val, "competitors": []}
                else:
                    agg[kw]["search_volume"] = max(agg[kw]["search_volume"], vol)
                    agg[kw]["cpc"] = max(agg[kw]["cpc"], cpc)
                    agg[kw]["competition"] = max(agg[kw]["competition"], comp_val)
                if comp not in agg[kw]["competitors"]:
                    agg[kw]["competitors"].append(comp)

        opportunities = []
        for kw, v in agg.items():
            score = v["search_volume"] * (1 + v["cpc"])
            opportunities.append({
                "keyword": kw,
                "search_volume": v["search_volume"],
                "cpc": round(v["cpc"], 4),
                "competition": round(v["competition"], 4),
                "competitor_count": len(v["competitors"]),
                "example_competitors": ",".join(v["competitors"][:5]),
                "score": round(score, 4),
            })
        opportunities.sort(key=lambda x: x["score"], reverse=True)
        logger.info("[Keyword Opportunities] Returning %d opportunities for %s", len(opportunities), target)
        return {"opportunities": opportunities, "competitors_used": competitors, "error": None}
    except Exception as e:
        logger.exception("[Keyword Opportunities] Failed")
        return {"opportunities": [], "competitors_used": [], "error": str(e)}


@mcp.custom_route("/keyword-opportunities", methods=["POST"])
async def keyword_opportunities_route(request):
    """POST with JSON { \"domain\": \"setsail.ca\", \"location_code\": 2124 }; returns opportunities JSON."""
    import asyncio
    from starlette.responses import JSONResponse

    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse({"error": f"Invalid JSON: {e}"}, status_code=400)
    domain = (body.get("domain") or body.get("target") or "").strip()
    if not domain:
        return JSONResponse({"error": "Missing domain or target"}, status_code=400)
    location_code = int(body.get("location_code", 2124))
    language_code = str(body.get("language_code", "en"))
    try:
        result = await asyncio.to_thread(
            fetch_keyword_opportunities_impl,
            domain,
            location_code=location_code,
            language_code=language_code,
            max_competitors=MAX_OPPORTUNITIES_COMPETITORS,
            gap_limit=MAX_OPPORTUNITIES_KEYWORDS_PER_COMPETITOR,
            min_vol=MIN_OPPORTUNITIES_VOLUME,
        )
    except Exception as e:
        logger.exception("[Keyword Opportunities] route failed")
        return JSONResponse({"error": str(e)}, status_code=500)
    if result.get("error") and not result.get("opportunities"):
        return JSONResponse({"error": result["error"]}, status_code=502)
    return JSONResponse(result)


if __name__ == "__main__":
    # Run the MCP server
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

