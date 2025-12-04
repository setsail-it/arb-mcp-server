import os
import base64
import time
import requests
from fastmcp import FastMCP
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
            raise Exception("DataForSEO API result has no items")
        
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
    Fetches client overview data from the client_contexts table.
    
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
                FROM client_contexts
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
    Fetches client writing rules from the client_contexts table.
    
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
                FROM client_contexts
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


if __name__ == "__main__":
    # Run the MCP server
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

