import os
import requests
from fastmcp import FastMCP
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

mcp = FastMCP("Keyword MCP Server")

# Database setup - create engine once
_database_url = os.getenv("DATABASE_URL")
_engine = None
_SessionLocal = None

if _database_url:
    _engine = create_engine(_database_url)
    _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=_engine)

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


if __name__ == "__main__":
    # Run the MCP server
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

