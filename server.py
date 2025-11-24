import os
import requests
from fastmcp import FastMCP

mcp = FastMCP("Keyword MCP Server")

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

if __name__ == "__main__":
    # Run the MCP server
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

