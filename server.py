import os
import requests
from fastmcp import FastMCP

mcp = FastMCP("Keyword MCP Server")

@mcp.tool
def get_search_volume(keyword: str, location_code: int = 2840, language_code: str = "en") -> dict:
    """
    Fetches search volume data for a given keyword using DataForSEO API.

    Args:
        keyword (str): The keyword to fetch data for.
        location_code (int): The location code (default is 2840 for the United States).
        language_code (str): The language code (default is "en" for English).

    Returns:
        dict: A dictionary containing keyword data, including search volume, competition, competition index, CPC, monthly searches, and the raw API response.
    """
    # DataForSEO API endpoint
    url = "https://api.dataforseo.com/v3/keywords_data/google_ads/search_volume/live"

    # Retrieve DataForSEO credentials from environment variables
    api_key = os.getenv("DATAFORSEO_API_KEY")  # Login email
    api_secret = os.getenv("DATAFORSEO_API_SECRET")  # API password

    if not api_key or not api_secret:
        raise ValueError("DataForSEO credentials are not set in environment variables.")

    # Prepare the payload
    payload = [
        {
            "location_code": location_code,
            "language_code": language_code,
            "keywords": [keyword]
        }
    ]

    # Make the POST request with HTTP Basic Auth
    response = requests.post(url, auth=(api_key, api_secret), json=payload)

    # Check for successful response
    if response.status_code != 200:
        raise Exception(f"DataForSEO API request failed with status code {response.status_code}: {response.text}")

    # Parse the response
    data = response.json()

    # Extract relevant data from tasks[0].result[0]
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
        keyword_data = {
            "keyword": result.get('keyword'),
            "search_volume": result.get('search_volume'),
            "competition": result.get('competition'),
            "competition_index": result.get('competition_index'),
            "cpc": result.get('cpc'),
            "monthly_searches": result.get('monthly_searches'),
            "raw": result
        }
        return keyword_data
    except (KeyError, IndexError, TypeError) as e:
        raise Exception(f"Unexpected response structure from DataForSEO API: {e}. Response: {data}")

if __name__ == "__main__":
    # Run the MCP server
    mcp.run(transport="streamable-http", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))

