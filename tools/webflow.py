import logging
import os
import requests
from typing import Optional

from fastmcp import FastMCP
from sqlalchemy import text

from config import WEBFLOW_API_BASE
from db import db_session

logger = logging.getLogger(__name__)

sub = FastMCP("Webflow Tools")


def _webflow_headers() -> dict:
    token = (os.getenv("WEBFLOW_ACCESS_TOKEN") or "").strip()
    if not token:
        raise ValueError("WEBFLOW_ACCESS_TOKEN is not set.")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


@sub.tool
def get_webflow_pages(site_id: Optional[str] = None, client_id: Optional[int] = None) -> dict:
    """
    Returns the stored Webflow pages for a site. Pages are stored when a user runs
    "Get all pages" in the Page Updater (AI panel). Provide either site_id (Webflow site ID)
    or client_id (looked up from the clients table via client.site_id).

    Args:
        site_id (str, optional): Webflow site ID.
        client_id (int, optional): Client ID; site_id is looked up from clients.site_id.

    Returns:
        dict: site_id (str), pages (list of {page_id, title, slug, published_path, collection_id, type}).
              On error, error (str) and optionally message.
    """
    if not site_id and client_id is None:
        return {"site_id": "", "pages": [], "error": "Provide either site_id or client_id."}
    resolved_site_id = site_id
    if not resolved_site_id and client_id is not None:
        with db_session() as db:
            row = db.execute(
                text("SELECT site_id FROM clients WHERE id = :client_id"),
                {"client_id": client_id},
            ).fetchone()
            if not row or not row[0]:
                return {
                    "site_id": "",
                    "pages": [],
                    "error": f"No site_id found for client_id={client_id}. Confirm a site in Page Updater first.",
                }
            resolved_site_id = row[0]
    if not resolved_site_id:
        return {"site_id": "", "pages": [], "error": "Could not resolve site_id."}
    with db_session() as db:
        try:
            rows = db.execute(
                text("""
                    SELECT page_id, title, slug, published_path, collection_id, type
                    FROM webflow_pages WHERE site_id = :site_id ORDER BY id
                """),
                {"site_id": resolved_site_id},
            ).fetchall()
            pages = [
                {
                    "page_id": r[0],
                    "title": r[1],
                    "slug": r[2],
                    "published_path": r[3],
                    "collection_id": r[4],
                    "type": r[5],
                }
                for r in rows
            ]
            return {"site_id": resolved_site_id, "pages": pages}
        except Exception as e:
            logger.exception("get_webflow_pages failed")
            return {
                "site_id": resolved_site_id or "",
                "pages": [],
                "error": str(e),
                "message": "Ensure webflow_pages table exists (run setsail-ai-panel-BE migrations).",
            }


@sub.tool
def get_webflow_page(page_id: str) -> dict:
    """
    Fetches a single Webflow page by ID. Returns id, title, seo, openGraph, publishedPath.

    Args:
        page_id (str): The Webflow page ID.

    Returns:
        dict: id, title, seo, openGraph, publishedPath. On error, error (str).
    """
    try:
        headers = _webflow_headers()
        r = requests.get(
            f"{WEBFLOW_API_BASE}/pages/{page_id}",
            headers=headers,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "id": data.get("id"),
            "title": data.get("title"),
            "seo": data.get("seo"),
            "openGraph": data.get("openGraph"),
            "publishedPath": data.get("publishedPath"),
        }
    except requests.RequestException as e:
        return {"error": str(e), "id": None, "title": None, "seo": None, "openGraph": None, "publishedPath": None}
    except ValueError as e:
        return {"error": str(e), "id": None, "title": None, "seo": None, "openGraph": None, "publishedPath": None}


@sub.tool
def update_webflow_page(
    page_id: str,
    seo: Optional[dict] = None,
    open_graph: Optional[dict] = None,
) -> dict:
    """
    Updates a Webflow page's SEO and/or Open Graph metadata.
    Pass seo (e.g. {"title": "...", "description": "..."}) and/or open_graph
    (e.g. {"title": "...", "description": "...", "titleCopied": false, "descriptionCopied": false}).
    Only provided keys are sent; omit a key to leave it unchanged.

    Args:
        page_id (str): The Webflow page ID.
        seo (dict, optional): SEO object with title, description, etc.
        open_graph (dict, optional): Open Graph object with title, description, titleCopied, descriptionCopied.

    Returns:
        dict: Updated page snippet or error.
    """
    try:
        headers = _webflow_headers()
        headers["Content-Type"] = "application/json"
        body = {}
        if seo is not None:
            body["seo"] = seo
        if open_graph is not None:
            body["openGraph"] = open_graph
        if not body:
            return {"error": "Provide at least one of seo or open_graph."}
        r = requests.put(
            f"{WEBFLOW_API_BASE}/pages/{page_id}",
            headers=headers,
            json=body,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        return {
            "id": data.get("id"),
            "title": data.get("title"),
            "seo": data.get("seo"),
            "openGraph": data.get("openGraph"),
            "publishedPath": data.get("publishedPath"),
        }
    except requests.RequestException as e:
        return {"error": str(e)}
    except ValueError as e:
        return {"error": str(e)}


@sub.tool
def publish_webflow_site(site_id: str) -> dict:
    """
    Publishes the Webflow site (pushes draft changes live). Use after updating pages.
    Requires WEBFLOW_ACCESS_TOKEN.

    Args:
        site_id (str): The Webflow site ID.

    Returns:
        dict: Result from Webflow or error.
    """
    try:
        headers = _webflow_headers()
        r = requests.post(
            f"{WEBFLOW_API_BASE}/sites/{site_id}/publish",
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        return r.json() if r.text else {"status": "ok"}
    except requests.RequestException as e:
        return {"error": str(e)}
    except ValueError as e:
        return {"error": str(e)}
