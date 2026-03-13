import logging
import os
import requests
from typing import Optional

from fastmcp import FastMCP
from sqlalchemy import text

from db import db_session

logger = logging.getLogger(__name__)

sub = FastMCP("Client Context Tools")


def _query_general_context(client_id: int, columns: list[str]) -> dict | None:
    """Query specific columns from general_contexts for a client. Returns dict or None."""
    col_str = ", ".join(columns)
    with db_session() as db:
        result = db.execute(
            text(f"SELECT {col_str} FROM general_contexts WHERE client_id = :client_id"),
            {"client_id": client_id}
        ).fetchone()
        if not result:
            return None
        return {col: result[i] for i, col in enumerate(columns)}


def _backend_url() -> str:
    """Base URL of the backend (for triggering KES job)."""
    url = os.getenv("BACKEND_URL")
    if not url:
        raise ValueError("BACKEND_URL environment variable is required")
    return url.rstrip("/")


@sub.tool
def getClientOverview(client_id: int) -> dict:
    """
    Fetches client overview data from the general_contexts table (formerly client_contexts).

    Args:
        client_id (int): The client ID.

    Returns:
        dict: A dictionary containing domain, call_to_action, about, competitors,
              ideal_target_market, company_details, and social_links.
    """
    columns = ["domain", "call_to_action", "about", "competitors",
                "ideal_target_market", "company_details", "social_links"]
    result = _query_general_context(client_id, columns)
    if not result:
        raise ValueError(f"Client context not found for client_id={client_id}")
    return result


@sub.tool
def getClientWritingRules(client_id: int) -> dict:
    """
    Fetches client writing rules from the general_contexts table (formerly client_contexts).

    Args:
        client_id (int): The client ID.

    Returns:
        dict: A dictionary containing brand_pov, brand_safety, questionnaire,
              author_tone, and author_rules.
    """
    columns = ["brand_pov", "brand_safety", "questionnaire", "author_tone", "author_rules"]
    result = _query_general_context(client_id, columns)
    if not result:
        raise ValueError(f"Client context not found for client_id={client_id}")
    return result


@sub.tool
def get_kw_sitemap(client_id: int) -> dict:
    """
    Fetches the keyword enhanced sitemap for a client from general_contexts.
    The sitemap is stored as NDJSON (newline-delimited JSON) with one object per URL:
    url, page_type, primary_keyword, secondary_keywords, notes, etc.

    Args:
        client_id (int): The client ID.

    Returns:
        dict: keyword_enhanced_sitemap_json (str or null), keyword_enhanced_sitemap_generated_at (ISO str or null).
              If no context or no sitemap, json is null and generated_at is null.
    """
    with db_session() as db:
        result = db.execute(
            text("""
                SELECT keyword_enhanced_sitemap_json, keyword_enhanced_sitemap_generated_at
                FROM general_contexts
                WHERE client_id = :client_id
            """),
            {"client_id": client_id}
        ).fetchone()

        if not result:
            return {
                "keyword_enhanced_sitemap_json": None,
                "keyword_enhanced_sitemap_generated_at": None,
                "message": f"No general context found for client_id={client_id}. Run the pipeline first."
            }

        json_val = result[0]
        generated_at = result[1]
        return {
            "keyword_enhanced_sitemap_json": json_val,
            "keyword_enhanced_sitemap_generated_at": generated_at.isoformat() if generated_at else None,
            "message": "No keyword enhanced sitemap yet. Run Sitemap then Keyword Enhanced Sitemap for this client." if not (json_val and json_val.strip()) else None,
        }


@sub.tool
def update_kw_sitemap(client_id: int) -> dict:
    """
    Triggers an update (regeneration) of the keyword enhanced sitemap for a client.
    Calls the backend to run the Keyword Enhanced Sitemap job. The client must
    already have a sitemap in context (run Sitemap first if needed).
    Use get_kw_sitemap(client_id) to read the updated sitemap after the job completes.

    Args:
        client_id (int): The client ID whose keyword enhanced sitemap should be updated.

    Returns:
        dict: job_id (str), message (str). On failure, error (str) and optional status_code.
    """
    url = f"{_backend_url()}/clients/{client_id}/context/keyword-enhanced-sitemap/retry"
    try:
        resp = requests.post(url, timeout=30)
        data = resp.json() if resp.text else {}
        if not resp.ok:
            detail = data.get("detail")
            if isinstance(detail, list) and detail:
                detail = detail[0].get("msg", str(detail[0])) if isinstance(detail[0], dict) else str(detail[0])
            elif not isinstance(detail, str):
                detail = resp.text or f"HTTP {resp.status_code}"
            return {
                "job_id": "",
                "message": "Keyword enhanced sitemap update failed.",
                "error": detail,
                "status_code": resp.status_code,
            }
        job_id = data.get("job_id", "")
        return {
            "job_id": job_id,
            "message": f"Keyword enhanced sitemap update started for client_id={client_id}. Poll GET /clients/{client_id}/context/keyword-enhanced-sitemap/status for status, then use get_kw_sitemap({client_id}) to read the result.",
        }
    except requests.RequestException as e:
        logger.exception("update_kw_sitemap request failed")
        return {
            "job_id": "",
            "message": "Keyword enhanced sitemap update failed.",
            "error": str(e),
        }
