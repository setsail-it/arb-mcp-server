#!/usr/bin/env python3
"""
Single test: crawl setsail.ca via DataForSEO On-Page, save sitemap.txt and sitemap.xml, report polling time.
Run from mcp-server with .env loaded (DATAFORSEO_USERNAME + DATAFORSEO_API_SECRET).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from server import fetch_site_pages_dataforseo_impl

TARGET = "https://www.setsail.ca"
MAX_CRAWL = 10000
MAX_WAIT = 600
POLL_INTERVAL = 5
OUT_DIR = os.path.dirname(os.path.abspath(__file__))
SITEMAP_TXT = os.path.join(OUT_DIR, "sitemap.txt")
SITEMAP_XML = os.path.join(OUT_DIR, "sitemap.xml")


def main():
    if not os.getenv("DATAFORSEO_USERNAME") and not os.getenv("DATAFORSEO_API_KEY"):
        print("Set DATAFORSEO_USERNAME and DATAFORSEO_API_SECRET (or DATAFORSEO_API_KEY) in .env")
        sys.exit(1)

    print(f"Target: {TARGET}")
    print(f"max_crawl_pages={MAX_CRAWL}, max_wait={MAX_WAIT}s, poll_interval={POLL_INTERVAL}s")
    print("STEP 1: Creating crawl task...")
    result = fetch_site_pages_dataforseo_impl(
        TARGET,
        max_crawl_pages=MAX_CRAWL,
        max_wait_seconds=MAX_WAIT,
        poll_interval_seconds=POLL_INTERVAL,
        save_sitemap_txt=SITEMAP_TXT,
        save_sitemap_xml=SITEMAP_XML,
    )

    err = result.get("error")
    count = result.get("total_count", 0)
    task_id = result.get("task_id", "")
    poll_seconds = result.get("poll_seconds_waited", 0)

    if err:
        print(f"Error: {err}")
        print(f"task_id: {task_id}")
        print(f"Time spent polling: {poll_seconds}s")
        sys.exit(1)

    print(f"task_id: {task_id}")
    print(f"Total URLs: {count}")
    print(f"Time spent polling (until task ready): {poll_seconds}s")
    print(f"Saved: {SITEMAP_TXT}")
    print(f"Saved: {SITEMAP_XML}")
    if result.get("urls"):
        for u in result["urls"][:5]:
            print(f"  {u}")
        if count > 5:
            print(f"  ... and {count - 5} more")
    print("Done.")


if __name__ == "__main__":
    main()
