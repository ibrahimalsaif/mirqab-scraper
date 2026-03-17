"""
Core scraping logic: launch a headless browser, navigate to a public Power BI
dashboard, and intercept every data-bearing API call the page makes.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from playwright.sync_api import sync_playwright, Page, Response

from . import config

logger = logging.getLogger(__name__)


@dataclass
class CapturedExchange:
    """One request/response pair intercepted from the network."""
    url: str
    request_payload: dict | None
    response_body: dict | None
    status: int = 0


@dataclass
class ScrapeResult:
    """Everything we captured during one scraping session."""
    exchanges: list[CapturedExchange] = field(default_factory=list)


def _matches_intercept_patterns(url: str) -> bool:
    lower = url.lower()
    return any(pat in lower for pat in config.INTERCEPT_PATTERNS)


def _try_parse_json(raw: str | bytes | None) -> dict | None:
    if raw is None:
        return None
    try:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None


def _capture_response(exchanges: list[CapturedExchange], response: Response) -> None:
    url = response.url
    if not _matches_intercept_patterns(url):
        return

    try:
        body = response.body()
    except Exception:
        body = None

    request_payload = _try_parse_json(response.request.post_data)
    response_json = _try_parse_json(body)

    if response_json is None:
        logger.debug("Skipping non-JSON response from %s", url)
        return

    exchanges.append(CapturedExchange(
        url=url,
        request_payload=request_payload,
        response_body=response_json,
        status=response.status,
    ))
    logger.info("Captured response from %s (status %d)", url, response.status)


def _force_click(page: Page, element, index: int) -> bool:
    try:
        element.scroll_into_view_if_needed(timeout=3_000)
    except Exception:
        pass

    for strategy in [
        lambda: element.click(timeout=5_000),
        lambda: element.click(force=True, timeout=5_000),
        lambda: page.evaluate("el => el.click()", element),
    ]:
        try:
            strategy()
            return True
        except Exception:
            pass

    try:
        box = element.bounding_box()
        if box:
            page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
            return True
    except Exception:
        pass

    return False


def _click_tabs(page: Page) -> None:
    try:
        tab_selectors = [
            'div[role="tab"]',
            'button[role="tab"]',
            ".navigation-wrapper button",
            ".tabStrip button",
        ]
        for selector in tab_selectors:
            tabs = page.query_selector_all(selector)
            if not tabs:
                continue
            logger.info("Found %d tab(s) using selector '%s'", len(tabs), selector)
            for i, tab in enumerate(tabs):
                try:
                    if _force_click(page, tab, i):
                        try:
                            page.wait_for_load_state("networkidle", timeout=10_000)
                        except Exception:
                            pass
                        page.wait_for_timeout(config.EXTRA_SETTLE_MS)
                        logger.info("Clicked tab %d/%d", i + 1, len(tabs))
                    else:
                        logger.warning("All click strategies failed for tab %d", i + 1)
                except Exception as exc:
                    logger.warning("Could not click tab %d: %s", i + 1, exc)
            break
    except Exception as exc:
        logger.warning("Tab-clicking phase failed: %s", exc)


def scrape(url: str | None = None, click_through_tabs: bool = True) -> ScrapeResult:
    """
    Launch a headless browser, navigate to the Power BI dashboard, and
    return every data exchange captured.
    """
    url = url or config.DASHBOARD_URL
    result = ScrapeResult()

    logger.info("Launching browser (headless=%s)", config.HEADLESS)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=config.HEADLESS)
        try:
            context = browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            )
            page = context.new_page()
            page.on("response", lambda resp: _capture_response(result.exchanges, resp))

            logger.info("Navigating to %s", url)
            try:
                page.goto(url, wait_until="networkidle", timeout=config.PAGE_LOAD_TIMEOUT_MS)
            except Exception:
                logger.warning("networkidle timed out — falling back to domcontentloaded")
                page.goto(url, wait_until="domcontentloaded", timeout=config.PAGE_LOAD_TIMEOUT_MS)
                page.wait_for_timeout(config.EXTRA_SETTLE_MS)

            page.wait_for_timeout(config.EXTRA_SETTLE_MS)
            logger.info("Initial load done — %d exchange(s) so far", len(result.exchanges))

            if click_through_tabs:
                _click_tabs(page)

        finally:
            logger.info("Closing browser — total: %d exchange(s)", len(result.exchanges))
            browser.close()

    return result
