"""
Core scraping logic: launch a headless browser, navigate to a public Power BI
dashboard, and intercept every data-bearing API call the page makes.

Power BI dashboards are single-page apps that fetch data through XHR calls to
endpoints containing "querydata" or "public/reports".  Each visual on the
dashboard fires its own request, so a single page typically produces many
request/response pairs.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from playwright.sync_api import sync_playwright, Page, Response

import config

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
    """Callback attached to the page's 'response' event."""
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

    exchange = CapturedExchange(
        url=url,
        request_payload=request_payload,
        response_body=response_json,
        status=response.status,
    )
    exchanges.append(exchange)
    logger.info("Captured response from %s (status %d)", url, response.status)


def _force_click(page: Page, element, index: int) -> bool:
    """Try multiple strategies to click an element that may not be visible."""
    try:
        element.scroll_into_view_if_needed(timeout=3_000)
    except Exception:
        pass

    try:
        element.click(timeout=5_000)
        return True
    except Exception:
        pass

    try:
        element.click(force=True, timeout=5_000)
        return True
    except Exception:
        pass

    try:
        page.evaluate("el => el.click()", element)
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
    """
    Attempt to click through report tabs/pages to trigger additional data
    requests.  Power BI renders page tabs inside elements whose
    ``role="tab"`` attribute or class names hint at navigation.
    """
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
            break  # only use the first selector that finds tabs
    except Exception as exc:
        logger.warning("Tab-clicking phase failed: %s", exc)


def scrape(
    url: str | None = None,
    click_through_tabs: bool = True,
) -> ScrapeResult:
    """
    Main entry point.  Returns every Power BI data exchange captured while
    browsing the dashboard.

    Parameters
    ----------
    url : str, optional
        Override for the dashboard URL (defaults to ``config.DASHBOARD_URL``).
    click_through_tabs : bool
        If *True*, try to discover and click tabs on the report to capture
        data from every page.
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

            page.on(
                "response",
                lambda resp: _capture_response(result.exchanges, resp),
            )

            logger.info("Navigating to %s", url)
            try:
                page.goto(url, wait_until="networkidle", timeout=config.PAGE_LOAD_TIMEOUT_MS)
            except Exception:
                # networkidle can hang on dashboards with persistent connections;
                # fall back to domcontentloaded and wait manually.
                logger.warning("networkidle timed out — falling back to domcontentloaded")
                page.goto(url, wait_until="domcontentloaded", timeout=config.PAGE_LOAD_TIMEOUT_MS)
                page.wait_for_timeout(config.EXTRA_SETTLE_MS)

            # Give any lazy-loaded visuals time to fire their requests.
            page.wait_for_timeout(config.EXTRA_SETTLE_MS)
            logger.info(
                "Initial load done — captured %d exchange(s) so far",
                len(result.exchanges),
            )

            if click_through_tabs:
                _click_tabs(page)

        finally:
            logger.info("Closing browser — total exchanges captured: %d", len(result.exchanges))
            browser.close()

    return result
