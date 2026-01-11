from __future__ import annotations

import asyncio
import logging
from typing import Optional

# Требует: pip install playwright
# И один раз: python -m playwright install
from playwright.async_api import async_playwright, Browser, Page


class BrowserFetcher:
    """
    Fetches HTML using a real browser (Playwright).
    Helps bypass CF challenges that block curl/httpx on some machines (often Windows).
    """

    def __init__(self, headless: bool = True, logger: Optional[logging.Logger] = None):
        self.headless = headless
        self.logger = logger or logging.getLogger(__name__)

        self._pw = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> None:
        if self._page is not None:
            return

        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=self.headless)

        context = await self._browser.new_context(
            locale="uk-UA",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        self._page = await context.new_page()

    async def fetch_html(self, url: str, wait_ms: int = 1500, timeout_ms: int = 60000) -> str:
        """
        Navigate to URL and return full HTML after minimal settle.
        wait_ms: extra wait after domcontentloaded
        """
        async with self._lock:
            await self._ensure()
            assert self._page is not None

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"[BrowserFetcher] goto: {url}")

            await self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if wait_ms > 0:
                await self._page.wait_for_timeout(wait_ms)

            html = await self._page.content()
            return html

    async def aclose(self) -> None:
        async with self._lock:
            try:
                if self._page is not None:
                    ctx = self._page.context
                    await self._page.close()
                    await ctx.close()
            except Exception:
                pass

            try:
                if self._browser is not None:
                    await self._browser.close()
            except Exception:
                pass

            try:
                if self._pw is not None:
                    await self._pw.stop()
            except Exception:
                pass

            self._pw = None
            self._browser = None
            self._page = None