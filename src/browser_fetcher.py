from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Optional
import random
import re

from playwright.async_api import async_playwright, Browser, BrowserContext, Page


class BrowserFetcher:
    """
    Fetches HTML using a real browser (Playwright).
    Helps bypass CF challenges that block curl/httpx on some machines (often Windows).
    """

    def __init__(
        self,
        headless: bool = True,
        timeout_sec: float = 60.0,
        extra_delay_sec: float = 1.5,
        profile_dir: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.headless = headless
        self.timeout_sec = float(timeout_sec)
        self.extra_delay_sec = float(extra_delay_sec)
        self.logger = logger or logging.getLogger(__name__)
        self.profile_dir = profile_dir

        self._pw = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._lock = asyncio.Lock()

        # Manual Cloudflare/Turnstile wait (NOT a bypass).
        # When challenge is shown, we pause and let a human solve it in the opened browser window,
        # then continue using the same persistent session (cookies/storage).
        # Env:
        #   COMPETITORS_TURNSTILE_MANUAL_WAIT=1 (default)
        #   COMPETITORS_TURNSTILE_MAX_WAIT_SEC=600 (default)
        self._turnstile_manual_wait = os.getenv("COMPETITORS_TURNSTILE_MANUAL_WAIT", "1").strip().lower() in (
            "1", "true", "yes", "y", "on"
        )
        try:
            self._turnstile_max_wait_sec = float(os.getenv("COMPETITORS_TURNSTILE_MAX_WAIT_SEC", "600"))
        except Exception:
            self._turnstile_max_wait_sec = 600.0

    def _looks_like_cf_challenge(self, html: str) -> bool:
        """Heuristic: detect Cloudflare challenge/turnstile pages."""
        if not html:
            return False
        h = html.lower()
        markers = (
            "cf-mitigated",
            "/cdn-cgi/",
            "turnstile",
            "challenge",
            "please wait",
            "just a moment",
            "трохи зачекайте",
            "ви людина",
        )
        return any(m in h for m in markers)


    async def _wait_for_human_verification(self, url: str, timeout_ms: int) -> None:
        """
        Pause on CF/Turnstile and wait until a human solves it.
        Does NOT solve automatically. Keeps the same page/session.
        """
        if not getattr(self, "_turnstile_manual_wait", True):
            return

        if self._page is None:
            return

        # Bring window to front (best-effort).
        try:
            await self._page.bring_to_front()
        except Exception:
            pass

        max_wait_sec = max(getattr(self, "_turnstile_max_wait_sec", 600.0), 1.0)
        deadline = asyncio.get_event_loop().time() + max_wait_sec

        self.logger.warning(
            "FETCH=TURNSTILE_WAIT url=%s (solve verification in browser window; max_wait=%.0fs)",
            url,
            max_wait_sec,
        )

        # If running in an interactive terminal, allow ENTER to continue after user solved it.
        is_tty = False
        try:
            is_tty = bool(getattr(os.sys.stdin, "isatty", lambda: False)())
        except Exception:
            is_tty = False

        async def _wait_enter() -> None:
            try:
                await asyncio.to_thread(
                    input,
                    "\n[TURNSTILE] Solve verification in browser, then press ENTER to continue... ",
                )
            except Exception:
                return

        enter_task: Optional[asyncio.Task] = None
        if is_tty:
            enter_task = asyncio.create_task(_wait_enter())

        try:
            while True:
                # If operator pressed ENTER, try a reload and continue checking.
                if enter_task is not None and enter_task.done():
                    try:
                        await self._page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
                    except Exception:
                        pass
                    # Reset task so we don't keep reloading every loop.
                    enter_task = None

                html = await self._page.content()
                if not self._looks_like_cf_challenge(html):
                    break

                if asyncio.get_event_loop().time() >= deadline:
                    raise TimeoutError("Turnstile/CF verification not solved within the configured timeout")

                # Idle while user interacts.
                await self._page.wait_for_timeout(1000)

            # Final guard: if still a challenge, keep waiting until timeout.
            html_final = await self._page.content()
            if self._looks_like_cf_challenge(html_final):
                raise TimeoutError("Turnstile/CF verification still present after manual confirmation")

            # After verification: stabilize and reload once.
            try:
                await self._page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 15000))
            except Exception:
                pass
            try:
                await self._page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception:
                pass

            self.logger.info("FETCH=BROWSER (after human verification) url=%s", url)
        finally:
            if enter_task is not None and not enter_task.done():
                enter_task.cancel()

    async def _ensure(self) -> None:
        if self._page is not None:
            return

        self._pw = await async_playwright().start()

        # If profile_dir is provided, use a persistent context (keeps cookies/storage).
        # This is often required on Windows when Cloudflare challenges block http clients.
        if self.profile_dir:
            profile_path = Path(self.profile_dir).expanduser()
            # Make relative paths stable: relative to current working dir (project root when running `python -m ...`).
            if not profile_path.is_absolute():
                profile_path = (Path.cwd() / profile_path).resolve()
            profile_path.mkdir(parents=True, exist_ok=True)

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"[BrowserFetcher] persistent profile_dir: {profile_path} (headless={self.headless})")

            self._context = await self._pw.chromium.launch_persistent_context(
                user_data_dir=str(profile_path),
                headless=self.headless,
                locale="uk-UA",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
            )

            # Reuse first page if any, otherwise open a new one.
            pages = self._context.pages
            self._page = pages[0] if pages else await self._context.new_page()
            return

        # Non-persistent mode (fresh context each run)
        self._browser = await self._pw.chromium.launch(headless=self.headless)
        self._context = await self._browser.new_context(
            locale="uk-UA",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        self._page = await self._context.new_page()

    async def fetch_html(self, url: str, wait_ms: Optional[int] = None, timeout_ms: Optional[int] = None) -> str:
        """
        Navigate to URL and return full HTML after minimal settle.
        If wait_ms/timeout_ms not provided - use defaults from init().
        """
        if wait_ms is None:
            wait_ms = int(self.extra_delay_sec * 1000)
        if timeout_ms is None:
            timeout_ms = int(self.timeout_sec * 1000)

        async with self._lock:
            await self._ensure()
            assert self._page is not None

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"[BrowserFetcher] goto: {url} (timeout_ms={timeout_ms}, wait_ms={wait_ms})")

            # Navigate and let the page settle.
            await self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Try to reach a more stable state if possible.
            try:
                await self._page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 15000))
            except Exception:
                pass

            if wait_ms > 0:
                await self._page.wait_for_timeout(wait_ms)

            html = await self._page.content()

            # If Cloudflare challenge is detected, give it some extra time to complete (non-interactive).
            # This helps in cases where the first HTML is the "Please wait" page that later redirects.
            if self._looks_like_cf_challenge(html):
                if self.logger.isEnabledFor(logging.DEBUG):
                    self.logger.debug("[BrowserFetcher] CF/Turnstile challenge detected; waiting and re-checking...")

                for attempt in range(1, 4):
                    extra = int(random.uniform(6.0, 12.0) * 1000)
                    await self._page.wait_for_timeout(extra)
                    try:
                        await self._page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 15000))
                    except Exception:
                        pass

                    html2 = await self._page.content()
                    if not self._looks_like_cf_challenge(html2):
                        html = html2
                        break

                    # Sometimes a reload helps after CF sets cookies.
                    try:
                        await self._page.reload(wait_until="domcontentloaded", timeout=timeout_ms)
                    except Exception:
                        pass

                    html = html2
                    if self.logger.isEnabledFor(logging.DEBUG):
                        self.logger.debug(f"[BrowserFetcher] still looks like challenge (attempt {attempt}/3)")

                # If still a challenge after passive waits -> stop and wait for manual verification.
                if self._looks_like_cf_challenge(html):
                    await self._wait_for_human_verification(url=url, timeout_ms=timeout_ms)
                    html = await self._page.content()

            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"[BrowserFetcher] fetched html via BROWSER: {len(html)} bytes")
            return html

    async def get_html(self, url: str, wait_ms: Optional[int] = None, timeout_ms: Optional[int] = None) -> str:
        """Backward-compatible alias for older callers."""
        return await self.fetch_html(url=url, wait_ms=wait_ms, timeout_ms=timeout_ms)

    async def aclose(self) -> None:
        async with self._lock:
            try:
                # Close page and context (persistent or not)
                if self._page is not None:
                    await self._page.close()
            except Exception:
                pass

            try:
                if self._context is not None:
                    await self._context.close()
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
            self._context = None
            self._page = None