"""
Deprecated: AmazonAuth is replaced by BrowserSession.
This module exists for backward compatibility with batch_daily_scraper.py and main.py.
Use src.browser_session.BrowserSession instead.
"""

from src.browser_session import BrowserSession


class AmazonAuth:
    """Backward-compatible wrapper around BrowserSession."""

    def __init__(self):
        self._session = BrowserSession()

    async def login_and_get_context(self):
        await self._session.start()
        await self._session.login()
        return self._session._context

    async def new_page(self):
        return await self._session._context.new_page()

    async def close(self):
        await self._session.close()
