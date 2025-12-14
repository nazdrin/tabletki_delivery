from __future__ import annotations
import httpx

class TelegramNotifier:
    def __init__(self, bot_token: str | None, chat_ids: list[int], timeout_sec: float = 15.0):
        self.bot_token = bot_token
        self.chat_ids = chat_ids
        self.timeout_sec = timeout_sec

    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_ids)

    async def send(self, text: str) -> None:
        if not self.enabled():
            return

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        async with httpx.AsyncClient(timeout=self.timeout_sec) as client:
            for chat_id in self.chat_ids:
                try:
                    await client.post(url, json={"chat_id": chat_id, "text": text})
                except Exception:
                    # намеренно тихо: не валим основной процесс из-за телеги
                    pass