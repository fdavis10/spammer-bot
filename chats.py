import re
import asyncio
from pathlib import Path

from openpyxl import load_workbook
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from core import SESSIONS_DIR, load_config, is_account_authorized, add_chat_links, get_chat_links

LINK_PATTERN = re.compile(
    r'(?:https?://)?(?:t\.me|telegram\.me|telegram\.dog)/([a-zA-Z0-9_+/-]+)',
    re.IGNORECASE
)


def extract_links_from_xlsx(filepath: str) -> list[str]:
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {filepath}")
    links = set()
    wb = load_workbook(path, data_only=True)
    for sheet in wb.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                val = cell.value
                if val is not None:
                    for m in LINK_PATTERN.finditer(str(val)):
                        part = m.group(1).strip("/")
                        links.add(f"t.me/{part}")
                if hasattr(cell, "hyperlink") and cell.hyperlink:
                    href = str(getattr(cell.hyperlink, "target", "") or "")
                    for m in LINK_PATTERN.finditer(href):
                        part = m.group(1).strip("/")
                        links.add(f"t.me/{part}")
    return list(links)


def _parse_link(link: str) -> tuple[str | None, str | None]:
    link = str(link).strip()
    m = re.search(r't\.me/joinchat/([a-zA-Z0-9_-]+)', link, re.I)
    if m:
        return "invite", m.group(1)
    m = re.search(r't\.me/([a-zA-Z0-9_+-]+)', link, re.I)
    if m:
        return "username", m.group(1)
    m = re.search(r'telegram\.me/joinchat/([a-zA-Z0-9_-]+)', link, re.I)
    if m:
        return "invite", m.group(1)
    m = re.search(r'telegram\.me/([a-zA-Z0-9_+-]+)', link, re.I)
    if m:
        return "username", m.group(1)
    return None, None


async def join_by_link(client: TelegramClient, link: str) -> tuple[bool, str]:
    link_type, value = _parse_link(link)
    if not value:
        return False, "Неверный формат ссылки"
    try:
        if link_type == "invite":
            await client(ImportChatInviteRequest(value))
        else:
            await client(JoinChannelRequest(value))
        return True, "OK"
    except Exception as e:
        return False, str(e)


async def join_links_from_account(
    client: TelegramClient, phone: str, links: list[str], on_progress=None
) -> dict:
    joined = 0
    failed = 0
    errors = []
    for link in links:
        success, msg = await join_by_link(client, link)
        if success:
            joined += 1
        else:
            failed += 1
            errors.append((link, msg))
        if on_progress:
            on_progress(joined, failed)
        await asyncio.sleep(1)
    return {"joined": joined, "failed": failed, "errors": errors}


async def run_join_all_links(links: list[str], on_progress=None) -> list[dict]:
    cfg = load_config()
    if not cfg or not cfg.get("api_id") or not cfg.get("api_hash"):
        raise ValueError("Не настроены api_id и api_hash")

    accounts = [a for a in cfg["accounts"] if is_account_authorized(a["phone"])]
    if not accounts:
        raise ValueError("Нет авторизованных аккаунтов")

    results = []
    for acc in accounts:
        phone = acc["phone"]
        session_path = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
        client = TelegramClient(str(session_path), cfg["api_id"], cfg["api_hash"])
        try:
            await client.connect()
            if not await client.is_user_authorized():
                results.append({"phone": phone, "joined": 0, "failed": len(links), "errors": [("", "Не авторизован")]})
                continue
            def acc_progress(joined, failed):
                if on_progress:
                    total_j = sum(r.get("joined", 0) for r in results) + joined
                    total_f = sum(r.get("failed", 0) for r in results) + failed
                    on_progress(total_j, total_f)

            r = await join_links_from_account(client, phone, links, on_progress=acc_progress)
            results.append({"phone": phone, **r})
        except Exception as e:
            results.append({"phone": phone, "joined": 0, "failed": len(links), "errors": [("", str(e))]})
        finally:
            await client.disconnect()
        await asyncio.sleep(2)
    return results
