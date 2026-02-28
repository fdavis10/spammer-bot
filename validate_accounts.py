import asyncio
import re
from typing import Callable, Optional

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from broadcast import create_client
from core import SESSIONS_DIR, load_config, is_account_authorized


def _parse_link(link: str) -> tuple[str | None, str | None]:
    link = str(link).strip()
    m = re.search(r't\.me/joinchat/([a-zA-Z0-9_-]+)', link, re.I)
    if m:
        return "invite", m.group(1)
    m = re.search(r't\.me/([a-zA-Z0-9_+-]+)', link, re.I)
    if m:
        return "username", m.group(1)
    return None, None


async def _join_group(client: TelegramClient, link: str) -> tuple[bool, str, object]:
    link_type, value = _parse_link(link)
    if not value:
        return False, "Неверный формат ссылки", None
    try:
        if link_type == "invite":
            result = await client(ImportChatInviteRequest(value))
            entity = getattr(result, "chats", [None])[0] if hasattr(result, "chats") and result.chats else None
            if not entity:
                entity = await client.get_entity(link)
        else:
            await client(JoinChannelRequest(value))
            entity = await client.get_entity(value)
        return True, "OK", entity
    except Exception as e:
        return False, str(e), None


async def _leave_group(client: TelegramClient, entity) -> tuple[bool, str]:
    try:
        from telethon.tl.types import Channel
        from telethon.tl.functions.messages import DeleteChatUserRequest
        if isinstance(entity, Channel):
            await client(LeaveChannelRequest(entity))
        else:
            me = await client.get_input_entity("me")
            await client(DeleteChatUserRequest(chat_id=entity.id, user_id=me))
        return True, "OK"
    except Exception as e:
        return False, str(e)


async def validate_one_account(
    phone: str,
    test_link: str,
    test_message: str,
    api_id: int,
    api_hash: str,
    proxy_cfg=None,
) -> tuple[bool, str]:
    session_path = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
    client = create_client(session_path, api_id, api_hash, proxy_cfg)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            return False, "Не авторизован"
        ok, msg, entity = await _join_group(client, test_link)
        if not ok:
            return False, f"Вступление: {msg}"
        try:
            await client.send_message(entity, test_message or "Тест")
        except Exception as e:
            await _leave_group(client, entity)
            return False, f"Отправка: {e}"
        ok_leave, msg_leave = await _leave_group(client, entity)
        if not ok_leave:
            return True, "OK (не удалось выйти)"
        return True, "OK"
    except Exception as e:
        return False, str(e)
    finally:
        await client.disconnect()


async def validate_all_accounts(
    test_link: str,
    test_message: str,
    on_progress: Optional[Callable[[int, int, int, str, bool, str], None]] = None,
) -> list[dict]:
    cfg = load_config()
    if not cfg or not cfg.get("api_id") or not cfg.get("api_hash"):
        raise ValueError("Не настроены api_id и api_hash")
    accounts = [a for a in cfg["accounts"] if is_account_authorized(a["phone"])]
    if not accounts:
        raise ValueError("Нет авторизованных аккаунтов")
    total = len(accounts)
    validated = 0
    failed = 0
    results = []
    for acc in accounts:
        phone = acc["phone"]
        proxy = acc.get("proxy") or cfg.get("proxy")
        try:
            success, msg = await validate_one_account(
                phone, test_link, test_message or "Тест",
                cfg["api_id"], cfg["api_hash"], proxy
            )
        except Exception as e:
            success, msg = False, str(e)
        results.append({"phone": phone, "valid": success, "message": msg})
        if success:
            validated += 1
            _mark_account_valid(cfg, phone)
        else:
            failed += 1
        if on_progress:
            on_progress(validated, failed, total, phone, success, msg)
        await asyncio.sleep(1)
    return results


def _mark_account_valid(cfg: dict, phone: str) -> None:
    from core import load_config, save_config
    c = load_config() or {}
    for acc in c.get("accounts", []):
        if acc["phone"] == phone:
            acc["validated"] = True
            break
    save_config(c)
