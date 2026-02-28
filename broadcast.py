import asyncio
import re
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

from core import load_config as _load_cfg, SESSIONS_DIR, DATA_DIR

DATA_DIR.mkdir(exist_ok=True)
BROADCAST_LOG = DATA_DIR / "broadcast.log"

CONCURRENT_SENDS = 8
LOGIN_DELAY = 1
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

VARIABLE_PATTERN = re.compile(r"\{([^}]+)\}")


def _log_send(prefix, phone, chat_name, extra=""):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {prefix} | {phone} -> {chat_name}"
    if extra:
        line += f": {extra}"
    with open(BROADCAST_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


def substitute_variables(text: str, chat_name: str, phone: str, participants_count: int = 0) -> str:
    if not text:
        return text
    replacements = {
        "имя": chat_name,
        "name": chat_name,
        "номер": phone,
        "number": phone,
        "телефон": phone,
        "phone": phone,
        "участников": str(participants_count),
        "participants": str(participants_count),
    }

    def replace(m):
        key = m.group(1).strip().lower()
        return replacements.get(key, m.group(0))

    return VARIABLE_PATTERN.sub(replace, text)


def load_config():
    return _load_cfg()


def _parse_proxy(proxy_cfg: dict) -> Optional[tuple]:
    if not proxy_cfg or not isinstance(proxy_cfg, dict):
        return None
    ptype = (proxy_cfg.get("type") or "socks5").lower()
    host = (proxy_cfg.get("host") or "").strip()
    port = int(proxy_cfg.get("port") or 1080)
    if not host:
        return None
    try:
        import socks
    except ImportError:
        return None
    if "socks5" in ptype or "socks" in ptype:
        sock_type = socks.SOCKS5
    elif "socks4" in ptype:
        sock_type = socks.SOCKS4
    elif "http" in ptype:
        sock_type = socks.HTTP
    else:
        sock_type = socks.SOCKS5
    username = (proxy_cfg.get("username") or "").strip()
    password = (proxy_cfg.get("password") or "").strip()
    if username or password:
        return (sock_type, host, port, True, username or None, password or None)
    return (sock_type, host, port)


def create_client(session_path, api_id, api_hash, proxy_cfg=None):
    proxy = _parse_proxy(proxy_cfg) if proxy_cfg else None
    return TelegramClient(str(session_path), api_id, api_hash, proxy=proxy)


async def _get_participants_count(client, entity):
    try:
        if hasattr(entity, "participants_count") and entity.participants_count is not None:
            return entity.participants_count or 0
        from telethon.tl.functions.channels import GetFullChannelRequest
        from telethon.tl.functions.messages import GetFullChatRequest
        if isinstance(entity, Channel):
            full = await client(GetFullChannelRequest(entity))
            return getattr(full.full_chat, "participants_count", 0) or 0
        if isinstance(entity, Chat):
            full = await client(GetFullChatRequest(entity.id))
            return getattr(full.full_chat, "participants_count", 0) or 0
    except Exception:
        pass
    return 0


def _name_matches(name: str, patterns: list) -> bool:
    if not patterns:
        return False
    name_lower = (name or "").lower()
    return any(p and p.strip().lower() in name_lower for p in patterns if p)


def _chat_matches_filters(dialog, filters: dict) -> bool:
    if not filters:
        return True
    name = (dialog.name or "").strip()
    inc = filters.get("include_by_name") or []
    exc = filters.get("exclude_by_name") or []
    for pattern in exc:
        if pattern and pattern.strip().lower() in (name or "").lower():
            return False
    if inc:
        if not _name_matches(name, inc):
            return False
    return True


def _chat_in_blacklist(name: str, blacklist: list) -> bool:
    if not blacklist:
        return False
    return _name_matches(name, blacklist)


def _chat_in_whitelist(name: str, whitelist: list) -> bool:
    if not whitelist:
        return True
    return _name_matches(name, whitelist)


async def get_chats(client, config: dict):
    chats = []
    async for d in client.iter_dialogs():
        if isinstance(d.entity, (Channel, Chat)):
            chats.append(d)
        elif isinstance(d.entity, User) and d.is_user and d.entity.is_self:
            chats.append(d)

    test_mode = config.get("test_mode") or "off"
    if test_mode == "self":
        return [type("_MeDialog", (), {"entity": "me", "name": "Избранное"})()]

    test_chat_name = (config.get("test_chat_name") or "").strip()
    if test_mode == "single_chat" and test_chat_name:
        filtered = [d for d in chats if test_chat_name.lower() in (d.name or "").lower()]
        return filtered[:1] if filtered else []

    chat_filter = config.get("chat_filter") or {}
    blacklist = config.get("blacklist") or []
    whitelist = config.get("whitelist") or []
    min_p = chat_filter.get("min_participants") or 0
    max_p = chat_filter.get("max_participants") or 0

    result = []
    for d in chats:
        name = d.name or ""
        if _chat_in_blacklist(name, blacklist):
            continue
        if not _chat_in_whitelist(name, whitelist):
            continue
        if not _chat_matches_filters(d, chat_filter):
            continue
        if min_p > 0 or max_p > 0:
            cnt = await _get_participants_count(client, d.entity)
            if min_p > 0 and cnt < min_p:
                continue
            if max_p > 0 and cnt > max_p:
                continue
        result.append(d)
    return result


async def request_code(client, phone):
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
    await client.disconnect()


async def sign_in_with_code(client, phone, code, password=None):
    await client.connect()
    if not await client.is_user_authorized():
        await client.sign_in(phone, code, password=password)
    await client.disconnect()


async def auth_account(client, phone, password, code_input=None):
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
        get_code = code_input if code_input else lambda p: input(f"Код для {p}: ").strip()
        code = get_code(phone)
        await client.sign_in(phone, code, password=password or None)
    await client.disconnect()


async def _send_one_with_retry(client, entity, text, parse_mode, files, phone, chat_name, max_retries=MAX_RETRIES):
    last_err = None
    for attempt in range(max_retries):
        try:
            if files:
                await client.send_file(entity, files, caption=text or None, parse_mode=parse_mode)
            else:
                await client.send_message(entity, text, parse_mode=parse_mode)
            return True, None
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if "flood" in err_str or "wait" in err_str or "connection" in err_str or "timeout" in err_str or "session" in err_str:
                if attempt < max_retries - 1:
                    await asyncio.sleep(RETRY_DELAY_SEC * (attempt + 1))
                    continue
            break
    return False, last_err


async def send_from_account(client, phone, message, stats, config=None, parse_mode=None, attachments=None, on_progress=None, stats_lock=None):
    config = config or {}
    delay_sec = max(0, float(config.get("message_delay_sec") or 2))
    use_variables = config.get("use_variables", True)
    max_retries = int(config.get("max_retries") or MAX_RETRIES)

    try:
        await client.connect()
        chats = await get_chats(client, config)
        files = attachments or []
        sem = asyncio.Semaphore(CONCURRENT_SENDS)

        for i, d in enumerate(chats):
            async with sem:
                chat_name = getattr(d, "name", "") or "Чат"
                participants_count = 0
                if use_variables and ("{" in (message or "")):
                    participants_count = await _get_participants_count(client, d.entity) if d.entity != "me" else 0
                text = substitute_variables(message or "", chat_name, phone, participants_count) if use_variables else (message or "")
                ok, err = await _send_one_with_retry(client, d.entity, text, parse_mode, files, phone, chat_name, max_retries)
                if ok:
                    _log_send("ОТПРАВЛЕНО", phone, chat_name)
                    if stats_lock:
                        async with stats_lock:
                            stats["success"] += 1
                            if on_progress:
                                on_progress(stats["success"], stats["failed"])
                    else:
                        stats["success"] += 1
                        if on_progress:
                            on_progress(stats["success"], stats["failed"])
                else:
                    err_str = str(err)
                    _log_send("ОШИБКА", phone, chat_name, err_str)
                    if stats_lock:
                        async with stats_lock:
                            stats["failed"] += 1
                            if on_progress:
                                on_progress(stats["success"], stats["failed"])
                    else:
                        stats["failed"] += 1
                        if on_progress:
                            on_progress(stats["success"], stats["failed"])
                    try:
                        from dashboard import add_alert
                        err_l = err_str.lower()
                        if "flood" in err_l or "wait" in err_l:
                            add_alert("warning", "FloodWait", f"{phone} -> {chat_name}: {err_str}")
                        elif "auth" in err_l or "session" in err_l or "unregister" in err_l:
                            add_alert("critical", "Бан/сессия", f"{phone}: {err_str}")
                        elif "connection" in err_l or "timeout" in err_l:
                            add_alert("error", "Сетевая ошибка", f"{phone}: {err_str}")
                    except Exception:
                        pass
                if delay_sec > 0 and i < len(chats) - 1:
                    await asyncio.sleep(delay_sec)
    except Exception as e:
        _log_send("ОШИБКА", phone, "(критическая)", str(e))
    finally:
        await client.disconnect()


async def run_broadcast(config, code_input=None, on_progress: Optional[Callable[[int, int], None]] = None):
    SESSIONS_DIR.mkdir(exist_ok=True)
    api_id = config["api_id"]
    api_hash = config["api_hash"]
    accounts = config["accounts"]
    message = config.get("message", "")
    parse_mode = config.get("parse_mode") or None
    _raw = config.get("attachments") or []
    attachments = [str(Path(p).resolve()) for p in _raw if Path(p).exists()]

    broadcast_config = {
        "message_delay_sec": config.get("message_delay_sec"),
        "test_mode": config.get("test_mode"),
        "test_chat_name": config.get("test_chat_name"),
        "chat_filter": config.get("chat_filter"),
        "blacklist": config.get("blacklist"),
        "whitelist": config.get("whitelist"),
        "use_variables": config.get("use_variables", True),
        "max_retries": config.get("max_retries", MAX_RETRIES),
    }
    stats = {"success": 0, "failed": 0}

    def _on_progress(s, f):
        stats["success"] = s
        stats["failed"] = f
        if on_progress:
            on_progress(s, f)

    clients_data = []
    for acc in accounts:
        phone = acc["phone"]
        session_name = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
        proxy = acc.get("proxy") or config.get("proxy")
        client = create_client(session_name, api_id, api_hash, proxy)
        clients_data.append((client, phone, acc.get("password", "")))

    for client, phone, password in clients_data:
        try:
            await auth_account(client, phone, password, code_input=code_input)
        except Exception:
            raise
        await asyncio.sleep(LOGIN_DELAY)

    stats_lock = asyncio.Lock()
    await asyncio.gather(*[
        send_from_account(
            client, phone, message, stats,
            config=broadcast_config,
            parse_mode=parse_mode or None,
            attachments=attachments or None,
            on_progress=_on_progress,
            stats_lock=stats_lock,
        )
        for client, phone, _ in clients_data
    ])

    done_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | --- Готово: {stats['success']} отправлено, {stats['failed']} ошибок ---"
    with open(BROADCAST_LOG, "a", encoding="utf-8") as f:
        f.write(done_line + "\n")
    print(done_line)
    try:
        from dashboard import add_alert
        if stats["failed"] > stats["success"] and stats["failed"] > 0:
            add_alert("warning", "Рассылка завершена с ошибками", f"{stats['success']} отправлено, {stats['failed']} ошибок")
        else:
            add_alert("info", "Рассылка завершена", f"{stats['success']} отправлено, {stats['failed']} ошибок")
    except Exception:
        pass
    return stats


def main():
    from auth import require_auth
    if not require_auth():
        return
    config = load_config()
    if not config:
        print("Конфиг не найден. Запусти: python manage.py setup")
        return
    asyncio.run(run_broadcast(config))


if __name__ == "__main__":
    main()
