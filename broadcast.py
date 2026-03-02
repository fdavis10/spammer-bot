import asyncio
import re
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat, User

from core import (
    load_config as _load_cfg,
    SESSIONS_DIR,
    DATA_DIR,
    is_account_authorized,
    get_chat_links,
    add_sent_message_link,
)

DATA_DIR.mkdir(exist_ok=True)
BROADCAST_LOG = DATA_DIR / "broadcast.log"

CONCURRENT_SENDS = 8
LOGIN_DELAY = 1
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

VARIABLE_PATTERN = re.compile(r"\{([^}]+)\}")


def _message_link(msg, entity) -> str | None:
    if not msg or not hasattr(msg, "id"):
        return None
    if isinstance(entity, Channel):
        username = getattr(entity, "username", None) or ""
        if username:
            return f"https://t.me/{username}/{msg.id}"
        cid = entity.id
        if cid < 0:
            cid = int(str(cid).replace("-100", ""))
        return f"https://t.me/c/{cid}/{msg.id}"
    return None


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


async def get_chats(client, config: dict):
    chats = []
    async for d in client.iter_dialogs():
        if isinstance(d.entity, (Channel, Chat)):
            chats.append(d)
        elif isinstance(d.entity, User) and d.is_user and d.entity.is_self:
            chats.append(d)
    return chats


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


async def check_premium_status(config) -> int:
    from core import set_account_premium
    SESSIONS_DIR.mkdir(exist_ok=True)
    api_id = config.get("api_id") or 0
    api_hash = config.get("api_hash") or ""
    accounts = [a for a in (config.get("accounts") or []) if is_account_authorized(a.get("phone", ""))]
    if not accounts or not api_id or not api_hash:
        return 0
    updated = 0
    for acc in accounts:
        phone = acc["phone"]
        session_name = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
        proxy = acc.get("proxy") or config.get("proxy")
        client = create_client(session_name, api_id, api_hash, proxy)
        try:
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                premium = bool(getattr(me, "premium", False))
                set_account_premium(phone, premium)
                updated += 1
        except Exception:
            pass
        finally:
            await client.disconnect()
        await asyncio.sleep(LOGIN_DELAY)
    return updated


async def _send_one_with_retry(client, entity, text, parse_mode, files, phone, chat_name, max_retries=MAX_RETRIES):
    last_err = None
    for attempt in range(max_retries):
        try:
            if files:
                msg = await client.send_file(entity, files, caption=text or None, parse_mode=parse_mode)
            else:
                msg = await client.send_message(entity, text, parse_mode=parse_mode)
            return True, None, msg
        except Exception as e:
            last_err = e
            err_str = str(e).lower()
            if "flood" in err_str or "wait" in err_str or "connection" in err_str or "timeout" in err_str or "session" in err_str:
                if attempt < max_retries - 1:
                    await asyncio.sleep(RETRY_DELAY_SEC * (attempt + 1))
                    continue
            break
    return False, last_err, None


async def send_from_account(client, phone, message, stats, config=None, parse_mode=None, attachments=None, on_progress=None, stats_lock=None):
    config = config or {}
    delay_sec = max(0, float(config.get("message_delay_sec") or 2))
    use_variables = config.get("use_variables", True)
    max_retries = int(config.get("max_retries") or MAX_RETRIES)
    messages_per_chat = max(1, int(config.get("messages_per_chat_per_account") or 1))

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

                for msg_num in range(messages_per_chat):
                    ok, err, sent_msg = await _send_one_with_retry(client, d.entity, text, parse_mode, files, phone, chat_name, max_retries)
                    if ok:
                        _log_send("ОТПРАВЛЕНО", phone, chat_name)
                        try:
                            link = _message_link(sent_msg, d.entity)
                            if link:
                                add_sent_message_link(phone, link)
                        except Exception:
                            pass
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
                            if "auth" in err_l or "session" in err_l or "unregister" in err_l or "ban" in err_l or "заблокирован" in err_str.lower():
                                add_alert("critical", "Аккаунт заблокирован/сессия", f"{phone}: {err_str}", category="account")
                            elif "channel" in err_l or "chat" in err_l or "participant" in err_l or "peer" in err_l:
                                add_alert("warning", "Не удалось отправить в чат", f"{phone} -> {chat_name}: {err_str}", category="chat")
                            elif "flood" in err_l or "wait" in err_l:
                                add_alert("warning", "FloodWait", f"{phone} -> {chat_name}: {err_str}", category="other")
                            elif "connection" in err_l or "timeout" in err_l:
                                add_alert("error", "Сетевая ошибка", f"{phone}: {err_str}", category="other")
                            else:
                                add_alert("error", "Ошибка отправки", f"{phone} -> {chat_name}: {err_str}", category="other")
                        except Exception:
                            pass
                    if delay_sec > 0 and msg_num < messages_per_chat - 1:
                        await asyncio.sleep(delay_sec)

                if delay_sec > 0 and i < len(chats) - 1:
                    await asyncio.sleep(delay_sec)
    except Exception as e:
        _log_send("ОШИБКА", phone, "(критическая)", str(e))
        try:
            from dashboard import add_alert
            err_s = str(e).lower()
            if "auth" in err_s or "session" in err_s or "unregister" in err_s or "ban" in err_s:
                add_alert("critical", "Аккаунт заблокирован", f"{phone}: {e}", category="account")
            else:
                add_alert("error", "Критическая ошибка аккаунта", f"{phone}: {e}", category="account")
        except Exception:
            pass
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
        "messages_per_chat_per_account": max(1, int(config.get("messages_per_chat_per_account") or 1)),
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


async def _send_dm_to_users(client, phone, users, message, parse_mode, attachments, stats, config, on_progress, stats_lock):
    delay_sec = max(0, float(config.get("message_delay_sec") or 2))
    use_variables = config.get("use_variables", True)
    max_retries = int(config.get("max_retries") or MAX_RETRIES)
    files = attachments or []
    for i, u in enumerate(users):
        if hasattr(u, "bot") and u.bot:
            continue
        if hasattr(u, "deleted") and u.deleted:
            continue
        user_name = getattr(u, "first_name", "") or getattr(u, "username", "") or str(getattr(u, "id", ""))
        if use_variables:
            text = substitute_variables(message or "", user_name, phone, 1)
        else:
            text = message or ""
        ok, err, _ = await _send_one_with_retry(client, u, text, parse_mode, files, phone, user_name, max_retries)
        if ok:
            _log_send("ОТПРАВЛЕНО (ЛС)", phone, user_name)
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
            _log_send("ОШИБКА (ЛС)", phone, user_name, str(err))
            if stats_lock:
                async with stats_lock:
                    stats["failed"] += 1
                    if on_progress:
                        on_progress(stats["success"], stats["failed"])
            else:
                stats["failed"] += 1
                if on_progress:
                    on_progress(stats["success"], stats["failed"])
        if delay_sec > 0 and i < len(users) - 1:
            await asyncio.sleep(delay_sec)


async def run_dm_broadcast(config, code_input=None, on_progress: Optional[Callable[[int, int], None]] = None):
    links = [l for l in (get_chat_links() or []) if l and str(l).strip()]
    if not links:
        raise ValueError("Нет ссылок на чаты в базе. Импортируйте ссылки в разделе «Чаты».")
    SESSIONS_DIR.mkdir(exist_ok=True)
    api_id = config["api_id"]
    api_hash = config["api_hash"]
    accounts = [a for a in config["accounts"] if is_account_authorized(a["phone"])]
    if not accounts:
        raise ValueError("Нет авторизованных аккаунтов")
    message = config.get("message", "")
    parse_mode = config.get("parse_mode") or None
    _raw = config.get("attachments") or []
    attachments = [str(Path(p).resolve()) for p in _raw if Path(p).exists()]
    dm_config = {
        "message_delay_sec": config.get("message_delay_sec"),
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

    async def _dm_task(client, phone, password):
        try:
            await client.connect()
            me = await client.get_me()
            seen_ids = set()
            for link in links:
                try:
                    entity = await client.get_entity(link)
                    users_in_chat = []
                    async for u in client.iter_participants(entity):
                        if isinstance(u, User) and not getattr(u, "bot", False) and not getattr(u, "deleted", False):
                            if u.id != me.id and u.id not in seen_ids:
                                seen_ids.add(u.id)
                                users_in_chat.append(u)
                    if users_in_chat:
                        await _send_dm_to_users(
                            client, phone, users_in_chat, message, parse_mode, attachments,
                            stats, dm_config, _on_progress, stats_lock
                        )
                except Exception as ex:
                    _log_send("ОШИБКА (ЛС)", phone, f"чат {link}", str(ex))
                    try:
                        from dashboard import add_alert
                        add_alert("warning", "Не удалось получить участников чата", f"{phone} -> {link}: {ex}", category="chat")
                    except Exception:
                        pass
        except Exception as e:
            _log_send("ОШИБКА (ЛС)", phone, "(критическая)", str(e))
            try:
                from dashboard import add_alert
                err_s = str(e).lower()
                if "auth" in err_s or "session" in err_s or "unregister" in err_s or "ban" in err_s:
                    add_alert("critical", "Аккаунт заблокирован", f"{phone}: {e}", category="account")
                else:
                    add_alert("error", "Критическая ошибка аккаунта", f"{phone}: {e}", category="account")
            except Exception:
                pass
        finally:
            await client.disconnect()

    await asyncio.gather(*[_dm_task(c, ph, pw) for c, ph, pw in clients_data])

    done_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | --- ЛС: {stats['success']} отправлено, {stats['failed']} ошибок ---"
    with open(BROADCAST_LOG, "a", encoding="utf-8") as f:
        f.write(done_line + "\n")
    print(done_line)
    try:
        from dashboard import add_alert
        add_alert("info", "Рассылка в личку завершена", f"{stats['success']} отправлено, {stats['failed']} ошибок")
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
