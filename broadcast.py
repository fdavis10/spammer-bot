import asyncio
import multiprocessing
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import Channel, Chat

from core import load_config as _load_cfg, SESSIONS_DIR, DATA_DIR

DATA_DIR.mkdir(exist_ok=True)
BROADCAST_LOG = DATA_DIR / "broadcast.log"


def _log_send(prefix, phone, chat_name, extra=""):
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {prefix} | {phone} -> {chat_name}"
    if extra:
        line += f": {extra}"
    with open(BROADCAST_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)


CONCURRENT_SENDS = 8
LOGIN_DELAY = 1
MAX_ACCOUNTS = 10


def load_config():
    return _load_cfg()


async def get_chats(client):
    chats = []
    async for d in client.iter_dialogs():
        if isinstance(d.entity, (Channel, Chat)):
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


async def send_from_account(client, phone, message, stats, parse_mode=None, attachments=None):
    try:
        await client.connect()
        chats = await get_chats(client)
        files = attachments or []
        sem = asyncio.Semaphore(CONCURRENT_SENDS)

        async def send_one(d):
            async with sem:
                try:
                    if files:
                        await client.send_file(d.entity, files, caption=message or None, parse_mode=parse_mode)
                    else:
                        await client.send_message(d.entity, message, parse_mode=parse_mode)
                    _log_send("ОТПРАВЛЕНО", phone, d.name)
                    return 1, 0
                except Exception as e:
                    _log_send("ОШИБКА", phone, d.name, str(e))
                    return 0, 1

        results = await asyncio.gather(*[send_one(d) for d in chats], return_exceptions=True)
        for r in results:
            if isinstance(r, tuple):
                stats["success"] += r[0]
                stats["failed"] += r[1]
            else:
                stats["failed"] += 1
    except Exception as e:
        _log_send("ОШИБКА", phone, "(критическая)", str(e))
    finally:
        await client.disconnect()


def _broadcast_worker(args):
    sessions_dir = Path(args["sessions_dir"])
    api_id = args["api_id"]
    api_hash = args["api_hash"]
    phone = args["phone"]
    message = args["message"]
    parse_mode = args.get("parse_mode")
    attachments = [str(Path(p).resolve()) for p in (args.get("attachments") or []) if Path(p).exists()]
    session_name = sessions_dir / phone.replace("+", "").replace(" ", "")
    client = TelegramClient(str(session_name), api_id, api_hash)
    stats = {"success": 0, "failed": 0}
    asyncio.run(send_from_account(client, phone, message, stats, parse_mode=parse_mode or None, attachments=attachments or None))
    return stats


async def run_broadcast(config, code_input=None):
    SESSIONS_DIR.mkdir(exist_ok=True)
    api_id = config["api_id"]
    api_hash = config["api_hash"]
    accounts = config["accounts"]
    message = config.get("message", "")
    parse_mode = config.get("parse_mode") or None
    _raw = config.get("attachments") or []
    attachments = [str(Path(p).resolve()) for p in _raw if Path(p).exists()]

    clients = []
    for acc in accounts:
        session_name = SESSIONS_DIR / acc["phone"].replace("+", "").replace(" ", "")
        client = TelegramClient(str(session_name), api_id, api_hash)
        clients.append((client, acc["phone"], acc.get("password", "")))

    for client, phone, password in clients:
        try:
            await auth_account(client, phone, password, code_input=code_input)
        except Exception:
            raise
        await asyncio.sleep(LOGIN_DELAY)
    stats = {"success": 0, "failed": 0}
    worker_args = [
        {
            "sessions_dir": str(SESSIONS_DIR),
            "api_id": api_id,
            "api_hash": api_hash,
            "phone": phone,
            "message": message,
            "parse_mode": parse_mode,
            "attachments": attachments or [],
        }
        for _, phone, _ in clients
    ]
    with multiprocessing.Pool(len(clients)) as pool:
        for s in pool.map(_broadcast_worker, worker_args):
            stats["success"] += s["success"]
            stats["failed"] += s["failed"]

    done_line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | --- Готово: {stats['success']} отправлено, {stats['failed']} ошибок ---"
    with open(BROADCAST_LOG, "a", encoding="utf-8") as f:
        f.write(done_line + "\n")
    print(done_line)
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
