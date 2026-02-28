import asyncio
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable, Optional

from core import TDATA_IMPORT_DIR, SESSIONS_DIR, load_config, save_config


def _is_tdata_folder(path: Path) -> bool:
    if not path.is_dir():
        return False
    return (path / "map").exists() or (path / "key_datas").exists()


def _find_tdata_folders(base: Path) -> list[tuple[Path, str]]:
    result = []
    if not base.exists():
        return result

    direct_tdata = base / "tdata"
    if _is_tdata_folder(direct_tdata):
        result.append((direct_tdata, base.name or "tdata"))

    for name in os.listdir(base):
        if name.startswith("."):
            continue
        sub = base / name
        if sub.is_dir():
            tdata_path = sub / "tdata"
            if _is_tdata_folder(tdata_path):
                result.append((tdata_path, name))
            elif _is_tdata_folder(sub):
                result.append((sub, name))
    return result


async def _convert_one(
    tdata_path: Path,
    identifier: str,
    api_id: int,
    api_hash: str,
    on_progress: Optional[Callable[[str, str, bool], None]] = None,
) -> Optional[str]:
    try:
        from opentele.td import TDesktop
        from opentele.api import UseCurrentSession, APIData
        from telethon.errors import SessionPasswordNeededError
    except ImportError:
        if on_progress:
            on_progress(identifier, "Ошибка: установите opentele (pip install opentele)", False)
        return None

    tdesk = TDesktop(str(tdata_path))
    if not tdesk.isLoaded():
        if on_progress:
            on_progress(identifier, "Не удалось загрузить tdata", False)
        return None

    api = APIData(api_id=api_id, api_hash=api_hash)
    session_path = SESSIONS_DIR / f"tdata_{identifier}.session"

    try:
        client = await tdesk.ToTelethon(session=str(session_path), flag=UseCurrentSession, api=api)
        await client.connect()
        if not await client.is_user_authorized():
            if on_progress:
                on_progress(identifier, "Сессия не авторизована", False)
            await client.disconnect()
            return None
        me = await client.get_me()
        raw_phone = me.phone
        phone = (raw_phone or str(me.id)).replace("+", "").replace(" ", "")
        await client.disconnect()
    except SessionPasswordNeededError:
        if on_progress:
            on_progress(identifier, "Включена 2FA — отключите в Telegram Desktop и повторите", False)
        return None
    except Exception as e:
        if on_progress:
            on_progress(identifier, str(e), False)
        return None

    final_path = SESSIONS_DIR / f"{phone}.session"
    if session_path != final_path:
        if final_path.exists():
            final_path.unlink()
        session_path.rename(final_path)
        for ext in (".session-journal",):
            j = Path(str(session_path) + ext)
            if j.exists():
                j.rename(str(final_path) + ext)

    if on_progress:
        on_progress(identifier, f"+{phone}" if raw_phone else f"ID {me.id}", True)
    return phone


async def import_tdata_folder(
    folder: Path,
    api_id: int,
    api_hash: str,
    on_progress: Optional[Callable[[str, str, bool], None]] = None,
) -> list[str]:
    pairs = _find_tdata_folders(folder)
    if not pairs:
        return []
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    phones = []
    for tdata_path, identifier in pairs:
        phone = await _convert_one(tdata_path, identifier, api_id, api_hash, on_progress)
        if phone:
            phones.append(phone)
    return phones


def add_accounts_to_config(phones: list[str]) -> None:
    cfg = load_config() or {"api_id": 0, "api_hash": "", "accounts": [], "message": ""}
    cfg.setdefault("accounts", [])
    existing = {acc["phone"].replace("+", "").replace(" ", "") for acc in cfg["accounts"]}
    for p in phones:
        normalized = p.replace("+", "").replace(" ", "")
        if normalized.isdigit():
            ph = f"+{normalized}" if not p.startswith("+") else p
        else:
            ph = p  # id_12345
        key = ph.replace("+", "").replace(" ", "")
        if key not in existing:
            cfg["accounts"].append({"phone": ph, "password": ""})
            existing.add(key)
    save_config(cfg)


def open_tdata_folder() -> None:
    TDATA_IMPORT_DIR.mkdir(parents=True, exist_ok=True)(parents=True, exist_ok=True)
    path = str(TDATA_IMPORT_DIR.resolve())
    if os.name == "nt":
        os.startfile(path)
    elif sys.platform == "darwin":
        subprocess.run(["open", path], check=False)
    else:
        subprocess.run(["xdg-open", path], check=False)
