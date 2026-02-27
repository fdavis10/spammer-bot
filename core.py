import json
import os
import shutil
import sys
import uuid
from pathlib import Path

def _get_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

BASE_DIR = _get_base_dir()
CONFIG_PATH = BASE_DIR / "config.json"
DATA_DIR = BASE_DIR / "data"
ATTACHMENTS_DIR = DATA_DIR / "attachments"


def add_file_to_storage(source_path: str) -> str:
    src = Path(source_path)
    if not src.exists():
        raise FileNotFoundError(f"Файл не найден: {source_path}")
    ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
    ext = src.suffix or ""
    name = f"{uuid.uuid4().hex}{ext}"
    dest = ATTACHMENTS_DIR / name
    shutil.copy2(src, dest)
    return str(dest.resolve())

_base = Path(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")))
SESSIONS_DIR = _base / "spammer-bot" / "sessions"
MAX_ACCOUNTS = 10

OLD_SESSIONS = BASE_DIR / "sessions"


def _migrate_sessions():
    if not OLD_SESSIONS.exists():
        return
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    for f in OLD_SESSIONS.glob("*.session*"):
        dst = SESSIONS_DIR / f.name
        if not dst.exists() or f.stat().st_mtime > dst.stat().st_mtime:
            import shutil

            shutil.copy2(f, dst)


def load_config():
    if not CONFIG_PATH.exists():
        return None
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg):
    DATA_DIR.mkdir(exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=4)


def is_account_authorized(phone):
    name = phone.replace("+", "").replace(" ", "")
    return (SESSIONS_DIR / f"{name}.session").exists()


def get_templates():
    cfg = load_config() or {}
    return cfg.get("templates") or []


def save_templates(templates):
    cfg = load_config() or {}
    cfg["templates"] = templates
    save_config(cfg)


def add_template(name, message, parse_mode, attachments):
    tpl = {"name": name.strip(), "message": message or "", "parse_mode": parse_mode or "none", "attachments": list(attachments or [])}
    templates = get_templates()
    templates.append(tpl)
    save_templates(templates)


def delete_template(name):
    templates = [t for t in get_templates() if t.get("name") != name]
    save_templates(templates)


CHAT_LINKS_PATH = DATA_DIR / "chat_links.json"


def _migrate_chat_links():
    cfg = load_config()
    if not cfg or "chat_links" not in cfg:
        return
    links = cfg.get("chat_links") or []
    if links:
        DATA_DIR.mkdir(exist_ok=True)
        with open(CHAT_LINKS_PATH, "w", encoding="utf-8") as f:
            json.dump(links, f, ensure_ascii=False, indent=0)
    del cfg["chat_links"]
    save_config(cfg)


def get_chat_links():
    if not CHAT_LINKS_PATH.exists():
        return []
    try:
        with open(CHAT_LINKS_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return []


def save_chat_links(links: list[str]):
    DATA_DIR.mkdir(exist_ok=True)
    cleaned = list(dict.fromkeys(str(l).strip() for l in links if l and str(l).strip()))
    with open(CHAT_LINKS_PATH, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, indent=0)


def add_chat_links(new_links: list[str]):
    current = get_chat_links()
    seen = set(current)
    for link in new_links:
        l = str(link).strip()
        if l and l not in seen:
            seen.add(l)
            current.append(l)
    save_chat_links(current)


def _simplify_log_line(line: str) -> str | None:
    s = line.strip()
    if "ОТПРАВЛЕНО" in s:
        if " -> " in s:
            chat = s.split(" -> ", 1)[-1].split(":")[0].strip()
            return f"Отправлено: {chat}"
        return "Отправлено"
    if "ОШИБКА" in s:
        if " -> " in s:
            rest = s.split(" -> ", 1)[-1]
            chat = rest.split(":")[0].strip() if ":" in rest else rest
            return f"Ошибка: {chat}"
        return "Ошибка"
    if "Готово" in s and "отправлено" in s:
        return s.split("|")[-1].strip()
    return None


def get_stats():
    log_path = DATA_DIR / "broadcast.log"
    if not log_path.exists():
        return {"total_sent": 0, "total_failed": 0, "last_lines": []}
    sent = 0
    failed = 0
    simplified = []
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if "ОТПРАВЛЕНО" in raw:
                sent += 1
            elif "ОШИБКА" in raw:
                failed += 1
            short = _simplify_log_line(raw)
            if short:
                simplified.append(short)
    return {
        "total_sent": sent,
        "total_failed": failed,
        "last_lines": simplified[-50:] if len(simplified) > 50 else simplified,
    }


_migrate_sessions()
_migrate_chat_links()
