import hashlib
import getpass
from pathlib import Path

try:
    from argon2 import PasswordHasher
    from argon2.exceptions import VerifyMismatchError
except ImportError:
    PasswordHasher = None
    VerifyMismatchError = Exception

from core import DATA_DIR

AUTH_FILE = DATA_DIR / ".auth"
REMEMBER_FILE = DATA_DIR / ".remember"

if PasswordHasher:
    _ph = PasswordHasher(time_cost=2, memory_cost=65536)
else:
    _ph = None


def _ensure_data_dir():
    AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)


def init_auth():
    if _ph is None:
        raise RuntimeError("Установи argon2-cffi: pip install argon2-cffi")
    _ensure_data_dir()
    pwd = getpass.getpass("Задай пароль доступа: ")
    if len(pwd) < 6:
        raise ValueError("Пароль минимум 6 символов")
    pwd2 = getpass.getpass("Повтори пароль: ")
    if pwd != pwd2:
        raise ValueError("Пароли не совпали")
    AUTH_FILE.write_text(_ph.hash(pwd), encoding="utf-8")
    print("Пароль установлен")


def verify(pwd: str) -> bool:
    if _ph is None or not AUTH_FILE.exists():
        return False
    stored = AUTH_FILE.read_text(encoding="utf-8").strip()
    try:
        _ph.verify(stored, pwd)
        return True
    except (VerifyMismatchError, Exception):
        return False


def _remember_token() -> str:
    if not AUTH_FILE.exists():
        return ""
    data = AUTH_FILE.read_bytes()
    return hashlib.sha256(data + b":remember").hexdigest()


def save_remember():
    _ensure_data_dir()
    REMEMBER_FILE.write_text(_remember_token(), encoding="utf-8")


def clear_remember():
    if REMEMBER_FILE.exists():
        REMEMBER_FILE.unlink()


def is_remembered() -> bool:
    if not REMEMBER_FILE.exists() or not AUTH_FILE.exists():
        return False
    try:
        return REMEMBER_FILE.read_text(encoding="utf-8").strip() == _remember_token()
    except Exception:
        return False


def require_auth() -> bool:
    if not AUTH_FILE.exists():
        print("Сначала выполни: python manage.py init-auth")
        return False
    pwd = getpass.getpass("Пароль доступа: ")
    if verify(pwd):
        return True
    print("Неверный пароль")
    return False
