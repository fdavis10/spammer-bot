from enum import Enum
from dataclasses import dataclass


class AlertLevel(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertCategory(str, Enum):
    ACCOUNT = "account"
    CHAT = "chat"
    OTHER = "other"


@dataclass
class AccordionCategory:
    key: str
    title: str
    subtitle: str


ACCORDION_CATEGORIES = {
    AlertCategory.ACCOUNT: AccordionCategory(
        key="account",
        title="Аккаунты",
        subtitle="Заблокированные аккаунты, проблемы с сессией, авторизацией",
    ),
    AlertCategory.CHAT: AccordionCategory(
        key="chat",
        title="Чаты",
        subtitle="Группы/чаты, в которые не удалось отправить сообщение или вступить",
    ),
    AlertCategory.OTHER: AccordionCategory(
        key="other",
        title="Прочие ошибки",
        subtitle="FloodWait, сетевые сбои и другие",
    ),
}


@dataclass
class AlertLevelStyle:
    level: AlertLevel
    color_key: str
    icon_key: str


def get_level_style(level: str) -> AlertLevelStyle:
    styles = {
        AlertLevel.CRITICAL: AlertLevelStyle(AlertLevel.CRITICAL, "error", "ERROR"),
        AlertLevel.ERROR: AlertLevelStyle(AlertLevel.ERROR, "error", "ERROR_OUTLINE"),
        AlertLevel.WARNING: AlertLevelStyle(AlertLevel.WARNING, "orange", "WARNING_AMBER"),
        AlertLevel.INFO: AlertLevelStyle(AlertLevel.INFO, "primary", "INFO_OUTLINE"),
    }
    try:
        lvl = AlertLevel(level)
    except ValueError:
        lvl = AlertLevel.INFO
    return styles.get(lvl, styles[AlertLevel.INFO])


@dataclass
class AlertError:
    message: str
    details: str = ""
    level: AlertLevel = AlertLevel.ERROR
    category: AlertCategory = AlertCategory.OTHER
    ts: str = ""

    def to_dict(self) -> dict:
        return {
            "ts": self.ts,
            "message": self.message,
            "details": self.details,
            "level": self.level.value,
            "category": self.category.value,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "AlertError":
        level = AlertLevel(d.get("level", "error"))
        category = AlertCategory(d.get("category", "other"))
        return cls(
            message=d.get("message", ""),
            details=d.get("details", ""),
            level=level,
            category=category,
            ts=d.get("ts", ""),
        )


class AccountBlockedError(AlertError):
    def __init__(self, message: str = "Аккаунт заблокирован", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.CRITICAL, category=AlertCategory.ACCOUNT, ts=ts)


class AccountSessionError(AlertError):
    def __init__(self, message: str = "Проблема с сессией", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.CRITICAL, category=AlertCategory.ACCOUNT, ts=ts)


class AccountNotAuthorizedError(AlertError):
    def __init__(self, message: str = "Аккаунт не авторизован", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.CRITICAL, category=AlertCategory.ACCOUNT, ts=ts)


class ChatSendFailedError(AlertError):
    def __init__(self, message: str = "Не удалось отправить в чат", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.WARNING, category=AlertCategory.CHAT, ts=ts)


class ChatJoinFailedError(AlertError):
    def __init__(self, message: str = "Не удалось вступить в чат", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.WARNING, category=AlertCategory.CHAT, ts=ts)


class ChatParticipantsError(AlertError):
    def __init__(self, message: str = "Не удалось получить участников чата", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.WARNING, category=AlertCategory.CHAT, ts=ts)


class FloodWaitError(AlertError):
    def __init__(self, message: str = "FloodWait", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.WARNING, category=AlertCategory.OTHER, ts=ts)


class NetworkError(AlertError):
    def __init__(self, message: str = "Сетевая ошибка", details: str = "", ts: str = ""):
        super().__init__(message=message, details=details, level=AlertLevel.ERROR, category=AlertCategory.OTHER, ts=ts)


class GenericError(AlertError):
    def __init__(self, message: str, details: str = "", level: AlertLevel = AlertLevel.ERROR, ts: str = ""):
        super().__init__(message=message, details=details, level=level, category=AlertCategory.OTHER, ts=ts)
