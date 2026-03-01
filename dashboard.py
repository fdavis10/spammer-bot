import csv
import json
import platform
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from core import DATA_DIR, load_config, CONFIG_PATH

BROADCAST_LOG = DATA_DIR / "broadcast.log"
ALERTS_FILE = DATA_DIR / "alerts.json"
STATS_CACHE_FILE = DATA_DIR / "dashboard_stats.json"


def _classify_error(msg: str) -> str:
    m = (msg or "").lower()
    if "flood" in m or "wait" in m or "seconds" in m:
        return "FloodWait"
    if "auth" in m or "unregister" in m or "session" in m:
        return "Бан/сессия"
    if "channel" in m or "chat" in m or "participant" in m:
        return "Доступ"
    if "connection" in m or "timeout" in m or "network" in m:
        return "Сеть"
    return "Прочее"


def get_dashboard_stats():
    result = {
        "total_sent": 0,
        "total_failed": 0,
        "by_date": [],
        "error_types": {},
        "last_lines": [],
        "last_7_days": [],
    }
    if not BROADCAST_LOG.exists():
        return result
    by_date = defaultdict(lambda: {"sent": 0, "failed": 0, "errors": defaultdict(int)})
    error_types = defaultdict(int)
    simplified = []
    with open(BROADCAST_LOG, encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue
            parts = raw.split(" | ")
            date_str = parts[0][:10] if len(parts) > 0 else ""
            if "ОТПРАВЛЕНО" in raw:
                result["total_sent"] += 1
                if date_str:
                    by_date[date_str]["sent"] += 1
                if " -> " in raw:
                    chat = raw.split(" -> ", 1)[-1].split(":")[0].strip()
                    simplified.append(f"✓ {chat}")
                else:
                    simplified.append("✓ Отправлено")
            elif "ОШИБКА" in raw:
                result["total_failed"] += 1
                if date_str:
                    by_date[date_str]["failed"] += 1
                err_msg = raw.split(":", 1)[-1].strip() if ":" in raw else ""
                etype = _classify_error(err_msg)
                error_types[etype] += 1
                if date_str:
                    by_date[date_str]["errors"][etype] += 1
                chat = ""
                if " -> " in raw:
                    rest = raw.split(" -> ", 1)[-1]
                    chat = rest.split(":")[0].strip() if ":" in rest else rest
                simplified.append(f"✗ {chat or 'Ошибка'}: {err_msg[:50]}...")
    result["last_lines"] = simplified[-30:][::-1]
    sorted_dates = sorted(by_date.keys())
    for d in sorted_dates[-14:]:
        v = by_date[d]
        result["by_date"].append({"date": d, "sent": v["sent"], "failed": v["failed"]})
    result["error_types"] = dict(error_types)
    result["last_7_days"] = result["by_date"][-7:]
    return result


def get_alerts(limit=200):
    if not ALERTS_FILE.exists():
        return []
    try:
        with open(ALERTS_FILE, encoding="utf-8") as f:
            alerts = json.load(f)
        result = list(alerts)[-limit:][::-1]
        for a in result:
            if "category" not in a:
                a["category"] = _infer_category(a.get("level", ""), a.get("message", ""), a.get("details", ""))
        return result
    except (json.JSONDecodeError, IOError):
        return []


def get_alerts_grouped(limit=200):
    alerts = get_alerts(limit=limit)
    grouped = {"account": [], "chat": [], "other": []}
    for a in alerts:
        cat = a.get("category") or "other"
        if cat not in grouped:
            grouped[cat] = []
        grouped[cat].append(a)
    return grouped


def _infer_category(level: str, message: str, details: str) -> str:
    combined = (str(message) + " " + str(details)).lower()
    if level == "critical" or "бан" in combined or "session" in combined or "auth" in combined or "unregister" in combined or "заблокирован" in combined:
        return "account"
    if "чат" in combined or "групп" in combined or "channel" in combined or "participant" in combined or "peer" in combined or " -> " in details:
        return "chat"
    return "other"


def add_alert(level: str, message: str, details: str = "", category: str = None):
    alerts = get_alerts(limit=500)
    cat = category or _infer_category(level, message, details)
    alerts.append({
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "message": message,
        "details": details,
        "category": cat,
    })
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump(alerts[-200:], f, ensure_ascii=False, indent=0)


def build_error_log_content() -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("SPAMMER BOT — ПОЛНЫЙ ЛОГ ДЛЯ РАЗРАБОТЧИКА")
    lines.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    lines.append("=" * 60)
    lines.append("")
    lines.append("--- СИСТЕМА ---")
    lines.append(f"Python: {sys.version}")
    lines.append(f"Платформа: {platform.platform()}")
    lines.append(f"Frozen (exe): {getattr(sys, 'frozen', False)}")
    lines.append("")
    lines.append("--- УВЕДОМЛЕНИЯ (alerts) ---")
    alerts = get_alerts(limit=500)
    if not alerts:
        lines.append("(нет)")
    else:
        for a in alerts:
            lines.append(f"  [{a.get('level', '')}] {a.get('message', '')}")
            if a.get("details"):
                lines.append(f"    {a['details']}")
            lines.append(f"    {a.get('ts', '')}")
    lines.append("")
    lines.append("--- ЛОГ РАССЫЛКИ (broadcast.log) ---")
    if BROADCAST_LOG.exists():
        with open(BROADCAST_LOG, encoding="utf-8") as f:
            log_lines = f.readlines()
        for line in log_lines[-500:]:
            lines.append(line.rstrip())
    else:
        lines.append("(файл не найден)")
    lines.append("")
    lines.append("--- КОНФИГ (без паролей и api_hash) ---")
    cfg = load_config()
    if cfg:
        safe = {}
        for k, v in cfg.items():
            if k == "accounts":
                safe[k] = [{"phone": a.get("phone", ""), "proxy": bool(a.get("proxy"))} for a in (v or [])]
            elif k == "api_hash":
                safe[k] = "(скрыто)"
            else:
                safe[k] = v
        lines.append(json.dumps(safe, ensure_ascii=False, indent=2))
    else:
        lines.append("(конфиг не найден)")
    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def add_alert_from_error(alert_error):
    from errors import AlertError
    if isinstance(alert_error, AlertError):
        d = alert_error.to_dict()
        d["ts"] = d["ts"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        add_alert(d["level"], d["message"], d["details"], d["category"])


def export_report_csv(filepath: str) -> int:
    stats = get_dashboard_stats()
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Метрика", "Значение"])
        w.writerow(["Всего отправлено", stats["total_sent"]])
        w.writerow(["Всего ошибок", stats["total_failed"]])
        total = stats["total_sent"] + stats["total_failed"]
        rate = (stats["total_sent"] / total * 100) if total else 0
        w.writerow(["Успешность %", f"{rate:.1f}"])
        w.writerow([])
        w.writerow(["По датам", "Отправлено", "Ошибок"])
        for row in stats["by_date"]:
            w.writerow([row["date"], row["sent"], row["failed"]])
        w.writerow([])
        w.writerow(["Типы ошибок", "Кол-во"])
        for etype, cnt in stats["error_types"].items():
            w.writerow([etype, cnt])
    return 10 + len(stats["by_date"]) + len(stats["error_types"])


def export_report_excel(filepath: str) -> int:
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment
    stats = get_dashboard_stats()
    wb = Workbook()
    ws = wb.active
    ws.title = "Сводка"
    ws.append(["Метрика", "Значение"])
    ws.append(["Всего отправлено", stats["total_sent"]])
    ws.append(["Всего ошибок", stats["total_failed"]])
    total = stats["total_sent"] + stats["total_failed"]
    rate = (stats["total_sent"] / total * 100) if total else 0
    ws.append(["Успешность %", f"{rate:.1f}"])
    ws.append([])
    ws.append(["По датам", "Отправлено", "Ошибок"])
    for row in stats["by_date"]:
        ws.append([row["date"], row["sent"], row["failed"]])
    ws.append([])
    ws.append(["Типы ошибок", "Кол-во"])
    for etype, cnt in stats["error_types"].items():
        ws.append([etype, cnt])
    ws2 = wb.create_sheet("Последние записи")
    ws2.append(["Событие"])
    for line in stats["last_lines"]:
        ws2.append([line])
    wb.save(filepath)
    return 1
