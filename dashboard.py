import csv
import json
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from core import DATA_DIR

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


def get_alerts(limit=20):
    if not ALERTS_FILE.exists():
        return []
    try:
        with open(ALERTS_FILE, encoding="utf-8") as f:
            alerts = json.load(f)
        return list(alerts)[-limit:][::-1]
    except (json.JSONDecodeError, IOError):
        return []


def add_alert(level: str, message: str, details: str = ""):
    alerts = get_alerts(limit=500)(limit=500)
    alerts.append({
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "level": level,
        "message": message,
        "details": details,
    })
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "w", encoding="utf-8") as f:
        json.dump(alerts[-200:], f, ensure_ascii=False, indent=0)


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
