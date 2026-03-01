import threading
from datetime import datetime, timedelta

SCHEDULE_LAST_RUN_KEY = "schedule_last_run"


def parse_datetime(date_str: str, time_str: str) -> datetime | None:
    try:
        s = f"{date_str.strip()} {time_str.strip()}"
        return datetime.strptime(s, "%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return None


def parse_time(time_str: str) -> tuple[int, int] | None:
    try:
        parts = str(time_str).strip().split(":")
        if len(parts) >= 2:
            return int(parts[0]), int(parts[1])
    except (ValueError, TypeError):
        pass
    return None


def get_next_run(cfg: dict) -> datetime | None:
    if not cfg.get("schedule_enabled"):
        return None
    t = cfg.get("schedule_type") or "once"
    now = datetime.now()
    if t == "once":
        dt = parse_datetime(
            cfg.get("schedule_once_date", ""),
            cfg.get("schedule_once_time", ""),
        )
        if dt and dt > now:
            return dt
        return None
    if t == "interval":
        minutes = int(cfg.get("schedule_interval_minutes") or 60)
        last = cfg.get(SCHEDULE_LAST_RUN_KEY)
        if last:
            try:
                last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M:%S")
                next_dt = last_dt + timedelta(minutes=minutes)
                if next_dt > now:
                    return next_dt
            except ValueError:
                pass
        return now + timedelta(seconds=10)
    if t == "daily":
        tp = parse_time(cfg.get("schedule_daily_time", "09:00"))
        if tp:
            next_dt = now.replace(hour=tp[0], minute=tp[1], second=0, microsecond=0)
            if next_dt <= now:
                next_dt += timedelta(days=1)
            return next_dt
    return None


def mark_run(cfg: dict) -> None:
    from core import load_config, save_config
    c = load_config() or {}
    c[SCHEDULE_LAST_RUN_KEY] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if cfg.get("schedule_type") == "once":
        c["schedule_enabled"] = False
    save_config(c)


def run_scheduler(callback, stop_event: threading.Event, check_interval_sec: int = 60):
    def loop():
        while not stop_event.is_set():
            try:
                from core import load_config
                cfg = load_config() or {}
                next_run = get_next_run(cfg)
                if next_run and datetime.now() >= next_run:
                    callback()
            except Exception:
                pass
            if stop_event.wait(check_interval_sec):
                break
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t
