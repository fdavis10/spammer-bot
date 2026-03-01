import asyncio
import queue
import sys
import threading
from datetime import datetime
from pathlib import Path
from tkinter import Tk, filedialog

import flet as ft

def _play_notify_sound():
    def _do_play():
        try:
            cfg = load_config() or {}
            sound_path = cfg.get("notify_sound_file", "")
            if not sound_path or not Path(sound_path).exists():
                _assets = Path(getattr(sys, "_MEIPASS", BASE_DIR)) / "assets"
                sound_path = _assets / "notification.mp3"
            if Path(sound_path).exists():
                from playsound3 import playsound
                playsound(str(sound_path), block=False)
            elif sys.platform == "win32":
                import winsound
                winsound.Beep(880, 120)
        except Exception:
            try:
                if sys.platform == "win32":
                    import winsound
                    winsound.Beep(880, 120)
            except Exception:
                pass
    threading.Thread(target=_do_play, daemon=True).start()

def show_notify(page, message: str, is_error: bool = False, duration: int = 3000):
    cfg = load_config() or {}
    if cfg.get("notify_toast", True):
        bg = ft.Colors.ERROR_CONTAINER if is_error else ft.Colors.PRIMARY_CONTAINER
        page.snack_bar = ft.SnackBar(ft.Text(message), bgcolor=bg, duration=duration)
        page.snack_bar.open = True
    if cfg.get("notify_sound", True):
        _play_notify_sound()
    page.update()

from ai_generate import generate_vacancy_text
from auth import clear_remember, is_remembered, save_remember, verify
from broadcast import auth_account, create_client, request_code, run_broadcast, run_dm_broadcast, sign_in_with_code, substitute_variables
from chats import extract_links_from_xlsx, run_join_all_links, run_leave_all_chats
from core import (
    BASE_DIR,
    DATA_DIR,
    TDATA_IMPORT_DIR,
    add_chat_links,
    get_stats,
    add_file_to_storage,
    add_template,
    delete_template,
    get_chat_links,
    get_stats,
    get_templates,
    is_account_authorized,
    load_config,
    save_chat_links,
    save_config,
    MAX_ACCOUNTS,
    SESSIONS_DIR,
)
from dashboard import build_error_log_content, get_alerts_grouped
from errors import ACCORDION_CATEGORIES, get_level_style
from schedule import get_next_run, mark_run, run_scheduler
from tray import run_tray
from tdata_import import add_accounts_to_config, import_tdata_folder, open_tdata_folder
from validate_accounts import validate_all_accounts
WIDTH = 1100
HEIGHT = 700
RESPONSE_QUEUE = queue.Queue()


def build_sidebar(on_nav, selected_index):
    return ft.NavigationRail(
        selected_index=selected_index,
        on_change=lambda e: on_nav(e.control.selected_index),
        label_type=ft.NavigationRailLabelType.ALL,
        min_width=80,
        min_extended_width=200,
        destinations=[
            ft.NavigationRailDestination(icon=ft.Icons.HOME, label="Главная"),
            ft.NavigationRailDestination(icon=ft.Icons.PEOPLE, label="Аккаунты"),
            ft.NavigationRailDestination(icon=ft.Icons.ANALYTICS, label="Статистика"),
            ft.NavigationRailDestination(icon=ft.Icons.NOTIFICATIONS, label="Уведомления"),
            ft.NavigationRailDestination(icon=ft.Icons.PERSON, label="Профиль"),
            ft.NavigationRailDestination(icon=ft.Icons.MESSAGE, label="Сообщения"),
            ft.NavigationRailDestination(icon=ft.Icons.SCHEDULE, label="Расписание"),
            ft.NavigationRailDestination(icon=ft.Icons.MAIL, label="В личку"),
            ft.NavigationRailDestination(icon=ft.Icons.FORUM, label="Чаты"),
        ],
    )


def build_home_view(page, on_nav):
    stats = get_stats()
    cfg = load_config() or {}
    accounts = cfg.get("accounts") or []
    accounts_count = len(accounts)
    auth_count = sum(1 for a in accounts if is_account_authorized(a.get("phone", "")))
    chats_count = len(get_chat_links())
    templates_count = len(get_templates() or [])
    next_run = get_next_run(cfg)
    next_run_str = next_run.strftime("%d.%m.%Y %H:%M") if next_run else "—"

    total = stats["total_sent"] + stats["total_failed"]
    sent_pct = (stats["total_sent"] / total * 100) if total else 50
    failed_pct = (stats["total_failed"] / total * 100) if total else 50

    if total > 0:
        bar_w = 200
        sw = max(8, int(bar_w * sent_pct / 100))
        fw = max(8, int(bar_w * failed_pct / 100))
        chart_row = ft.Column(
            [
                ft.Row(
                    [
                        ft.Text("Отправлено / Ошибок", size=12, weight=ft.FontWeight.W_500),
                        ft.Text(f"{stats['total_sent']} / {stats['total_failed']}", size=12, color=ft.Colors.GREY),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Container(
                    content=ft.Row(
                        [
                            ft.Container(width=sw, height=24, bgcolor=ft.Colors.GREEN, border_radius=4),
                            ft.Container(width=fw, height=24, bgcolor=ft.Colors.ERROR, border_radius=4),
                        ],
                        spacing=2,
                    ),
                ),
            ],
            spacing=4,
        )
    else:
        chart_row = ft.Text("Нет данных рассылки", size=12, color=ft.Colors.GREY)

    last_lines = (stats.get("last_lines") or [])[-5:][::-1]
    last_activity = ft.Column(
        [ft.Text(ln[:70] + ("..." if len(ln) > 70 else ""), size=11, color=ft.Colors.ON_SURFACE_VARIANT) for ln in last_lines] if last_lines else [ft.Text("—", size=12, color=ft.Colors.GREY)],
        spacing=2,
    )

    def _card(title, value, icon, color=ft.Colors.PRIMARY, on_click=None):
        c = ft.Container(
            content=ft.Column(
                [
                    ft.Icon(icon, color=color, size=28),
                    ft.Text(str(value), size=24, weight=ft.FontWeight.BOLD),
                    ft.Text(title, size=12, color=ft.Colors.GREY),
                ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=4,
            ),
            padding=20,
            width=140,
            border_radius=12,
            bgcolor=ft.Colors.SURFACE_CONTAINER,
        )
        if on_click:
            return ft.GestureDetector(content=ft.Card(content=c), on_tap=lambda e: on_nav(on_click))
        return ft.Card(content=c)

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Обзор", size=20, weight=ft.FontWeight.BOLD),
                ft.Divider(height=1),
                ft.Row(
                    [
                        _card("Отправлено", stats["total_sent"], ft.Icons.CHECK_CIRCLE, ft.Colors.GREEN, 2),
                        _card("Ошибок", stats["total_failed"], ft.Icons.ERROR, ft.Colors.ERROR, 2),
                        _card("Аккаунтов", f"{auth_count}/{accounts_count}", ft.Icons.PEOPLE, ft.Colors.PRIMARY, 1),
                        _card("Чатов", chats_count, ft.Icons.FORUM, ft.Colors.TEAL, 8),
                        _card("Шаблонов", templates_count, ft.Icons.BOOKMARK, ft.Colors.AMBER, 5),
                    ],
                    spacing=12,
                    wrap=True,
                ),
                ft.Container(height=12),
                ft.Text("Рассылка: успех / ошибки", size=14, weight=ft.FontWeight.W_500),
                chart_row,
                ft.Container(height=12),
                ft.Row(
                    [
                        ft.Column(
                            [
                                ft.Text("Следующая рассылка", size=14, weight=ft.FontWeight.W_500),
                                ft.Text(next_run_str, size=12, color=ft.Colors.GREY),
                            ],
                            spacing=2,
                            expand=True,
                        ),
                        ft.TextButton("Расписание →", on_click=lambda e: on_nav(6)),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Container(height=12),
                ft.Text("Последняя активность", size=14, weight=ft.FontWeight.W_500),
                ft.Container(
                    content=last_activity,
                    padding=12,
                    bgcolor=ft.Colors.SURFACE_CONTAINER,
                    border_radius=8,
                ),
                ft.TextButton("Вся статистика →", on_click=lambda e: on_nav(2)),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
            spacing=8,
        ),
        padding=20,
    )


def build_accounts_view(page):
    cfg = load_config()
    accounts_list = ft.Column(scroll=ft.ScrollMode.AUTO, spacing=8)

    def refresh_list():
        accounts_list.controls.clear()
        cfg = load_config()
        if not cfg or not cfg.get("accounts"):
            accounts_list.controls.append(
                ft.Text("Нет аккаунтов. Добавь аккаунт.", color=ft.Colors.GREY)
            )
        else:
            for i, acc in enumerate(cfg["accounts"]):
                is_auth = is_account_authorized(acc["phone"])
                is_valid = acc.get("validated", False)
                proxy_tag = ft.Text("Прокси", size=10, color=ft.Colors.TEAL) if acc.get("proxy") else ft.Container()
                valid_tag = ft.Text("Валидный", size=10, color=ft.Colors.GREEN) if is_valid else ft.Container()
                accounts_list.controls.append(
                    ft.Card(
                        content=ft.Container(
                            content=ft.Row(
                                [
                                    ft.Row(
                                        [
                                            ft.Icon(ft.Icons.PHONE_ANDROID, color=ft.Colors.BLUE),
                                            ft.Icon(ft.Icons.CHECK_CIRCLE, color=ft.Colors.GREEN, size=20) if is_auth else ft.Container(width=20),
                                            ft.Icon(ft.Icons.VERIFIED_USER, color=ft.Colors.TEAL, size=18) if is_valid else ft.Container(width=18),
                                        ],
                                        spacing=4,
                                    ),
                                    ft.Column(
                                        [
                                            ft.Text(acc["phone"], weight=ft.FontWeight.W_600),
                                            ft.Row([
                                                ft.Text("2FA: ***" if acc.get("password") else "2FA: —", size=12, color=ft.Colors.GREY),
                                                proxy_tag,
                                                valid_tag,
                                            ], spacing=8),
                                        ],
                                        spacing=2,
                                    ),
                                    ft.Row([
                                        ft.IconButton(icon=ft.Icons.EDIT, tooltip="Редактировать", on_click=lambda e, idx=i: edit_account_dialog(e, idx)),
                                        ft.IconButton(icon=ft.Icons.DELETE, on_click=lambda e, idx=i: delete_account(idx)),
                                    ], spacing=0),
                                ],
                                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                            ),
                            padding=12,
                        )
                    )
                )
        page.update()

    def delete_account(idx):
        cfg = load_config()
        if cfg and 0 <= idx < len(cfg["accounts"]):
            cfg["accounts"].pop(idx)
            save_config(cfg)
            refresh_list()

    def _proxy_fields():
        ph = ft.TextField(label="Прокси: хост", hint_text="IP или домен", width=200)
        pp = ft.TextField(label="Порт", hint_text="1080", width=80, keyboard_type=ft.KeyboardType.NUMBER)
        pt = ft.Dropdown(label="Тип", width=100, value="socks5", options=[ft.dropdown.Option("socks5", "SOCKS5"), ft.dropdown.Option("socks4", "SOCKS4"), ft.dropdown.Option("http", "HTTP")])
        pu = ft.TextField(label="Логин (опц.)", width=140)
        pw = ft.TextField(label="Пароль (опц.)", password=True, width=140)
        return ph, pp, pt, pu, pw

    def _account_dialog(p, title, on_save, initial_phone="", initial_pwd="", initial_proxy=None):
        phone_field = ft.TextField(label="Телефон", hint_text="+7...", width=300, value=initial_phone)
        pwd_field = ft.TextField(label="Пароль 2FA", hint_text="пусто если нет", password=True, width=300, value=initial_pwd)
        proxy_host, proxy_port, proxy_type, proxy_user, proxy_pass = _proxy_fields()
        if initial_proxy and isinstance(initial_proxy, dict):
            proxy_host.value = initial_proxy.get("host", "")
            proxy_port.value = str(initial_proxy.get("port", ""))
            proxy_type.value = initial_proxy.get("type", "socks5")
            proxy_user.value = initial_proxy.get("username", "")
            proxy_pass.value = initial_proxy.get("password", "")
        proxy_row = ft.Row([
            ft.Text("Прокси (опционально)", size=12, color=ft.Colors.GREY),
        ])
        proxy_fields = ft.Column([
            ft.Row([proxy_host, proxy_port, proxy_type]),
            ft.Row([proxy_user, proxy_pass]),
        ], tight=True)
        content = ft.Column([phone_field, pwd_field, proxy_row, proxy_fields], tight=True)
        dlg = ft.AlertDialog(modal=True, title=ft.Text(title), content=content, actions=[])
        def save_click(ev):
            phone = (phone_field.value or "").strip()
            if not phone:
                return
            proxy = None
            if (proxy_host.value or "").strip():
                try:
                    port = int((proxy_port.value or "1080").strip()) or 1080
                except ValueError:
                    port = 1080
                proxy = {
                    "type": proxy_type.value or "socks5",
                    "host": proxy_host.value.strip(),
                    "port": port,
                    "username": (proxy_user.value or "").strip() or None,
                    "password": (proxy_pass.value or "").strip() or None,
                }
            on_save(ev, phone, pwd_field.value or "", proxy)
            p.pop_dialog()
            p.update()
            refresh_list()
        dlg.actions = [ft.TextButton("Отмена", on_click=lambda ev: (p.pop_dialog(), p.update())), ft.Button("Сохранить", on_click=save_click)]
        p.show_dialog(dlg)
        p.update()

    def add_account_dialog(e):
        p = e.page
        def on_save(ev, phone, pwd, proxy):
            cfg = load_config() or {"api_id": 0, "api_hash": "", "accounts": [], "message": ""}
            acc = {"phone": phone, "password": pwd}
            if proxy:
                acc["proxy"] = proxy
            cfg.setdefault("accounts", []).append(acc)
            save_config(cfg)
        _account_dialog(p, "Добавить аккаунт", lambda ev, ph, pw, px: on_save(ev, ph, pw, px))

    def edit_account_dialog(e, idx):
        cfg = load_config()
        if not cfg or not (0 <= idx < len(cfg["accounts"])):
            return
        acc = cfg["accounts"][idx]
        p = e.page
        def on_save(ev, phone, pwd, proxy):
            cfg = load_config()
            if cfg and 0 <= idx < len(cfg["accounts"]):
                cfg["accounts"][idx] = {"phone": phone, "password": pwd}
                if proxy:
                    cfg["accounts"][idx]["proxy"] = proxy
                elif "proxy" in cfg["accounts"][idx]:
                    del cfg["accounts"][idx]["proxy"]
                save_config(cfg)
        _account_dialog(p, "Редактировать аккаунт", on_save, acc["phone"], acc.get("password", ""), acc.get("proxy"))

    def tdata_auth_dialog(e):
        cfg = load_config()
        if not cfg or not cfg.get("api_id") or not cfg.get("api_hash"):
            def close_err(ev):
                ev.page.pop_dialog()
                ev.page.update()
            err_dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Ошибка"),
                content=ft.Text("Сначала настройте api_id и api_hash в Профиле."),
                actions=[ft.Button("OK", on_click=close_err)],
            )
            e.page.show_dialog(err_dlg)
            e.page.update()
            return
        p = e.page
        tdata_path = str(TDATA_IMPORT_DIR.resolve())
        log_text = ft.Text("", size=12, selectable=True, expand=True)
        log_container = ft.Container(
            content=ft.Column([ft.Text("Результат:", size=12, weight=ft.FontWeight.W_500), log_text], scroll=ft.ScrollMode.AUTO, height=160),
        )
        progress_text = ft.Text("", size=12, color=ft.Colors.GREY)
        content = ft.Column([
            ft.Text("Скопируйте папки tdata в указанную папку.", size=14),
            ft.Text("Структура: каждая подпапка с tdata внутри = один аккаунт.", size=12, color=ft.Colors.GREY),
            ft.Text("Пример: Account1/tdata, Account2/tdata — или одна папка tdata напрямую.", size=12, color=ft.Colors.GREY),
            ft.Text(tdata_path, size=11, color=ft.Colors.PRIMARY, selectable=True, overflow=ft.TextOverflow.ELLIPSIS),
            ft.Row([
                ft.Button("Открыть папку", icon=ft.Icons.FOLDER_OPEN, on_click=lambda ev: (open_tdata_folder(), ev.page.update())),
                ft.Button("Импортировать", icon=ft.Icons.UPLOAD, on_click=lambda ev: None),
            ], spacing=12),
            progress_text,
            log_container,
        ], spacing=12)
        dlg = ft.AlertDialog(modal=True, title=ft.Text("Авторизация через tdata"), content=content, actions=[])
        import_btn = content.controls[4].controls[1]
        def do_import(ev):
            import_btn.disabled = True
            progress_text.value = "Импорт..."
            p.update()
            def run():
                def on_prog(ident, msg, ok):
                    def upd():
                        log_text.value = (log_text.value or "") + f"{ident}: {msg}\n"
                        progress_text.value = "Обработка..."
                        p.update()
                    _run_on_page(p, upd)
                try:
                    phones = asyncio.run(import_tdata_folder(
                        TDATA_IMPORT_DIR, cfg["api_id"], cfg["api_hash"], on_progress=on_prog
                    ))
                    def done():
                        import_btn.disabled = False
                        progress_text.value = f"Готово: импортировано {len(phones)} аккаунт(ов)"
                        add_accounts_to_config(phones)
                        refresh_list()
                        show_notify(p, f"Импорт tdata: {len(phones)} аккаунт(ов)")
                        p.update()
                    _run_on_page(p, done)
                except Exception as ex:
                    def err():
                        import_btn.disabled = False
                        progress_text.value = f"Ошибка: {ex}"
                        show_notify(p, f"Ошибка импорта: {ex}", is_error=True)
                        p.update()
                    _run_on_page(p, err)
            threading.Thread(target=run, daemon=True).start()
        import_btn.on_click = do_import
        def on_cancel(ev):
            ev.page.pop_dialog()
            ev.page.update()
        dlg.actions = [ft.TextButton("Закрыть", on_click=on_cancel)]
        p.show_dialog(dlg)
        p.update()

    def clear_accounts_dialog(e):
        cfg = load_config()
        count = len((cfg or {}).get("accounts") or [])
        if count == 0:
            show_notify(e.page, "Нет аккаунтов для очистки.", is_error=False)
            return

        def confirm_clear(ev):
            c = load_config() or {}
            c["accounts"] = []
            save_config(c)
            ev.page.pop_dialog()
            refresh_list()
            show_notify(ev.page, f"Удалено {count} аккаунт(ов).")

        def cancel(ev):
            ev.page.pop_dialog()
            ev.page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Очистить все аккаунты?"),
            content=ft.Text(f"Будет удалено {count} аккаунт(ов) из списка. Сессии авторизации не удаляются."),
            actions=[
                ft.TextButton("Отмена", on_click=cancel),
                ft.Button("Удалить всё", icon=ft.Icons.DELETE, on_click=confirm_clear),
            ],
        )
        e.page.show_dialog(dlg)
        e.page.update()

    def validate_accounts_dialog(e):
        cfg = load_config()
        if not cfg or not cfg.get("api_id") or not cfg.get("api_hash"):
            def close_err(ev):
                ev.page.pop_dialog()
                ev.page.update()
            err_dlg = ft.AlertDialog(
                modal=True, title=ft.Text("Ошибка"),
                content=ft.Text("Сначала настройте api_id и api_hash в Профиле."),
                actions=[ft.Button("OK", on_click=close_err)],
            )
            e.page.show_dialog(err_dlg)
            e.page.update()
            return
        authorized = [a for a in (cfg.get("accounts") or []) if is_account_authorized(a["phone"])]
        if not authorized:
            def close_err(ev):
                ev.page.pop_dialog()
                ev.page.update()
            err_dlg = ft.AlertDialog(
                modal=True, title=ft.Text("Ошибка"),
                content=ft.Text("Нет авторизованных аккаунтов для проверки."),
                actions=[ft.Button("OK", on_click=close_err)],
            )
            e.page.show_dialog(err_dlg)
            e.page.update()
            return
        p = e.page
        link_field = ft.TextField(label="Ссылка на тестовую группу", hint_text="t.me/group или t.me/joinchat/xxx", width=360, value=cfg.get("validation_test_link", ""))
        msg_field = ft.TextField(label="Тестовое сообщение", value=cfg.get("validation_test_message", "Тест валидации"), width=360)
        progress_bar = ft.ProgressBar(visible=False, expand=True)
        counter_text = ft.Text("", size=14, color=ft.Colors.PRIMARY)
        log_text = ft.Text("", size=12, selectable=True, expand=True)
        log_container = ft.Container(content=ft.Column([ft.Text("Результаты:", size=12, weight=ft.FontWeight.W_500), log_text], scroll=ft.ScrollMode.AUTO, height=180))
        run_btn = ft.Button("Запустить проверку", icon=ft.Icons.PLAY_ARROW)
        content = ft.Column([
            ft.Text("Для каждого аккаунта: вступление в группу → отправка сообщения → выход.", size=12, color=ft.Colors.GREY),
            link_field,
            msg_field,
            ft.Row([progress_bar, counter_text], alignment=ft.MainAxisAlignment.START, spacing=12),
            log_container,
            run_btn,
        ], spacing=12)
        dlg = ft.AlertDialog(modal=True, title=ft.Text("Проверка на валидность"), content=content, actions=[])

        def do_run(ev):
            link = (link_field.value or "").strip()
            if not link:
                show_notify(p, "Введите ссылку на тестовую группу", is_error=False)
                return
            run_btn.visible = False
            progress_bar.visible = True
            counter_text.value = "0 валидных, 0 ошибок из 0"
            log_text.value = ""
            p.update()
            c = load_config()
            c["validation_test_link"] = link
            c["validation_test_message"] = msg_field.value or "Тест"
            save_config(c)

            def on_prog(validated, failed, total, phone, success, message):
                def upd():
                    done = validated + failed
                    progress_bar.value = done / total if total else 0
                    counter_text.value = f"{validated} валидных, {failed} ошибок из {total}"
                    log_text.value = (log_text.value or "") + f"{phone}: {'✓' if success else '✗'} {message}\n"
                    p.update()
                _run_on_page(p, upd)

            def run():
                try:
                    results = asyncio.run(validate_all_accounts(link, msg_field.value or "Тест", on_progress=on_prog))
                    def done():
                        run_btn.visible = True
                        progress_bar.visible = False
                        v_count = sum(1 for r in results if r["valid"])
                        f_count = len(results) - v_count
                        counter_text.value = f"Готово: {v_count} валидных, {f_count} ошибок"
                        refresh_list()
                        show_notify(p, f"Проверка валидности: {v_count} валидных, {f_count} ошибок", duration=5000)
                        p.update()
                    _run_on_page(p, done)
                except Exception as ex:
                    def err():
                        run_btn.visible = True
                        progress_bar.visible = False
                        counter_text.value = f"Ошибка: {ex}"
                        show_notify(p, f"Ошибка проверки: {ex}", is_error=True)
                        p.update()
                    _run_on_page(p, err)
            threading.Thread(target=run, daemon=True).start()

        run_btn.on_click = do_run
        def on_cancel(ev):
            ev.page.pop_dialog()
            ev.page.update()
        dlg.actions = [ft.TextButton("Закрыть", on_click=on_cancel)]
        p.show_dialog(dlg)
        p.update()

    def auth_account_dialog(e):
        cfg = load_config()
        if not cfg or not cfg.get("accounts") or not cfg.get("api_id") or not cfg.get("api_hash"):
            def close_err(ev):
                ev.page.pop_dialog()
                ev.page.update()
            err_dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Ошибка"),
                content=ft.Text("Сначала настрой api_id и api_hash через manage.py setup"),
                actions=[ft.Button("OK", on_click=close_err)],
            )
            e.page.show_dialog(err_dlg)
            e.page.update()
            return
        opts = [ft.dropdown.Option(acc["phone"]) for acc in cfg["accounts"]]
        dd = ft.Dropdown(label="Аккаунт", options=opts, width=280)
        code_field = ft.TextField(label="Код из Telegram или SMS", width=280, visible=False)
        status = ft.Text("", color=ft.Colors.GREY, size=12)
        request_btn = ft.Button("Запросить код", icon=ft.Icons.SMS)
        dlg = ft.AlertDialog(title=ft.Text("Авторизация"), content=ft.Column([dd, request_btn, code_field, status], tight=True), actions=[])

        def on_acc_change(e):
            code_field.visible = True
            code_field.value = ""
            status.value = "Нажмите «Запросить код», дождитесь SMS/Telegram, введите код и «Войти»"
            page.update()

        def do_request_code(e):
            phone = dd.value
            if not phone:
                status.value = "Выберите аккаунт"
                page.update()
                return
            if is_account_authorized(phone):
                status.value = "Аккаунт уже авторизован"
                page.update()
                return
            status.value = "Отправка кода..."
            request_btn.disabled = True
            page.update()
            session_path = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
            acc = next((a for a in cfg["accounts"] if a["phone"] == phone), None)
            proxy = (acc or {}).get("proxy") or cfg.get("proxy")
            client = create_client(session_path, cfg["api_id"], cfg["api_hash"], proxy)
            def run():
                try:
                    asyncio.run(request_code(client, phone))
                    _run_on_page(page, lambda: (setattr(status, "value", "Код отправлен. Введите его выше и нажмите «Войти»"), setattr(request_btn, "disabled", False), page.update()))
                except Exception as ex:
                    _run_on_page(page, lambda msg=str(ex): (setattr(status, "value", f"Ошибка: {msg}"), setattr(request_btn, "disabled", False), page.update()))
            threading.Thread(target=run).start()

        def do_auth(e):
            phone = dd.value
            if not phone:
                return
            if is_account_authorized(phone):
                def close_warn(ev):
                    ev.page.pop_dialog()
                    ev.page.pop_dialog()
                    ev.page.update()
                warn_dlg = ft.AlertDialog(
                    modal=True,
                    title=ft.Text("Аккаунт уже авторизован"),
                    content=ft.Text("Этот аккаунт уже авторизован."),
                    actions=[ft.Button("OK", on_click=close_warn)],
                )
                e.page.show_dialog(warn_dlg)
                e.page.update()
                return
            code = code_field.value.strip()
            if not code:
                status.value = "Сначала запросите код и введите его"
                page.update()
                return
            e.page.pop_dialog()
            e.page.update()
            acc = next((a for a in cfg["accounts"] if a["phone"] == phone), None)
            if not acc:
                return
            session_path = SESSIONS_DIR / phone.replace("+", "").replace(" ", "")
            proxy = (acc or {}).get("proxy") or cfg.get("proxy")
            client = create_client(session_path, cfg["api_id"], cfg["api_hash"], proxy)
            def run():
                try:
                    asyncio.run(sign_in_with_code(client, phone, code, acc.get("password", "")))
                    _run_on_page(page, lambda: (refresh_list(), page.update()))
                except Exception as ex:
                    err = str(ex)
                    def show_err():
                        show_notify(page, f"Ошибка: {err}", is_error=True)
                        refresh_list()
                        page.update()
                    _run_on_page(page, show_err)
            threading.Thread(target=run).start()

        request_btn.on_click = do_request_code
        dd.on_select = on_acc_change
        def cancel_auth(ev):
            ev.page.pop_dialog()
            ev.page.update()
        dlg.actions = [
            ft.TextButton("Отмена", on_click=cancel_auth),
            ft.Button("Войти", on_click=do_auth),
        ]
        page.show_dialog(dlg)
        page.update()

    refresh_list()

    return ft.Container(
        content=ft.Column(
            [
                ft.Row(
                    [
                        ft.Text("Аккаунты", size=24, weight=ft.FontWeight.BOLD),
                        ft.Row([
                            ft.Button("Добавить", icon=ft.Icons.ADD, on_click=add_account_dialog),
                            ft.Button("Авторизовать", icon=ft.Icons.LOGIN, on_click=auth_account_dialog),
                            ft.Button("Через tdata", icon=ft.Icons.FOLDER_OPEN, on_click=tdata_auth_dialog),
                            ft.Button("Проверить валидность", icon=ft.Icons.VERIFIED_USER, on_click=validate_accounts_dialog),
                            ft.OutlinedButton("Очистить аккаунты", icon=ft.Icons.DELETE_SWEEP, on_click=clear_accounts_dialog),
                        ], spacing=8, wrap=True),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    wrap=True,
                ),
                ft.Divider(),
                accounts_list,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def _level_color(level: str):
    style = get_level_style(level)
    color_map = {"error": ft.Colors.ERROR, "orange": ft.Colors.ORANGE, "primary": ft.Colors.PRIMARY}
    return color_map.get(style.color_key, ft.Colors.PRIMARY)


def _level_icon(level: str):
    style = get_level_style(level)
    icon_map = {"ERROR": ft.Icons.ERROR, "ERROR_OUTLINE": ft.Icons.ERROR_OUTLINE, "WARNING_AMBER": ft.Icons.WARNING_AMBER, "INFO_OUTLINE": ft.Icons.INFO_OUTLINE}
    return icon_map.get(style.icon_key, ft.Icons.INFO_OUTLINE)


def build_notifications_view(page):
    grouped = get_alerts_grouped(limit=100)
    category_labels = {c.key: (c.title, c.subtitle) for c in ACCORDION_CATEGORIES.values()}
    panels = []
    for cat_key, (title, subtitle) in category_labels.items():
        items = grouped.get(cat_key) or []
        content_list = ft.Column(spacing=4, scroll=ft.ScrollMode.AUTO)
        if not items:
            content_list.controls.append(ft.Text("Нет уведомлений", size=12, color=ft.Colors.GREY, italic=True))
        else:
            for a in items:
                ts = a.get("ts", "")
                msg = a.get("message", "")
                details = a.get("details", "")
                lvl = a.get("level", "info")
                color = _level_color(lvl)
                icon = _level_icon(lvl)
                row = ft.Container(
                    content=ft.Row(
                        [
                            ft.Icon(icon, color=color, size=18),
                            ft.Column(
                                [
                                    ft.Text(msg, size=13, weight=ft.FontWeight.W_500),
                                    ft.Text(details, size=11, color=ft.Colors.GREY_700, overflow=ft.TextOverflow.ELLIPSIS, max_lines=2),
                                    ft.Text(ts, size=10, color=ft.Colors.GREY_600),
                                ],
                                spacing=2,
                                expand=True,
                            ),
                        ],
                        spacing=8,
                    ),
                    padding=8,
                    bgcolor=ft.Colors.SURFACE_CONTAINER_LOWEST,
                    border_radius=6,
                )
                content_list.controls.append(row)
        panel = ft.ExpansionPanel(
            header=ft.ListTile(
                title=ft.Text(title, weight=ft.FontWeight.W_600),
                subtitle=ft.Text(f"{len(items)} уведомлений" if items else "Нет уведомлений"),
            ),
            content=ft.Container(content=content_list, height=min(400, 100 + len(items) * 70)),
        )
        panels.append(panel)
    accordion = ft.ExpansionPanelList(
        expand_icon_color=ft.Colors.PRIMARY,
        elevation=2,
        controls=panels,
    )

    def download_error_log(e):
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.asksaveasfilename(
            title="Сохранить лог для разработчика",
            defaultextension=".txt",
            filetypes=[("Текст", "*.txt"), ("Все файлы", "*.*")],
            initialfile=f"spammer-bot-errors-{datetime.now().strftime('%Y%m%d-%H%M%S')}.txt",
        )
        root.destroy()
        if not path:
            return
        try:
            content = build_error_log_content()
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            show_notify(page, f"Лог сохранён: {path}")
        except Exception as ex:
            show_notify(page, f"Ошибка: {ex}", is_error=True)
        page.update()

    download_btn = ft.OutlinedButton(
        "Скачать полный лог для разработчика",
        icon=ft.Icons.DOWNLOAD,
        on_click=download_error_log,
    )

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Уведомления", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Text("Классифицированные уведомления о рассылках и ошибках.", size=12, color=ft.Colors.GREY),
                ft.Row([download_btn], alignment=ft.MainAxisAlignment.START),
                accordion,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_stats_view(page):
    stats = get_stats()
    rows = [ft.DataRow(cells=[ft.DataCell(ft.Text(line[:80] + "..." if len(line) > 80 else line))]) for line in stats["last_lines"][-20:][::-1]]
    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Статистика", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Row(
                    [
                        ft.Card(
                            content=ft.Container(
                                ft.Column([
                                    ft.Text("Отправлено", size=14, color=ft.Colors.GREY),
                                    ft.Text(str(stats["total_sent"]), size=32, weight=ft.FontWeight.BOLD, color=ft.Colors.GREEN),
                                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                                padding=24, width=160,
                            ),
                        ),
                        ft.Card(
                            content=ft.Container(
                                ft.Column([
                                    ft.Text("Ошибок", size=14, color=ft.Colors.GREY),
                                    ft.Text(str(stats["total_failed"]), size=32, weight=ft.FontWeight.BOLD, color=ft.Colors.RED),
                                ], horizontal_alignment=ft.CrossAxisAlignment.CENTER),
                                padding=24, width=160,
                            ),
                        ),
                    ],
                    spacing=16,
                ),
                ft.Text("Последние записи лога", size=16, weight=ft.FontWeight.W_500),
                ft.Container(
                    content=ft.DataTable(columns=[ft.DataColumn(ft.Text("Лог"))], rows=rows or [ft.DataRow(cells=[ft.DataCell(ft.Text("—"))])]),
                    border=ft.Border.all(1, ft.Colors.OUTLINE),
                    border_radius=8,
                ),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_chats_view(page, on_refresh=None):
    cfg = load_config()
    status_text = ft.Text("", size=12, color=ft.Colors.GREY)
    links_list = ft.Column(scroll=ft.ScrollMode.AUTO, spacing=4)
    pb = ft.ProgressBar(visible=False)
    search_field = ft.TextField(
        hint_text="Поиск по ссылке...",
        width=320,
        on_change=lambda e: refresh_links(),
    )

    MAX_LINKS_DISPLAY = 10
    MAX_LINKS_WHEN_FILTERED = 100

    def refresh_links():
        links_list.controls.clear()
        links = get_chat_links()
        search_term = (search_field.value or "").strip().lower()
        if search_term:
            links = [lnk for lnk in links if search_term in (lnk or "").lower()]
        if not links:
            links_list.controls.append(
                ft.Text("Нет ссылок." + (" Ничего не найдено по запросу." if search_term else " Импортируйте из xlsx."), color=ft.Colors.GREY)
            )
        else:
            limit = MAX_LINKS_WHEN_FILTERED if search_term else MAX_LINKS_DISPLAY
            to_show = links[:limit]
            for link in to_show:
                links_list.controls.append(
                    ft.Row(
                        [
                            ft.Text(link, size=12, overflow=ft.TextOverflow.ELLIPSIS, expand=True),
                            ft.IconButton(icon=ft.Icons.DELETE, icon_size=18, on_click=lambda e, lnk=link: _delete_link(lnk)),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    )
                )
            if len(links) > limit:
                links_list.controls.append(ft.Text(f"Показано {limit} из {len(links)}", size=12, color=ft.Colors.GREY))
        page.update()

    def _delete_link(link):
        links = get_chat_links()
        if link in links:
            links.remove(link)
            save_chat_links(links)
            refresh_links()

    def clear_all_links_dialog(e):
        links = get_chat_links()
        count = len(links)
        if count == 0:
            show_notify(page, "Нет ссылок для удаления.", is_error=False)
            return

        def confirm_clear(ev):
            save_chat_links([])
            ev.page.pop_dialog()
            refresh_links()
            if on_refresh:
                on_refresh()
            status_text.value = f"Удалено {count} ссылок"
            show_notify(ev.page, f"Удалено {count} ссылок.")

        def cancel(ev):
            ev.page.pop_dialog()
            ev.page.update()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Удалить все ссылки?"),
            content=ft.Text(f"Будет удалено {count} сохранённых ссылок. Это действие нельзя отменить."),
            actions=[
                ft.TextButton("Отмена", on_click=cancel),
                ft.Button("Удалить всё", icon=ft.Icons.DELETE, on_click=confirm_clear),
            ],
        )
        page.show_dialog(dlg)
        page.update()

    def do_import(e):
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askopenfilename(
            title="Выберите xlsx файл",
            filetypes=[("Excel файлы", "*.xlsx"), ("Все файлы", "*.*")],
        )
        root.destroy()
        if not path:
            return

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Импорт ссылок"),
            content=ft.Column(
                [
                    ft.ProgressBar(visible=True),
                    ft.Text("Обработка файла...", size=14),
                ],
                tight=True,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            ),
            actions=[],
        )
        page.show_dialog(dlg)
        page.update()

        def do_extract():
            try:
                new_links = extract_links_from_xlsx(path)
                add_chat_links(new_links)
                def done():
                    page.pop_dialog()
                    status_text.value = f"Импортировано {len(new_links)} ссылок"
                    refresh_links()
                    if on_refresh:
                        on_refresh()
                    show_notify(page, f"Импорт xlsx: {len(new_links)} ссылок")
                    page.update()
                _run_on_page(page, done)
            except Exception as ex:
                def err():
                    page.pop_dialog()
                    status_text.value = f"Ошибка: {ex}"
                    show_notify(page, f"Ошибка импорта: {ex}", is_error=True)
                    page.update()
                _run_on_page(page, err)

        threading.Thread(target=do_extract, daemon=True).start()

    def show_join_dialog(links):
        if not links:
            status_text.value = "Нет ссылок для вступления"
            page.update()
            return
        lines = [ft.Text(f"• {link}", size=12) for link in links[:25]]
        if len(links) > 25:
            lines.append(ft.Text(f"... и ещё {len(links) - 25}", size=12, color=ft.Colors.GREY))
        content = ft.Column(
            [ft.Text(f"Ссылок: {len(links)}", weight=ft.FontWeight.W_600)] + lines,
            scroll=ft.ScrollMode.AUTO,
            height=280,
        )

        def on_close(e):
            e.page.pop_dialog()
            e.page.update()

        def on_join(e):
            e.page.pop_dialog()
            page.update()

            counter_text = ft.Text("0 вступлений, 0 ошибок", size=14)
            cancel_event = threading.Event()

            def on_progress(joined, failed):
                def update():
                    counter_text.value = f"{joined} вступлений, {failed} ошибок"
                    page.update()
                _run_on_page(page, update)

            def on_stop(e):
                cancel_event.set()
                stop_btn.disabled = True
                stop_btn.text = "Остановка..."
                page.update()

            stop_btn = ft.TextButton("Остановить", on_click=on_stop)
            progress_dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Вступление в чаты"),
                content=ft.Column(
                    [
                        ft.ProgressBar(visible=True),
                        counter_text,
                    ],
                    tight=True,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                actions=[stop_btn],
            )
            page.show_dialog(progress_dlg)
            page.update()

            def do_join_thread():
                try:
                    results = asyncio.run(run_join_all_links(links, on_progress=on_progress, cancel_event=cancel_event))
                    total_joined = sum(r["joined"] for r in results)
                    total_failed = sum(r["failed"] for r in results)
                    summary = "\n".join([f"{r['phone']}: вступил в {r['joined']}, ошибок {r['failed']}" for r in results])
                    def done():
                        page.pop_dialog()
                        status_text.value = f"Готово. Вступлений: {total_joined}, ошибок: {total_failed}"
                        show_notify(page, summary, duration=5000)
                        page.update()
                    _run_on_page(page, done)
                except Exception as ex:
                    def err():
                        page.pop_dialog()
                        status_text.value = f"Ошибка: {ex}"
                        show_notify(page, f"Ошибка: {ex}", is_error=True)
                        page.update()
                    _run_on_page(page, err)

            threading.Thread(target=do_join_thread, daemon=True).start()

        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Ссылки на чаты"),
            content=content,
            actions=[
                ft.TextButton("Закрыть", on_click=on_close),
                ft.Button("Вступить со всех аккаунтов", on_click=on_join),
            ],
        )
        page.show_dialog(dlg)
        page.update()

    def show_leave_dialog(e):
        authorized = [a for a in (cfg.get("accounts") or []) if is_account_authorized(a["phone"])]
        if not authorized:
            status_text.value = "Нет авторизованных аккаунтов"
            page.update()
            return
        if not cfg.get("api_id") or not cfg.get("api_hash"):
            status_text.value = "Настройте api_id и api_hash в Профиле"
            page.update()
            return
        confirm_dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Выход из всех чатов"),
            content=ft.Text(
                f"Все {len(authorized)} аккаунт(ов) выйдут из всех групп и каналов. "
                "Избранное (Saved Messages) не затрагивается.\n\nПродолжить?"
            ),
            actions=[
                ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())),
                ft.Button("Да, выйти", icon=ft.Icons.EXIT_TO_APP, on_click=lambda ev: _do_leave(ev)),
            ],
        )
        def _do_leave(ev):
            ev.page.pop_dialog()
            page.update()
            counter_text = ft.Text("0 выходов, 0 ошибок", size=14)
            cancel_event = threading.Event()

            def on_progress(left, failed):
                def update():
                    counter_text.value = f"{left} выходов, {failed} ошибок"
                    page.update()
                _run_on_page(page, update)

            def on_stop(ev2):
                cancel_event.set()
                stop_btn.disabled = True
                stop_btn.text = "Остановка..."
                page.update()

            stop_btn = ft.TextButton("Остановить", on_click=on_stop)
            progress_dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Выход из чатов"),
                content=ft.Column(
                    [ft.ProgressBar(visible=True), counter_text],
                    tight=True,
                    horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                ),
                actions=[stop_btn],
            )
            page.show_dialog(progress_dlg)
            page.update()

            def do_leave_thread():
                try:
                    results = asyncio.run(run_leave_all_chats(on_progress=on_progress, cancel_event=cancel_event))
                    total_left = sum(r["left"] for r in results)
                    total_failed = sum(r["failed"] for r in results)
                    summary = "\n".join([f"{r['phone']}: вышел из {r['left']}, ошибок {r['failed']}" for r in results])
                    def done():
                        page.pop_dialog()
                        status_text.value = f"Готово. Выходов: {total_left}, ошибок: {total_failed}"
                        show_notify(page, summary, duration=5000)
                        page.update()
                    _run_on_page(page, done)
                except Exception as ex:
                    def err():
                        page.pop_dialog()
                        status_text.value = f"Ошибка: {ex}"
                        show_notify(page, f"Ошибка: {ex}", is_error=True)
                        page.update()
                    _run_on_page(page, err)
            threading.Thread(target=do_leave_thread, daemon=True).start()
        page.show_dialog(confirm_dlg)
        page.update()

    def do_join(e):
        links = get_chat_links()
        if not links:
            status_text.value = "Сначала импортируйте ссылки из xlsx"
            page.update()
            return
        authorized = [a for a in (cfg.get("accounts") or []) if is_account_authorized(a["phone"])]
        if not authorized:
            status_text.value = "Нет авторизованных аккаунтов"
            page.update()
            return
        if not cfg.get("api_id") or not cfg.get("api_hash"):
            status_text.value = "Настройте api_id и api_hash в Профиле"
            page.update()
            return
        show_join_dialog(links)

    refresh_links()

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Чаты", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Row(
                    [
                        ft.Button("Импорт из xlsx", icon=ft.Icons.UPLOAD_FILE, on_click=do_import),
                        ft.Button("Вступить во все", icon=ft.Icons.GROUP_ADD, on_click=do_join),
                        ft.OutlinedButton("Выйти из всех чатов", icon=ft.Icons.EXIT_TO_APP, on_click=show_leave_dialog),
                        ft.OutlinedButton("Удалить все ссылки", icon=ft.Icons.DELETE_SWEEP, on_click=clear_all_links_dialog),
                    ],
                    spacing=12,
                    alignment=ft.MainAxisAlignment.START,
                    wrap=True,
                ),
                ft.Row([pb, status_text], spacing=8),
                ft.Text("Нажмите «Импорт из xlsx» и выберите файл. Скрипт сохраняет только ссылки (t.me/...).", size=12, color=ft.Colors.GREY),
                ft.Text("Сохранённые ссылки:", size=14, weight=ft.FontWeight.W_500),
                search_field,
                links_list,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_profile_view(page):
    import json as _json
    cfg = load_config() or {}
    theme_dd = ft.Dropdown(
        label="Тема",
        width=200,
        value="system",
        options=[
            ft.dropdown.Option("system", "Системная"),
            ft.dropdown.Option("light", "Светлая"),
            ft.dropdown.Option("dark", "Тёмная"),
        ],
    )
    theme_dd.on_select = lambda e: apply_theme(page, e.control.value)
    notify_toast_cb = ft.Checkbox(label="Уведомления (тост)", value=cfg.get("notify_toast", True))
    notify_sound_cb = ft.Checkbox(label="Звук при действиях", value=cfg.get("notify_sound", True))
    default_sound = str((Path(getattr(sys, "_MEIPASS", BASE_DIR)) / "assets" / "notification.mp3"))
    notify_sound_field = ft.TextField(
        label="Файл звука (mp3)",
        value=cfg.get("notify_sound_file", "") or default_sound,
        hint_text="Путь к notification.mp3",
        width=400,
    )

    def pick_sound_file(e):
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askopenfilename(
            title="Выберите звуковой файл",
            filetypes=[("MP3", "*.mp3"), ("Все файлы", "*.*")],
        )
        root.destroy()
        if path:
            notify_sound_field.value = path
            c = load_config() or {}
            c["notify_sound_file"] = path
            save_config(c)
            show_notify(page, "Звук сохранён")
            page.update()

    def save_notify_settings(e):
        c = load_config() or {}
        c["notify_toast"] = notify_toast_cb.value
        c["notify_sound"] = notify_sound_cb.value
        c["notify_sound_file"] = (notify_sound_field.value or "").strip() or None
        save_config(c)
        show_notify(page, "Настройки уведомлений сохранены")

    api_id_field = ft.TextField(label="api_id", value=str(cfg.get("api_id", "")), width=200)
    api_hash_field = ft.TextField(label="api_hash", value=cfg.get("api_hash", ""), width=280, password=True)
    def save_api(e):
        c = load_config() or {"accounts": [], "message": ""}
        try:
            c["api_id"] = int(api_id_field.value or 0)
        except ValueError:
            pass
        c["api_hash"] = api_hash_field.value or ""
        save_config(c)
        show_notify(page, "Сохранено")

    def do_export(e):
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.asksaveasfilename(
            title="Экспорт настроек",
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
            initialfile="spammer-bot-config-backup.json",
        )
        root.destroy()
        if not path:
            return
        try:
            c = load_config()
            if not c:
                show_notify(page, "Нет настроек для экспорта", is_error=True)
            else:
                with open(path, "w", encoding="utf-8") as f:
                    _json.dump(c, f, ensure_ascii=False, indent=2)
                show_notify(page, f"Настройки экспортированы: {path}")
        except Exception as ex:
            show_notify(page, f"Ошибка: {ex}", is_error=True)
        page.update()

    def do_import(e):
        root = Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askopenfilename(
            title="Импорт настроек",
            filetypes=[("JSON", "*.json"), ("Все файлы", "*.*")],
        )
        root.destroy()
        if not path:
            return
        try:
            with open(path, encoding="utf-8") as f:
                imported = _json.load(f)
            if not isinstance(imported, dict):
                raise ValueError("Неверный формат файла")
            save_config(imported)
            show_notify(page, "Настройки импортированы. Обновите страницу.")
        except Exception as ex:
            show_notify(page, f"Ошибка: {ex}", is_error=True)
        page.update()

    def show_reset_password_dialog(e):
        from auth import AUTH_FILE
        from core import DATA_DIR
        p = e.page
        info = ft.Text(
            "Сброс пароля удалит текущий пароль. После этого необходимо закрыть приложение и перезапустить его — при следующем входе будет предложено задать новый пароль.\n\n"
            "Или выполните в терминале:\npython manage.py reset-password",
            size=12,
            color=ft.Colors.GREY,
        )
        def do_reset(ev):
            ev.page.pop_dialog()
            ev.page.update()
            try:
                if AUTH_FILE.exists():
                    AUTH_FILE.unlink()
                if (DATA_DIR / ".remember").exists():
                    (DATA_DIR / ".remember").unlink()
                show_notify(ev.page, "Пароль сброшен. Закройте приложение и перезапустите его — при следующем входе будет предложено задать новый пароль.", duration=6000)
            except Exception as ex:
                show_notify(ev.page, f"Ошибка: {ex}", is_error=True)
            ev.page.update()
        def on_cancel(ev):
            ev.page.pop_dialog()
            ev.page.update()
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Сброс пароля"),
            content=ft.Column([info], tight=True),
            actions=[
                ft.TextButton("Отмена", on_click=on_cancel),
                ft.Button("Сбросить пароль", on_click=do_reset),
            ],
        )
        p.show_dialog(dlg)
        p.update()

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Профиль", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Row([ft.Text("Тема:"), theme_dd], alignment=ft.MainAxisAlignment.START),
                ft.Divider(),
                ft.Text("Уведомления", size=16, weight=ft.FontWeight.W_600),
                ft.Row([notify_toast_cb, notify_sound_cb], spacing=24),
                ft.Row([notify_sound_field, ft.Button("Выбрать файл", icon=ft.Icons.AUDIO_FILE, on_click=pick_sound_file)], spacing=8),
                ft.Button("Применить", icon=ft.Icons.NOTIFICATIONS_ACTIVE, on_click=save_notify_settings),
                ft.Divider(),
                ft.Text("API Telegram (https://my.telegram.org)", size=14),
                ft.Row([api_id_field, api_hash_field], spacing=12),
                ft.Button("Сохранить API", on_click=save_api),
                ft.Divider(),
                ft.Text("Экспорт / Импорт настроек", size=16, weight=ft.FontWeight.W_600),
                ft.Row([
                    ft.Button("Экспорт настроек", icon=ft.Icons.UPLOAD_FILE, on_click=do_export),
                    ft.Button("Импорт настроек", icon=ft.Icons.DOWNLOAD, on_click=do_import),
                ], spacing=12),
                ft.Divider(),
                ft.Text("Безопасность", size=16, weight=ft.FontWeight.W_600),
                ft.Button("Сброс пароля", icon=ft.Icons.LOCK_RESET, on_click=show_reset_password_dialog),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def _store_selection(e, last_selection_ref):
    if e.control and hasattr(e.control, "selection") and e.control.selection:
        last_selection_ref[0] = e.control.selection


def _apply_format_to_selection(msg_field, parse_mode_dd, last_selection_ref, fmt_type, url=None):
    sel = last_selection_ref[0]
    text = msg_field.value or ""
    if not sel or sel.is_collapsed:
        return False
    start, end = sel.start, sel.end
    if start < 0 or end > len(text):
        return False
    selected = text[start:end]
    pm = (parse_mode_dd.value or "none").lower()
    if pm == "none":
        pm = "html"
        parse_mode_dd.value = "html"

    if fmt_type == "bold":
        wrapped = f"<b>{selected}</b>" if pm == "html" else f"**{selected}**"
    elif fmt_type == "italic":
        wrapped = f"<i>{selected}</i>" if pm == "html" else f"_{selected}_"
    elif fmt_type == "underline":
        wrapped = f"<u>{selected}</u>" if pm == "html" else f"__{selected}__"
    elif fmt_type == "strikethrough":
        wrapped = f"<s>{selected}</s>" if pm == "html" else f"~{selected}~"
    elif fmt_type == "code":
        wrapped = f"<code>{selected}</code>" if pm == "html" else f"`{selected}`"
    elif fmt_type == "spoiler":
        wrapped = f"<tg-spoiler>{selected}</tg-spoiler>" if pm == "html" else f"||{selected}||"
    elif fmt_type == "link" and url:
        wrapped = f'<a href="{url}">{selected}</a>' if pm == "html" else f"[{selected}]({url})"
    else:
        return False
    new_text = text[:start] + wrapped + text[end:]
    msg_field.value = new_text
    msg_field.focus()
    new_end = start + len(wrapped)
    try:
        from flet.controls.core.text import TextSelection
        msg_field.selection = TextSelection(base_offset=start, extent_offset=new_end)
    except Exception:
        pass
    return True


def build_dm_view(page, status_text=None, pb=None, counter_text=None):
    def run_dm_thread():
        def run():
            c = load_config()
            if not c or not c.get("accounts") or not c.get("message"):
                def err():
                    if status_text:
                        status_text.value = "Ошибка: добавьте аккаунты и сообщение в разделе «Сообщения»"
                    page.update()
                _run_on_page(page, err)
                return
            links = get_chat_links() or []
            if not links:
                def err():
                    if status_text:
                        status_text.value = "Нет ссылок в базе. Импортируйте в разделе «Чаты»."
                    page.update()
                _run_on_page(page, err)
                return
            def get_code(phone_num):
                _run_on_page(page, lambda ph=phone_num: _show_code_dialog(page, ph))
                return RESPONSE_QUEUE.get(timeout=120)
            def start_ui():
                if status_text:
                    status_text.value = "Рассылка в личку..."
                if pb:
                    pb.visible = True
                if counter_text:
                    counter_text.value = "Отправлено: 0, ошибок: 0"
                page.update()
            def on_prog(success, failed):
                def upd():
                    if counter_text:
                        counter_text.value = f"Отправлено: {success}, ошибок: {failed}"
                    page.update()
                _run_on_page(page, upd)
            try:
                _run_on_page(page, start_ui)
                stats = asyncio.run(run_dm_broadcast(c, code_input=get_code, on_progress=on_prog))
                def done():
                    if status_text:
                        status_text.value = "Рассылка в личку завершена"
                    if pb:
                        pb.visible = False
                    if counter_text:
                        counter_text.value = f"Готово: {stats['success']} в ЛС, {stats['failed']} ошибок"
                    show_notify(page, f"Рассылка в личку: {stats['success']} отправлено, {stats['failed']} ошибок", duration=5000)
                    dlg = ft.AlertDialog(
                        modal=True,
                        title=ft.Text("Рассылка в личку завершена"),
                        content=ft.Text(f"Отправлено: {stats['success']}\nОшибок: {stats['failed']}"),
                        actions=[ft.Button("OK", on_click=lambda e: (e.page.pop_dialog(), e.page.update()))],
                    )
                    page.show_dialog(dlg)
                    page.update()
                _run_on_page(page, done)
            except Exception as ex:
                def err():
                    if status_text:
                        status_text.value = f"Ошибка: {ex}"
                    if pb:
                        pb.visible = False
                    show_notify(page, f"Ошибка рассылки: {ex}", is_error=True)
                    page.update()
                _run_on_page(page, err)
        threading.Thread(target=run, daemon=True).start()

    def _show_code_dialog(pg, phone):
        code_field = ft.TextField(label="Код из Telegram", width=300, autofocus=True)
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Код для {phone}"),
            content=code_field,
            actions=[],
        )
        def on_code_ok(e):
            RESPONSE_QUEUE.put((code_field.value or "").strip())
            pg.pop_dialog()
            pg.update()
        dlg.actions = [ft.Button("OK", on_click=on_code_ok)]
        pg.show_dialog(dlg)
        pg.update()

    def on_start_dm(e):
        links = get_chat_links() or []
        if not links:
            show_notify(page, "Нет ссылок в базе. Импортируйте в разделе «Чаты».")
            return
        acc_count = len([a for a in (load_config() or {}).get("accounts", []) if is_account_authorized(a["phone"])])
        confirm_dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Рассылка в личку"),
            content=ft.Text(
                f"Сообщение будет отправлено в личку каждому участнику всех {len(links)} чатов из базы "
                f"с {acc_count} аккаунт(ов). Один пользователь получит не более одного сообщения.\n\n"
                "Вы уверены?"
            ),
            actions=[
                ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())),
                ft.Button("Да, запустить", icon=ft.Icons.CHAT, on_click=lambda ev: (
                    ev.page.pop_dialog(),
                    ev.page.update(),
                    run_dm_thread()
                )),
            ],
        )
        page.show_dialog(confirm_dlg)
        page.update()

    start_btn = ft.Button("Запустить рассылку в личку", icon=ft.Icons.CHAT, on_click=on_start_dm)

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Рассылка в личку участникам", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Text(
                    "Сообщение будет отправлено в личку каждому участнику всех чатов из базы (раздел «Чаты»). "
                    "Текст и вложения берутся из раздела «Сообщения». Дубликаты по одному пользователю исключаются.",
                    size=12,
                    color=ft.Colors.GREY,
                ),
                ft.Row([start_btn], alignment=ft.MainAxisAlignment.START),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_messages_view(page):
    cfg = load_config() or {}
    last_selection = [None]

    msg_field = ft.TextField(
        label="Текст рассылки",
        multiline=True,
        min_lines=6,
        value=cfg.get("message", ""),
        hint_text="Выделите текст и нажмите кнопку форматирования",
        on_blur=lambda e: _do_save(page, msg_field, parse_mode_dd, attachments_list),
        on_selection_change=lambda e: _store_selection(e, last_selection),
    )
    parse_mode_dd = ft.Dropdown(
        label="Форматирование",
        width=180,
        value=cfg.get("parse_mode") or "none",
        options=[
            ft.dropdown.Option("none", "Без форматирования"),
            ft.dropdown.Option("html", "HTML"),
            ft.dropdown.Option("md", "Markdown"),
        ],
    )
    attachments_list = [str(Path(p).resolve()) for p in (cfg.get("attachments") or []) if Path(p).exists()]
    attachments_display = ft.Column(spacing=4)

    templates = get_templates()
    _NONE_OPT = "— Без шаблона —"
    template_dd = ft.Dropdown(
        label="Шаблон",
        width=260,
        value=_NONE_OPT,
        options=[ft.dropdown.Option(_NONE_OPT, _NONE_OPT)] + [ft.dropdown.Option(t["name"], t["name"]) for t in templates],
    )

    def refresh_attachments():
        attachments_display.controls.clear()
        for i, p in enumerate(attachments_list):
            name = Path(p).name if p else ""
            attachments_display.controls.append(
                ft.Row(
                    [
                        ft.Icon(ft.Icons.INSERT_DRIVE_FILE, size=18),
                        ft.Text(name, size=12, overflow=ft.TextOverflow.ELLIPSIS, expand=True),
                        ft.IconButton(icon=ft.Icons.CLOSE, icon_size=18, on_click=lambda e, idx=i: _remove_attach(idx)),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                )
            )
        if not attachments_list:
            attachments_display.controls.append(ft.Text("Нет вложений", size=12, color=ft.Colors.GREY))
        page.update()

    def _remove_attach(idx):
        attachments_list.pop(idx)
        refresh_attachments()
        _do_save(page, msg_field, parse_mode_dd, attachments_list)

    def _do_save(p, mf, pm_dd, att_list):
        parse_mode = pm_dd.value if pm_dd.value and pm_dd.value != "none" else None
        save_message(mf.value, parse_mode, list(att_list), p)

    def _apply_template(tpl):
        msg_field.value = tpl.get("message", "")
        parse_mode_dd.value = tpl.get("parse_mode") or "none"
        atts = tpl.get("attachments") or []
        attachments_list.clear()
        for p in atts:
            pp = Path(p)
            if pp.exists():
                attachments_list.append(str(pp.resolve()))
        refresh_attachments()
        _do_save(page, msg_field, parse_mode_dd, attachments_list)
        page.update()

    def on_template_select(e):
        val = template_dd.value
        if val and val != _NONE_OPT:
            tpl = next((t for t in get_templates() if t.get("name") == val), None)
            if tpl:
                _apply_template(tpl)

    def save_as_template_click(e):
        name_field = ft.TextField(label="Название шаблона", width=300, hint_text="Например: Вакансия строителя")
        err_text = ft.Text("", color=ft.Colors.RED, size=12)

        def do_save(ev):
            name = (name_field.value or "").strip()
            if not name:
                err_text.value = "Введите название"
                err_text.update()
                return
            add_template(name, msg_field.value, parse_mode_dd.value, attachments_list)
            ev.page.pop_dialog()
            template_dd.options = [ft.dropdown.Option(_NONE_OPT, _NONE_OPT)] + [ft.dropdown.Option(t["name"], t["name"]) for t in get_templates()]
            template_dd.value = name
            show_notify(ev.page, f"Шаблон «{name}» сохранён")

        dlg = ft.AlertDialog(
            title=ft.Text("Сохранить как шаблон"),
            content=ft.Column([name_field, err_text], tight=True),
            actions=[ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())), ft.Button("Сохранить", on_click=do_save)],
        )
        page.show_dialog(dlg)
        page.update()

    def delete_template_click(e):
        val = template_dd.value
        if not val or val == _NONE_OPT:
            show_notify(page, "Выберите шаблон для удаления")
            return

        def do_delete(ev):
            delete_template(val)
            ev.page.pop_dialog()
            template_dd.options = [ft.dropdown.Option(_NONE_OPT, _NONE_OPT)] + [ft.dropdown.Option(t["name"], t["name"]) for t in get_templates()]
            template_dd.value = _NONE_OPT
            show_notify(ev.page, f"Шаблон «{val}» удалён")

        dlg = ft.AlertDialog(
            title=ft.Text("Удалить шаблон?"),
            content=ft.Text(f"Шаблон «{val}» будет удалён."),
            actions=[ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())), ft.Button("Удалить", on_click=do_delete)],
        )
        page.show_dialog(dlg)
        page.update()

    template_dd.on_select = on_template_select

    def _make_format_click(fmt_type):
        def handler(e):
            ok = _apply_format_to_selection(msg_field, parse_mode_dd, last_selection, fmt_type)
            if ok:
                _do_save(page, msg_field, parse_mode_dd, attachments_list)
            else:
                if fmt_type != "link":
                    show_notify(page, "Выделите текст в поле сообщения")
            page.update()
        return handler

    def link_format_click(e):
        if not last_selection[0] or last_selection[0].is_collapsed:
            show_notify(page, "Выделите текст для ссылки")
            page.update()
            return
        url_field = ft.TextField(label="URL", width=350, hint_text="https://...")

        def do_link(ev):
            url = (url_field.value or "").strip()
            if not url:
                return
            ok = _apply_format_to_selection(msg_field, parse_mode_dd, last_selection, "link", url=url)
            ev.page.pop_dialog()
            if ok:
                _do_save(page, msg_field, parse_mode_dd, attachments_list)
            ev.page.update()

        dlg = ft.AlertDialog(
            title=ft.Text("Вставка ссылки"),
            content=url_field,
            actions=[
                ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())),
                ft.Button("Вставить", icon=ft.Icons.LINK, on_click=do_link),
            ],
        )
        page.show_dialog(dlg)
        page.update()

    EMOJIS_STR = "😀😃😄😁😅😂🤣😊😇🙂🙃😉😌😍🥰😘😋😛😜😝🤗🤔🤐😐😏😒🙄😬😴😷🤒🤕🤢🤮🤧🥵🥶🥴😵🤯😎🤓🧐😕😟😮😯😲😳🥺😢😭😱😤😡🤬👍👎👏🙌🤝🙏✌️🤞🤟🤘👌👈👉👆👇💪❤️🧡💛💚💙💜🖤🤍💔💕💞💓💗💖💘✨⭐🌟💫💥🔥💯✅❌🎉🎊🚀⚡💬📝📌📍🔔💡⚠️📎🔗"

    def emoji_picker_click(e):
        emoji_grid = ft.Column(wrap=True, spacing=4, scroll=ft.ScrollMode.AUTO)
        row_emojis = []
        for i, em in enumerate(EMOJIS_STR):
            row_emojis.append(em)
            if len(row_emojis) >= 12 or i == len(EMOJIS_STR) - 1:
                emoji_grid.controls.append(
                    ft.Row(
                        [ft.TextButton(em, data=em, on_click=lambda ev: _insert_emoji(ev, msg_field, last_selection, page, parse_mode_dd, attachments_list)) for em in row_emojis],
                        spacing=2,
                        wrap=True,
                    )
                )
                row_emojis = []

        def _insert_emoji(ev, mf, sel_ref, p, pm_dd, att_list):
            em = ev.control.data
            text = mf.value or ""
            pos = sel_ref[0].start if sel_ref[0] and sel_ref[0].is_valid else len(text)
            pos = min(max(0, pos), len(text))
            mf.value = text[:pos] + em + text[pos:]
            p.pop_dialog()
            _do_save(p, mf, pm_dd, att_list)
            mf.focus()
            try:
                from flet.controls.core.text import TextSelection
                mf.selection = TextSelection(base_offset=pos + len(em), extent_offset=pos + len(em))
            except Exception:
                pass
            p.update()

        dlg = ft.AlertDialog(
            title=ft.Text("Выберите смайлик"),
            content=ft.Container(emoji_grid, height=280, width=400),
            actions=[ft.TextButton("Закрыть", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update()))],
        )
        page.show_dialog(dlg)
        page.update()

    format_toolbar = ft.Row(
        [
            ft.IconButton(icon=ft.Icons.FORMAT_BOLD, tooltip="Жирный", on_click=_make_format_click("bold")),
            ft.IconButton(icon=ft.Icons.FORMAT_ITALIC, tooltip="Курсив", on_click=_make_format_click("italic")),
            ft.IconButton(icon=ft.Icons.FORMAT_UNDERLINED, tooltip="Подчёркнутый", on_click=_make_format_click("underline")),
            ft.IconButton(icon=ft.Icons.FORMAT_STRIKETHROUGH, tooltip="Зачёркнутый", on_click=_make_format_click("strikethrough")),
            ft.IconButton(icon=ft.Icons.CODE, tooltip="Код", on_click=_make_format_click("code")),
            ft.IconButton(icon=ft.Icons.BLUR_ON, tooltip="Спойлер", on_click=_make_format_click("spoiler")),
            ft.IconButton(icon=ft.Icons.LINK, tooltip="Ссылка", on_click=link_format_click),
            ft.IconButton(icon=ft.Icons.EMOJI_EMOTIONS, tooltip="Смайлик", on_click=emoji_picker_click),
        ],
        spacing=4,
        wrap=True,
    )

    async def _pick_files_click(e):
        fp = ft.FilePicker()
        files = await fp.pick_files(allow_multiple=True)
        if files:
            for f in files:
                if f.path:
                    try:
                        stored = add_file_to_storage(f.path)
                        attachments_list.append(stored)
                    except Exception as ex:
                        show_notify(page, f"Ошибка: {ex}", is_error=True)
            refresh_attachments()
            _do_save(page, msg_field, parse_mode_dd, attachments_list)
            page.update()

    refresh_attachments()

    def ai_generate_click(e):
        pos_field = ft.TextField(label="Должность", value="", width=400, hint_text="Например: Строитель, Монтажник")
        desc_field = ft.TextField(label="Описание / обязанности", value="", multiline=True, min_lines=2, width=400)
        req_field = ft.TextField(label="Требования", value="", multiline=True, min_lines=1, width=400)
        salary_field = ft.TextField(label="Зарплата", value="", width=400, hint_text="Например: от 80 000 руб")
        contacts_field = ft.TextField(label="Контакты", value="", width=400, hint_text="Написать в личку")
        extra_field = ft.TextField(label="Доп. пожелания", value="", multiline=True, min_lines=1, width=400)
        backend_dd = ft.Dropdown(
            label="Сервис",
            width=220,
            value=cfg.get("ai_backend", "g4f"),
            options=[
                ft.dropdown.Option("g4f", "g4f (бесплатно, без ключа)"),
                ft.dropdown.Option("openrouter", "OpenRouter (бесплатный тариф)"),
            ],
        )
        api_key_field = ft.TextField(
            label="API ключ OpenRouter",
            value=cfg.get("openrouter_api_key", ""),
            width=400,
            password=True,
            visible=(cfg.get("ai_backend", "g4f") == "openrouter"),
        )
        status_text = ft.Text("", size=12, color=ft.Colors.GREY_700)
        gen_btn = ft.Button("Сгенерировать", icon=ft.Icons.AUTO_AWESOME)

        def on_backend_change(ev):
            api_key_field.visible = backend_dd.value == "openrouter"
            page.update()

        backend_dd.on_change = on_backend_change

        def do_generate(ev):
            backend = backend_dd.value or "g4f"
            api_key = (api_key_field.value or "").strip() if backend == "openrouter" else None
            if backend == "openrouter" and not api_key:
                status_text.value = "Укажите API ключ OpenRouter"
                status_text.color = ft.Colors.ERROR
                page.update()
                return
            gen_btn.visible = False
            status_text.value = "Генерация… подождите"
            status_text.color = ft.Colors.GREY_700
            page.update()
            try:
                c = load_config() or {}
                c["ai_backend"] = backend
                if backend == "openrouter" and api_key:
                    c["openrouter_api_key"] = api_key
                save_config(c)
                text = generate_vacancy_text(
                    position=pos_field.value or "",
                    description=desc_field.value or "",
                    requirements=req_field.value or "",
                    salary=salary_field.value or "",
                    contacts=contacts_field.value or "",
                    extra=extra_field.value or "",
                    backend=backend,
                    api_key=api_key,
                )
                msg_field.value = text
                _do_save(page, msg_field, parse_mode_dd, attachments_list)
                ev.page.pop_dialog()
                show_notify(ev.page, "Текст сгенерирован")
            except Exception as ex:
                status_text.value = str(ex)
                status_text.color = ft.Colors.ERROR
                gen_btn.visible = True
            ev.page.update()

        dlg = ft.AlertDialog(
            title=ft.Text("Сгенерировать текст вакансии (ИИ)"),
            content=ft.Container(
                content=ft.Column(
                    [
                        pos_field,
                        desc_field,
                        req_field,
                        salary_field,
                        contacts_field,
                        extra_field,
                        ft.Row([backend_dd, api_key_field], spacing=12, wrap=True),
                        status_text,
                    ],
                    tight=True,
                    scroll=ft.ScrollMode.AUTO,
                ),
                width=450,
            ),
            actions=[
                ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())),
                gen_btn,
            ],
        )
        gen_btn.on_click = do_generate
        page.show_dialog(dlg)
        page.update()

    def _show_variables_help(e):
        help_text = (
            "Переменные для подстановки в текст:\n\n"
            "• {имя} или {name} — название чата\n"
            "• {номер}, {number}, {телефон}, {phone} — телефон аккаунта\n"
            "• {участников} или {participants} — количество участников чата\n\n"
            "Пример: «Привет, {имя}! Пишу из аккаунта {номер}»"
        )
        dlg = ft.AlertDialog(
            title=ft.Text("Переменные в сообщении"),
            content=ft.Text(help_text),
            actions=[ft.TextButton("Понятно", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update()))],
        )
        page.show_dialog(dlg)
        page.update()

    def _build_preview_section(mf, pm_dd):
        preview_text = ft.Text("", size=12, color=ft.Colors.GREY_700, selectable=True)
        preview_container = ft.Container(
            content=ft.Column(
                [
                    ft.Text("Предпросмотр", size=14, weight=ft.FontWeight.W_500),
                    ft.Container(
                        content=preview_text,
                        bgcolor=ft.Colors.SURFACE_CONTAINER_HIGHEST,
                        padding=12,
                        border_radius=8,
                    ),
                ],
                tight=True,
            ),
        )

        def update_preview(*_):
            text = mf.value or ""
            sample = substitute_variables(text, "Пример чата", "+7 900 123-45-67", 150)
            preview_text.value = sample
            if preview_text.page:
                preview_text.update()

        mf.on_change = lambda e: update_preview()
        pm_dd.on_change = lambda e: update_preview()
        preview_text.value = substitute_variables(mf.value or "", "Пример чата", "+7 900 123-45-67", 150)
        return preview_container

    def _build_broadcast_settings_section(p):
        cfg = load_config() or {}
        delay_field = ft.TextField(
            label="Задержка между сообщениями (сек)",
            value=str(cfg.get("message_delay_sec", 2)),
            width=180,
            keyboard_type=ft.KeyboardType.NUMBER,
        )
        test_mode_dd = ft.Dropdown(
            label="Тестовый режим",
            width=220,
            value=cfg.get("test_mode") or "off",
            options=[
                ft.dropdown.Option("off", "Выключен"),
                ft.dropdown.Option("self", "Только себе (Избранное)"),
                ft.dropdown.Option("single_chat", "Один чат"),
            ],
        )
        test_chat_field = ft.TextField(
            label="Название чата (подстрока)",
            value=cfg.get("test_chat_name", ""),
            width=260,
            hint_text="Для режима «Один чат»",
            visible=(cfg.get("test_mode") == "single_chat"),
        )

        chat_f = cfg.get("chat_filter") or {}
        inc_val = chat_f.get("include_by_name")
        include_field = ft.TextField(
            label="Включить чаты",
            value=", ".join(inc_val) if isinstance(inc_val, list) else (inc_val or ""),
            hint_text="Названия или подстроки через запятую (пусто = все)",
        )

        exc_val = chat_f.get("exclude_by_name")
        exclude_field = ft.TextField(
            label="Исключить чаты",
            value=", ".join(exc_val) if isinstance(exc_val, list) else (exc_val or ""),
            hint_text="Названия или подстроки через запятую",
        )

        min_p_field = ft.TextField(
            hint_text="Мин. участников",
            value=str(chat_f.get("min_participants") or ""),
            width=120,
            keyboard_type=ft.KeyboardType.NUMBER,
        )
        max_p_field = ft.TextField(
            hint_text="Макс. участников",
            value=str(chat_f.get("max_participants") or ""),
            width=120,
            keyboard_type=ft.KeyboardType.NUMBER,
        )

        blacklist_field = ft.TextField(
            label="Чёрный список",
            value=", ".join(cfg.get("blacklist") or []),
            hint_text="Названия чатов через запятую",
        )
        whitelist_field = ft.TextField(
            label="Белый список",
            value=", ".join(cfg.get("whitelist") or []),
            hint_text="Названия чатов через запятую (если не пусто — только эти)",
        )

        use_vars_cb = ft.Checkbox(label="Подставлять переменные {имя}, {номер}", value=cfg.get("use_variables", True))
        max_retries_field = ft.TextField(
            label="Повторов при ошибке",
            value=str(cfg.get("max_retries", 3)),
            width=100,
            keyboard_type=ft.KeyboardType.NUMBER,
            hint_text="1–5",
        )

        def _parse_list(s):
            return [x.strip() for x in str(s or "").split(",") if x.strip()]

        def save_broadcast_settings(ev):
            c = load_config() or {}
            try:
                c["message_delay_sec"] = max(0, float(delay_field.value or 2))
            except ValueError:
                c["message_delay_sec"] = 2
            c["test_mode"] = test_mode_dd.value or "off"
            c["test_chat_name"] = (test_chat_field.value or "").strip()
            c["chat_filter"] = {
                "include_by_name": _parse_list(include_field.value),
                "exclude_by_name": _parse_list(exclude_field.value),
                "min_participants": int(min_p_field.value or 0) or 0,
                "max_participants": int(max_p_field.value or 0) or 0,
            }
            c["blacklist"] = _parse_list(blacklist_field.value)
            c["whitelist"] = _parse_list(whitelist_field.value)
            c["use_variables"] = use_vars_cb.value
            try:
                r = int(max_retries_field.value or 3)
                c["max_retries"] = max(1, min(5, r))
            except ValueError:
                c["max_retries"] = 3
            save_config(c)
            show_notify(p, "Настройки рассылки сохранены")
            p.update()

        def on_test_mode_change(e):
            test_chat_field.visible = test_mode_dd.value == "single_chat"
            p.update()

        test_mode_dd.on_change = on_test_mode_change

        participants_row = ft.Row(
            [
                ft.Text("Участников:", size=12, color=ft.Colors.ON_SURFACE_VARIANT),
                min_p_field,
                ft.Text("—", size=14, color=ft.Colors.ON_SURFACE_VARIANT),
                max_p_field,
            ],
            spacing=8,
            alignment=ft.MainAxisAlignment.START,
        )

        return ft.Container(
            content=ft.Column(
                [
                    ft.Text("Настройки рассылки", size=16, weight=ft.FontWeight.W_600),
                    ft.Divider(height=1),
                    ft.Row([delay_field, test_mode_dd, test_chat_field], spacing=16, wrap=True),
                    ft.Text("Фильтр по названию", size=12, color=ft.Colors.ON_SURFACE_VARIANT),
                    include_field,
                    exclude_field,
                    participants_row,
                    ft.Text("Списки чатов", size=12, color=ft.Colors.ON_SURFACE_VARIANT),
                    blacklist_field,
                    whitelist_field,
                    ft.Divider(height=1),
                    ft.Row([use_vars_cb, max_retries_field], spacing=16),
                    ft.Row(
                        [ft.Button("Сохранить настройки", icon=ft.Icons.SETTINGS, on_click=save_broadcast_settings)],
                        alignment=ft.MainAxisAlignment.START,
                    ),
                ],
                spacing=12,
            ),
            padding=16,
            bgcolor=ft.Colors.SURFACE_CONTAINER,
            border_radius=12,
        )

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Сообщения", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Row(
                    [
                        template_dd,
                        ft.Button("Сохранить как шаблон", icon=ft.Icons.BOOKMARK_ADD, on_click=save_as_template_click),
                        ft.IconButton(icon=ft.Icons.DELETE_OUTLINE, tooltip="Удалить шаблон", on_click=delete_template_click),
                    ],
                    spacing=12,
                ),
                ft.Divider(),
                ft.Row([parse_mode_dd], spacing=12),
                ft.Text("Выделите текст и нажмите кнопку:", size=12, color=ft.Colors.GREY),
                format_toolbar,
                ft.Divider(),
                msg_field,
                ft.Row(
                    [
                        ft.Button("Сохранить", icon=ft.Icons.SAVE, on_click=lambda e: _do_save(page, msg_field, parse_mode_dd, attachments_list)),
                        ft.Button("Сгенерировать ИИ", icon=ft.Icons.AUTO_AWESOME, on_click=ai_generate_click, visible=False),
                        ft.IconButton(icon=ft.Icons.HELP_OUTLINE, tooltip="Переменные", on_click=lambda e: _show_variables_help(page)),
                    ],
                    spacing=8,
                ),
                ft.Container(height=8),
                _build_preview_section(msg_field, parse_mode_dd),
                ft.Container(height=8),
                _build_broadcast_settings_section(page),
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_schedule_view(page, run_broadcast_callback, schedule_status_ref):
    cfg = load_config() or {}
    enabled_cb = ft.Checkbox(label="Включить отложенную рассылку", value=cfg.get("schedule_enabled", False))
    type_dd = ft.Dropdown(
        label="Тип",
        width=200,
        value=cfg.get("schedule_type") or "once",
        options=[
            ft.dropdown.Option("once", "Один раз в указанное время"),
            ft.dropdown.Option("interval", "С переодичностью (каждые N минут)"),
            ft.dropdown.Option("daily", "Ежедневно в указанное время"),
        ],
    )
    today = datetime.now().strftime("%Y-%m-%d")
    once_date = ft.TextField(label="Дата (ГГГГ-ММ-ДД)", value=cfg.get("schedule_once_date") or today, width=160, hint_text="2025-03-10")
    once_time = ft.TextField(label="Время (ЧЧ:ММ)", value=cfg.get("schedule_once_time") or "09:00", width=100)
    interval_min = ft.TextField(label="Интервал (минут)", value=str(cfg.get("schedule_interval_minutes") or 60), width=120, keyboard_type=ft.KeyboardType.NUMBER)
    daily_time = ft.TextField(label="Время (ЧЧ:ММ)", value=cfg.get("schedule_daily_time") or "09:00", width=100)
    next_run_text = ft.Text("", size=12, color=ft.Colors.GREY)
    status_text = ft.Text("", size=12, color=ft.Colors.PRIMARY)

    once_row = ft.Row([once_date, once_time], spacing=12)
    interval_row = ft.Row([interval_min], spacing=12)
    daily_row = ft.Row([daily_time], spacing=12)

    def update_visibility(e=None):
        t = type_dd.value or "once"
        once_row.visible = t == "once"
        interval_row.visible = t == "interval"
        daily_row.visible = t == "daily"
        page.update()

    def refresh_next():
        c = load_config() or {}
        enabled_cb.value = c.get("schedule_enabled", False)
        type_dd.value = c.get("schedule_type") or "once"
        once_date.value = c.get("schedule_once_date") or today
        once_time.value = c.get("schedule_once_time") or "09:00"
        interval_min.value = str(c.get("schedule_interval_minutes") or 60)
        daily_time.value = c.get("schedule_daily_time") or "09:00"
        next_run = get_next_run(c)
        if next_run:
            next_run_text.value = f"Следующий запуск: {next_run.strftime('%Y-%m-%d %H:%M')}"
        else:
            next_run_text.value = "Расписание не активно или уже выполнено"

    def save_schedule(e):
        c = load_config() or {}
        c["schedule_enabled"] = enabled_cb.value
        c["schedule_type"] = type_dd.value or "once"
        c["schedule_once_date"] = (once_date.value or "").strip()
        c["schedule_once_time"] = (once_time.value or "09:00").strip()
        try:
            c["schedule_interval_minutes"] = max(1, int(interval_min.value or 60))
        except ValueError:
            c["schedule_interval_minutes"] = 60
        c["schedule_daily_time"] = (daily_time.value or "09:00").strip()
        save_config(c)
        refresh_next()
        show_notify(page, "Расписание сохранено")
        if schedule_status_ref:
            schedule_status_ref[0] = c.get("schedule_enabled", False)
        page.update()

    type_dd.on_change = update_visibility
    update_visibility()
    refresh_next()

    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Отложенная рассылка", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Text("Настройте время рассылки. Рассылка запустится автоматически в указанное время.", size=12, color=ft.Colors.GREY),
                enabled_cb,
                type_dd,
                once_row,
                interval_row,
                daily_row,
                next_run_text,
                ft.Row([ft.Button("Сохранить расписание", icon=ft.Icons.SCHEDULE, on_click=save_schedule)], alignment=ft.MainAxisAlignment.START),
                status_text,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def save_message(text, parse_mode=None, attachments=None, page=None):
    cfg = load_config() or {}
    cfg["message"] = text or ""
    cfg["parse_mode"] = parse_mode if parse_mode else "none"
    cfg["attachments"] = attachments or []
    if page:
        def do_save():
            save_config(cfg)
            def on_done():
                show_notify(page, "Сохранено")
                page.update()
            _run_on_page(page, on_done)
        threading.Thread(target=do_save, daemon=True).start()
    else:
        save_config(cfg)


def apply_theme(page, mode):
    page.theme_mode = ft.ThemeMode.SYSTEM if mode == "system" else (ft.ThemeMode.LIGHT if mode == "light" else ft.ThemeMode.DARK)
    page.update()


def _run_on_page(page, fn):
    if hasattr(page, "loop") and page.loop:
        page.loop.call_soon_threadsafe(fn)
    else:
        fn()


def run_broadcast_thread(page, pb, status_text, counter_text=None):
    def show_code_dialog(phone):
        code_field = ft.TextField(label="Код из Telegram", width=300, autofocus=True)
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text(f"Код для {phone}"),
            content=code_field,
            actions=[],
        )
        def on_code_ok(e):
            RESPONSE_QUEUE.put(code_field.value.strip())
            page.pop_dialog()
            page.update()
        dlg.actions = [ft.Button("OK", on_click=on_code_ok)]
        page.show_dialog(dlg)
        page.update()

    def run():
        cfg = load_config()
        if not cfg or not cfg.get("accounts") or not cfg.get("message"):
            _run_on_page(page, lambda: (setattr(status_text, "value", "Ошибка: нет аккаунтов или сообщения"), page.update()))
            return
        def get_code(p):
            _run_on_page(page, lambda: show_code_dialog(p))
            return RESPONSE_QUEUE.get(timeout=120)

        def on_progress(success, failed):
            if counter_text is not None:
                def update():
                    counter_text.value = f"Отправлено: {success}, ошибок: {failed}"
                    page.update()
                _run_on_page(page, update)

        try:
            def start_ui():
                status_text.value = "Запуск рассылки..."
                pb.visible = True
                if counter_text:
                    counter_text.value = "Отправлено: 0, ошибок: 0"
                page.update()
            _run_on_page(page, start_ui)
            stats = asyncio.run(run_broadcast(cfg, code_input=get_code, on_progress=on_progress))

            def show_done():
                setattr(status_text, "value", "Рассылка завершена")
                setattr(pb, "visible", False)
                if counter_text:
                    counter_text.value = f"Готово: {stats['success']} отправлено, {stats['failed']} ошибок"
                show_notify(page, f"Рассылка завершена: {stats['success']} отправлено, {stats['failed']} ошибок", duration=5000)
                done_dlg = ft.AlertDialog(
                    modal=True,
                    title=ft.Text("Рассылка завершена"),
                    content=ft.Text(f"Отправлено: {stats['success']}\nОшибок: {stats['failed']}"),
                    actions=[ft.Button("OK", on_click=lambda e: (e.page.pop_dialog(), e.page.update()))],
                )
                page.show_dialog(done_dlg)
                page.update()

            def after_done():
                show_done()
                try:
                    from schedule import mark_run
                    mark_run(load_config() or {})
                except Exception:
                    pass
            _run_on_page(page, after_done)
        except Exception as ex:
            msg = str(ex)
            def on_err():
                status_text.value = f"Ошибка: {msg}"
                pb.visible = False
                show_notify(page, f"Ошибка рассылки: {msg}", is_error=True)
                page.update()
            _run_on_page(page, on_err)

    t = threading.Thread(target=run)
    t.start()


def main(page: ft.Page):
    page.title = "Рассылка"
    page.window.width = WIDTH
    page.window.height = HEIGHT
    page.window.resizable = False
    page.window.maximizable = False
    _assets_dir = Path(getattr(sys, "_MEIPASS", BASE_DIR)) / "assets"
    icon_path = _assets_dir / "icon.ico"
    if icon_path.exists():
        page.window.icon = str(icon_path.resolve())

    page.window.prevent_close = True
    _tray_started = [False]

    def on_window_event(e: ft.WindowEvent):
        if getattr(e, "type", None) == ft.WindowEventType.CLOSE:
            page.window.visible = False
            page.update()
            if not _tray_started[0]:
                _tray_started[0] = True
                run_tray(icon_path, page)

    page.window.on_event = on_window_event

    page.theme = ft.Theme(color_scheme_seed=ft.Colors.BLUE)
    page.dark_theme = ft.Theme(color_scheme_seed=ft.Colors.BLUE)

    def on_login(pwd, remember=False):
        if verify(pwd):
            if remember:
                save_remember()
            else:
                clear_remember()
            page.controls.clear()
            page.overlay.clear()
            show_main(page)
            page.update()
        else:
            err_text.value = "Неверный пароль"
            err_text.update()

    pwd_field = ft.TextField(label="Пароль", password=True, width=280, on_submit=lambda e: do_login(e))
    remember_cb = ft.Checkbox(label="Запомнить меня", value=False)
    err_text = ft.Text("", color=ft.Colors.RED)

    def do_login(e):
        err_text.value = ""
        on_login(pwd_field.value or "", remember=remember_cb.value)

    login_img_path = _assets_dir / "login.png"
    login_image = ft.Image(src=str(login_img_path.resolve()), width=120, height=120, fit=ft.BoxFit.CONTAIN) if login_img_path.exists() else ft.Icon(ft.Icons.LOCK, size=48)
    login_view = ft.Column(
        [
            login_image,
            ft.Text("Вход", size=24, weight=ft.FontWeight.BOLD),
            pwd_field,
            ft.Row([remember_cb], alignment=ft.MainAxisAlignment.CENTER),
            ft.Button("Войти", on_click=do_login),
            err_text,
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        spacing=16,
    )

    def show_main(p):
        p.controls.clear()
        nav_index = [0]
        content_area = ft.Container(expand=True)

        schedule_enabled_ref = [load_config().get("schedule_enabled", False)]

        def build_content(idx):
            if idx == 0:
                return build_home_view(p, on_nav)
            if idx == 1:
                return build_accounts_view(p)
            if idx == 2:
                return build_stats_view(p)
            if idx == 3:
                return build_notifications_view(p)
            if idx == 4:
                return build_profile_view(p)
            if idx == 5:
                return build_messages_view(p)
            if idx == 6:
                return build_schedule_view(p, lambda: run_broadcast_thread(p, pb, status_text, counter_text), schedule_enabled_ref)
            if idx == 7:
                return build_dm_view(p, status_text, pb, counter_text)
            if idx == 8:
                return build_chats_view(p, on_refresh=lambda: on_nav(8))
            return build_home_view(p, on_nav)

        def on_nav(idx):
            nav_index[0] = idx
            content_area.content = build_content(idx)
            p.update()

        status_text = ft.Text("", size=12)
        counter_text = ft.Text("", size=12, color=ft.Colors.PRIMARY)
        pb = ft.ProgressBar(visible=False)

        def on_start_click(e):
            cfg = load_config()
            if not cfg or not cfg.get("accounts") or not cfg.get("message"):
                status_text.value = "Ошибка: добавьте аккаунты и сообщение"
                p.update()
                return
            acc_count = len(cfg["accounts"])
            confirm_dlg = ft.AlertDialog(
                modal=True,
                title=ft.Text("Подтверждение рассылки"),
                content=ft.Text(
                    f"Будет выполнена рассылка сообщения с {acc_count} аккаунт(ов) во все подходящие чаты.\n\n"
                    "Вы уверены, что хотите продолжить?"
                ),
                actions=[
                    ft.TextButton("Отмена", on_click=lambda ev: (ev.page.pop_dialog(), ev.page.update())),
                    ft.Button("Да, запустить", icon=ft.Icons.SEND, on_click=lambda ev: (
                        ev.page.pop_dialog(),
                        ev.page.update(),
                        run_broadcast_thread(p, pb, status_text, counter_text)
                    )),
                ],
            )
            p.show_dialog(confirm_dlg)
            p.update()

        start_btn = ft.Button("Запустить рассылку", icon=ft.Icons.SEND, on_click=on_start_click)

        content_area.content = build_content(0)
        sidebar = build_sidebar(on_nav, 0)

        schedule_stop = threading.Event()
        def scheduled_callback():
            def do_run():
                run_broadcast_thread(p, pb, status_text, counter_text)
            _run_on_page(p, do_run)
        run_scheduler(scheduled_callback, schedule_stop)

        p.add(
            ft.Row(
                [
                    sidebar,
                    ft.VerticalDivider(width=1),
                    ft.Column(
                        [
                            ft.Row([content_area], expand=True),
                            ft.Divider(),
                            ft.Row([start_btn, pb, counter_text, status_text], alignment=ft.MainAxisAlignment.START, wrap=True),
                        ],
                        expand=True,
                    ),
                ],
                expand=True,
            )
        )

    auth_file = DATA_DIR / ".auth"
    if not auth_file.exists():
        page.vertical_alignment = ft.MainAxisAlignment.CENTER
        page.horizontal_alignment = ft.CrossAxisAlignment.CENTER

        pwd1 = ft.TextField(label="Задай пароль доступа", password=True, width=280)
        pwd2 = ft.TextField(label="Повтори пароль", password=True, width=280)
        err = ft.Text("", color=ft.Colors.RED, size=12)

        def _init_from_gui():
            a, b = (pwd1.value or "").strip(), (pwd2.value or "").strip()
            if len(a) < 6:
                raise ValueError("Пароль минимум 6 символов")
            if a != b:
                raise ValueError("Пароли не совпали")
            from argon2 import PasswordHasher
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            (DATA_DIR / ".auth").write_text(PasswordHasher(time_cost=2, memory_cost=65536).hash(a), encoding="utf-8")

        def do_init_click(e):
            err.value = ""
            try:
                _init_from_gui()
                page.controls.clear()
                page.add(login_view)
            except ValueError as ex:
                err.value = str(ex)
            page.update()

        page.add(ft.Column(
            [
                ft.Text("Первый запуск: задай пароль доступа", size=18, weight=ft.FontWeight.BOLD),
                pwd1,
                pwd2,
                err,
                ft.Button("Создать пароль", on_click=do_init_click),
            ],
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=12,
        ))
        return

    if is_remembered():
        page.controls.clear()
        show_main(page)
        page.update()
        return

    page.vertical_alignment = ft.MainAxisAlignment.CENTER
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER
    page.add(login_view)
    page.update()


if __name__ == "__main__":
    import multiprocessing
    if multiprocessing.current_process().name == "MainProcess":
        ft.run(main)
