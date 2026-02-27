import asyncio
import queue
import sys
import threading
from pathlib import Path
from tkinter import Tk, filedialog


import flet as ft

from auth import clear_remember, is_remembered, save_remember, verify
from broadcast import auth_account, request_code, run_broadcast, sign_in_with_code
from chats import extract_links_from_xlsx, run_join_all_links
from core import (
    BASE_DIR,
    DATA_DIR,
    add_chat_links,
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
from telethon import TelegramClient

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
        leading=ft.Container(
            content=ft.Icon(ft.Icons.SEND, size=36),
            margin=ft.Margin.only(bottom=20),
        ),
        destinations=[
            ft.NavigationRailDestination(icon=ft.Icons.PEOPLE, label="Аккаунты"),
            ft.NavigationRailDestination(icon=ft.Icons.ANALYTICS, label="Статистика"),
            ft.NavigationRailDestination(icon=ft.Icons.PERSON, label="Профиль"),
            ft.NavigationRailDestination(icon=ft.Icons.MESSAGE, label="Сообщения"),
            ft.NavigationRailDestination(icon=ft.Icons.FORUM, label="Чаты"),
        ],
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
                accounts_list.controls.append(
                    ft.Card(
                        content=ft.Container(
                            content=ft.Row(
                                [
                                    ft.Row(
                                        [
                                            ft.Icon(ft.Icons.PHONE_ANDROID, color=ft.Colors.BLUE),
                                            ft.Icon(ft.Icons.CHECK_CIRCLE, color=ft.Colors.GREEN, size=20) if is_auth else ft.Container(width=20),
                                        ],
                                        spacing=4,
                                    ),
                                    ft.Column(
                                        [
                                            ft.Text(acc["phone"], weight=ft.FontWeight.W_600),
                                            ft.Text("2FA: ***" if acc.get("password") else "2FA: —", size=12, color=ft.Colors.GREY),
                                        ],
                                        spacing=2,
                                    ),
                                    ft.IconButton(icon=ft.Icons.DELETE, on_click=lambda e, idx=i: delete_account(idx)),
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

    def add_account_dialog(e):
        p = e.page
        phone_field = ft.TextField(label="Телефон", hint_text="+7...", width=300)
        pwd_field = ft.TextField(label="Пароль 2FA", hint_text="пусто если нет", password=True, width=300)
        dlg = ft.AlertDialog(
            modal=True,
            title=ft.Text("Добавить аккаунт"),
            content=ft.Column([phone_field, pwd_field], tight=True),
            actions=[],
        )
        def save_add(ev):
            phone = phone_field.value.strip()
            if not phone:
                return
            cfg = load_config() or {"api_id": 0, "api_hash": "", "accounts": [], "message": ""}
            cfg.setdefault("accounts", []).append({"phone": phone, "password": pwd_field.value or ""})
            save_config(cfg)
            p.pop_dialog()
            p.update()
            refresh_list()
        def cancel_add(ev):
            p.pop_dialog()
            p.update()
        dlg.actions = [
            ft.TextButton("Отмена", on_click=cancel_add),
            ft.Button("Добавить", on_click=save_add),
        ]
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
            client = TelegramClient(str(session_path), cfg["api_id"], cfg["api_hash"])
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
            client = TelegramClient(str(session_path), cfg["api_id"], cfg["api_hash"])
            def run():
                try:
                    asyncio.run(sign_in_with_code(client, phone, code, acc.get("password", "")))
                    _run_on_page(page, lambda: (refresh_list(), page.update()))
                except Exception as ex:
                    err = str(ex)
                    def show_err():
                        page.snack_bar = ft.SnackBar(ft.Text(f"Ошибка: {err}"), bgcolor=ft.Colors.ERROR_CONTAINER)
                        page.snack_bar.open = True
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
                        ft.Button("Добавить", icon=ft.Icons.ADD, on_click=add_account_dialog),
                        ft.Button("Авторизовать", icon=ft.Icons.LOGIN, on_click=auth_account_dialog),
                    ],
                    alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                ),
                ft.Divider(),
                accounts_list,
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

    MAX_LINKS_DISPLAY = 10

    def refresh_links():
        links_list.controls.clear()
        links = get_chat_links()
        if not links:
            links_list.controls.append(ft.Text("Нет ссылок. Импортируйте из xlsx.", color=ft.Colors.GREY))
        else:
            to_show = links[:MAX_LINKS_DISPLAY]
            for i, link in enumerate(to_show):
                links_list.controls.append(
                    ft.Row(
                        [
                            ft.Text(link, size=12, overflow=ft.TextOverflow.ELLIPSIS, expand=True),
                            ft.IconButton(icon=ft.Icons.DELETE, icon_size=18, on_click=lambda e, idx=i: _delete_link(idx)),
                        ],
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                    )
                )
            if len(links) > MAX_LINKS_DISPLAY:
                links_list.controls.append(ft.Text(f"Показано {MAX_LINKS_DISPLAY} из {len(links)}", size=12, color=ft.Colors.GREY))
        page.update()

    def _delete_link(idx):
        links = get_chat_links()
        if 0 <= idx < len(links):
            links.pop(idx)
            save_chat_links(links)
            refresh_links()

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
                    page.update()
                _run_on_page(page, done)
            except Exception as ex:
                def err():
                    page.pop_dialog()
                    status_text.value = f"Ошибка: {ex}"
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

            def on_progress(joined, failed):
                def update():
                    counter_text.value = f"{joined} вступлений, {failed} ошибок"
                    page.update()
                _run_on_page(page, update)

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
                actions=[],
            )
            page.show_dialog(progress_dlg)
            page.update()

            def do_join_thread():
                try:
                    results = asyncio.run(run_join_all_links(links, on_progress=on_progress))
                    total_joined = sum(r["joined"] for r in results)
                    total_failed = sum(r["failed"] for r in results)
                    summary = "\n".join([f"{r['phone']}: вступил в {r['joined']}, ошибок {r['failed']}" for r in results])
                    def done():
                        page.pop_dialog()
                        status_text.value = f"Готово. Вступлений: {total_joined}, ошибок: {total_failed}"
                        page.snack_bar = ft.SnackBar(ft.Text(summary), duration=5000)
                        page.snack_bar.open = True
                        page.update()
                    _run_on_page(page, done)
                except Exception as ex:
                    def err():
                        page.pop_dialog()
                        status_text.value = f"Ошибка: {ex}"
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
                    ],
                    spacing=12,
                    alignment=ft.MainAxisAlignment.START,
                ),
                ft.Row([pb, status_text], spacing=8),
                ft.Text("Нажмите «Импорт из xlsx» и выберите файл. Скрипт сохраняет только ссылки (t.me/...).", size=12, color=ft.Colors.GREY),
                ft.Text("Сохранённые ссылки:", size=14, weight=ft.FontWeight.W_500),
                links_list,
            ],
            expand=True,
            scroll=ft.ScrollMode.AUTO,
        ),
        padding=20,
    )


def build_profile_view(page):
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
        page.snack_bar = ft.SnackBar(ft.Text("Сохранено"))
        page.snack_bar.open = True
        page.update()
    return ft.Container(
        content=ft.Column(
            [
                ft.Text("Профиль", size=24, weight=ft.FontWeight.BOLD),
                ft.Divider(),
                ft.Row([ft.Text("Тема:"), theme_dd], alignment=ft.MainAxisAlignment.START),
                ft.Divider(),
                ft.Text("API Telegram (https://my.telegram.org)", size=14),
                ft.Row([api_id_field, api_hash_field], spacing=12),
                ft.Button("Сохранить API", on_click=save_api),
            ],
            expand=True,
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
            ev.page.snack_bar = ft.SnackBar(ft.Text(f"Шаблон «{name}» сохранён"))
            ev.page.snack_bar.open = True
            ev.page.update()

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
            page.snack_bar = ft.SnackBar(ft.Text("Выберите шаблон для удаления"))
            page.snack_bar.open = True
            page.update()
            return

        def do_delete(ev):
            delete_template(val)
            ev.page.pop_dialog()
            template_dd.options = [ft.dropdown.Option(_NONE_OPT, _NONE_OPT)] + [ft.dropdown.Option(t["name"], t["name"]) for t in get_templates()]
            template_dd.value = _NONE_OPT
            ev.page.snack_bar = ft.SnackBar(ft.Text(f"Шаблон «{val}» удалён"))
            ev.page.snack_bar.open = True
            ev.page.update()

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
                    page.snack_bar = ft.SnackBar(ft.Text("Выделите текст в поле сообщения"))
                    page.snack_bar.open = True
            page.update()
        return handler

    def link_format_click(e):
        if not last_selection[0] or last_selection[0].is_collapsed:
            page.snack_bar = ft.SnackBar(ft.Text("Выделите текст для ссылки"))
            page.snack_bar.open = True
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
                        page.snack_bar = ft.SnackBar(ft.Text(f"Ошибка: {ex}"), bgcolor=ft.Colors.ERROR_CONTAINER)
                        page.snack_bar.open = True
            refresh_attachments()
            _do_save(page, msg_field, parse_mode_dd, attachments_list)
            page.update()

    refresh_attachments()

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
                ft.Button("Сохранить", icon=ft.Icons.SAVE, on_click=lambda e: _do_save(page, msg_field, parse_mode_dd, attachments_list)),
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
                page.snack_bar = ft.SnackBar(ft.Text("Сохранено"))
                page.snack_bar.open = True
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


def run_broadcast_thread(page, pb, status_text):
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

        try:
            _run_on_page(page, lambda: (setattr(status_text, "value", "Запуск..."), setattr(pb, "visible", True), page.update()))
            stats = asyncio.run(run_broadcast(cfg, code_input=get_code))

            def show_done():
                setattr(status_text, "value", "Рассылка завершена")
                setattr(pb, "visible", False)
                done_dlg = ft.AlertDialog(
                    modal=True,
                    title=ft.Text("Рассылка завершена"),
                    content=ft.Text(f"Отправлено: {stats['success']}\nОшибок: {stats['failed']}"),
                    actions=[ft.Button("OK", on_click=lambda e: (e.page.pop_dialog(), e.page.update()))],
                )
                page.show_dialog(done_dlg)
                page.update()

            _run_on_page(page, show_done)
        except Exception as ex:
            msg = str(ex)
            _run_on_page(page, lambda m=msg: (setattr(status_text, "value", f"Ошибка: {m}"), setattr(pb, "visible", False), page.update()))

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

        def build_content(idx):
            if idx == 0:
                return build_accounts_view(p)
            if idx == 1:
                return build_stats_view(p)
            if idx == 2:
                return build_profile_view(p)
            if idx == 3:
                return build_messages_view(p)
            return build_chats_view(p, on_refresh=lambda: on_nav(4))

        def on_nav(idx):
            nav_index[0] = idx
            content_area.content = build_content(idx)
            p.update()

        status_text = ft.Text("", size=12)
        pb = ft.ProgressBar(visible=False)
        start_btn = ft.Button("Запустить рассылку", icon=ft.Icons.SEND, on_click=lambda e: run_broadcast_thread(p, pb, status_text))

        content_area.content = build_content(0)
        sidebar = build_sidebar(on_nav, 0)

        p.add(
            ft.Row(
                [
                    sidebar,
                    ft.VerticalDivider(width=1),
                    ft.Column(
                        [
                            ft.Row([content_area], expand=True),
                            ft.Divider(),
                            ft.Row([start_btn, pb, status_text], alignment=ft.MainAxisAlignment.START),
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
