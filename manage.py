import argparse
import asyncio
import sys

from core import CONFIG_PATH, load_config, save_config, MAX_ACCOUNTS


def _check_auth():
    from auth import require_auth
    return require_auth()


def cmd_init_auth(args):
    from auth import init_auth
    try:
        init_auth()
    except (ValueError, RuntimeError) as e:
        print(e)
        sys.exit(1)


def cmd_setup(args):
    if not _check_auth():
        sys.exit(1)
    print("=== Настройка ===\n")
    print("API credentials: https://my.telegram.org")

    api_id = int(input("api_id: ").strip())
    api_hash = input("api_hash: ").strip()

    while True:
        try:
            count = int(input(f"Аккаунтов (1-{MAX_ACCOUNTS}): ").strip())
            if 1 <= count <= MAX_ACCOUNTS:
                break
        except ValueError:
            pass
        print("Введи число от 1 до 10")

    accounts = []
    for i in range(count):
        print(f"\nАккаунт {i + 1}:")
        phone = input("  Телефон (+7...): ").strip()
        password = input("  Пароль 2FA (пусто если нет): ").strip()
        accounts.append({"phone": phone, "password": password})

    print("\nТекст сообщения (END с новой строки - конец):")
    lines = []
    while True:
        line = input()
        if line.strip().upper() == "END":
            break
        lines.append(line)
    message = "\n".join(lines).strip()
    if not message:
        print("Сообщение не может быть пустым")
        sys.exit(1)

    cfg = {
        "api_id": api_id,
        "api_hash": api_hash,
        "accounts": accounts,
        "message": message,
    }
    save_config(cfg)
    print(f"\nСохранено: {CONFIG_PATH.resolve()}")


def cmd_accounts(args):
    if not _check_auth():
        sys.exit(1)
    cfg = load_config()
    if not cfg:
        print("Конфиг не найден. Сделай: python manage.py setup")
        return
    for i, acc in enumerate(cfg["accounts"], 1):
        mask = "***" if acc.get("password") else "-"
        print(f"  {i}. {acc['phone']} 2FA:{mask}")


def cmd_message(args):
    if not _check_auth():
        sys.exit(1)
    cfg = load_config()
    if not cfg:
        print("Конфиг не найден. Сделай: python manage.py setup")
        return
    if args.set:
        print("Новый текст (END - конец):")
        lines = []
        while True:
            line = input()
            if line.strip().upper() == "END":
                break
            lines.append(line)
        cfg["message"] = "\n".join(lines).strip()
        save_config(cfg)
        print("Сообщение обновлено")
    else:
        print("--- Текущее сообщение ---")
        print(cfg["message"])
        print("---")


def cmd_run(args):
    if not _check_auth():
        sys.exit(1)
    cfg = load_config()
    if not cfg:
        print("Конфиг не найден. Сделай: python manage.py setup")
        sys.exit(1)
    from broadcast import run_broadcast
    import asyncio
    asyncio.run(run_broadcast(cfg))


def cmd_reset_password(args):
    from auth import reset_password
    if reset_password():
        return
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-auth")
    sub.add_parser("setup")
    sub.add_parser("accounts")
    msg_p = sub.add_parser("message")
    msg_p.add_argument("--set", action="store_true")
    sub.add_parser("run")
    sub.add_parser("reset-password", help="Сбросить пароль доступа при утере")

    args = parser.parse_args()
    handlers = {
        "init-auth": cmd_init_auth,
        "setup": cmd_setup,
        "accounts": cmd_accounts,
        "message": cmd_message,
        "run": cmd_run,
        "reset-password": cmd_reset_password,
    }
    handlers[args.cmd](args)


if __name__ == "__main__":
    main()
