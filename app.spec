import os

project_dir = os.getcwd()
assets_dir = os.path.join(project_dir, "assets")

a = Analysis(
    ["app.py"],
    pathex=[project_dir],
    binaries=[],
    datas=[
        (os.path.join(assets_dir, "icon.ico"), "assets"),
        (os.path.join(assets_dir, "login.png"), "assets"),
        (os.path.join(assets_dir, "notification.mp3"), "assets"),
    ] if os.path.exists(assets_dir) else [],
    hiddenimports=[
        "flet",
        "telethon",
        "telethon.client",
        "telethon.tl",
        "openpyxl",
        "argon2",
        "argon2.low_level",
        "PIL",
        "auth",
        "broadcast",
        "chats",
        "core",
        "dashboard",
        "errors",
        "ai_generate",
        "updater",
        "version",
        "tdata_import",
        "validate_accounts",
        "schedule",
        "tray",
        "playsound3",
        "pystray",
        "opentele",
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["PyQt5", "PyQt6", "PySide2", "PySide6"],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name="SpammerBot",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=os.path.join(assets_dir, "icon.ico") if os.path.exists(os.path.join(assets_dir, "icon.ico")) else None,
)
