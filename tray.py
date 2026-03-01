import sys
import threading
from pathlib import Path

_tray_icon = None
_tray_thread = None


def _make_tray_image(icon_path: Path | None):
    try:
        from PIL import Image
        if icon_path and icon_path.exists():
            img = Image.open(icon_path)
            if img.mode != "RGBA":
                img = img.convert("RGBA")
        else:
            img = Image.new("RGBA", (64, 64), (70, 130, 180, 255))
        size = (64, 64)
        img = img.resize(size, Image.Resampling.LANCZOS)
        return img
    except Exception:
        from PIL import Image
        return Image.new("RGBA", (64, 64), (70, 130, 180, 255))


def run_tray(icon_path: Path | None, page):
    global _tray_icon, _tray_thread

    def on_open(icon, item):
        def do():
            try:
                page.window.visible = True
                page.update()
            except Exception:
                pass
        if hasattr(page, "loop") and page.loop:
            page.loop.call_soon_threadsafe(do)
        else:
            do()

    def on_quit(icon, item):
        icon.stop()
        def do():
            try:
                page.window.destroy()
            except Exception:
                pass
        if hasattr(page, "loop") and page.loop:
            page.loop.call_soon_threadsafe(do)
        else:
            do()

    try:
        import pystray
        img = _make_tray_image(icon_path)
        menu = pystray.Menu(
            pystray.MenuItem("Открыть", on_open, default=True),
            pystray.MenuItem("Выход", on_quit),
        )
        _tray_icon = pystray.Icon("spammer-bot", img, "Рассылка", menu)

        def run():
            _tray_icon.run()

        _tray_thread = threading.Thread(target=run, daemon=True)
        _tray_thread.start()
    except Exception:
        pass
