import re
import urllib.request
import urllib.error
import json
from typing import Optional
from dataclasses import dataclass

from version import APP_VERSION


@dataclass
class UpdateInfo:
    version: str
    url: str
    notes: str = ""


def _parse_version(v: str) -> tuple:
    v = re.sub(r"^v", "", str(v or "0").strip())
    parts = re.findall(r"\d+", v)
    return tuple(int(p) for p in (parts[:3] or [0]))


def _version_newer(current: str, latest: str) -> bool:
    try:
        a = _parse_version(current)
        b = _parse_version(latest)
        for i in range(max(len(a), len(b))):
            x = a[i] if i < len(a) else 0
            y = b[i] if i < len(b) else 0
            if y > x:
                return True
            if y < x:
                return False
        return False
    except Exception:
        return False


def _fetch_github_latest(owner: str, repo: str) -> Optional[UpdateInfo]:
    url = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/vnd.github.v3+json"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        version = data.get("tag_name", "").strip()
        html_url = data.get("html_url", "")
        body = (data.get("body") or "")[:500]
        assets = data.get("assets") or []
        download_url = html_url
        for a in assets:
            name = (a.get("name") or "").lower()
            if name.endswith(".exe") or "win" in name:
                download_url = a.get("browser_download_url", html_url)
                break
        return UpdateInfo(version=version, url=download_url, notes=body)
    except Exception:
        return None


def _fetch_custom_json(url: str) -> Optional[UpdateInfo]:
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        version = str(data.get("version", "")).strip()
        url_dl = str(data.get("url", data.get("download_url", ""))).strip()
        notes = str(data.get("notes", data.get("body", "")))[:500]
        if version and url_dl:
            return UpdateInfo(version=version, url=url_dl, notes=notes)
        return None
    except Exception:
        return None


def check_for_updates(update_url: Optional[str] = None) -> Optional[UpdateInfo]:
    if not update_url or not str(update_url).strip():
        return None
    url = str(update_url).strip()
    if "api.github.com" in url and "/repos/" in url:
        m = re.search(r"github\.com/repos/([^/]+)/([^/?]+)", url)
        if m:
            info = _fetch_github_latest(m.group(1), m.group(2).split("/")[0])
            if info and _version_newer(APP_VERSION, info.version):
                return info
        return None
    info = _fetch_custom_json(url)
    if info and _version_newer(APP_VERSION, info.version):
        return info
    return None
