"""Scanners for FTP servers, Apache AutoIndex and h5ai directory listings."""
import ftplib
import json
import logging
import re
from dataclasses import dataclass
from typing import Iterator, List, Optional
from urllib.parse import unquote, urljoin, urlparse

import requests
from lxml import html

logger = logging.getLogger(__name__)


@dataclass
class FileEntry:
    """A single file discovered in a remote repository."""
    name: str
    path: str           # full remote path or URL
    folder_path: str    # path of parent folders relative to repo root
    size: Optional[int]
    repo_id: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_size_str(size_str: str) -> Optional[int]:
    """Convert human-readable size string to bytes.

    Handles plain integers and suffixed values: K, M, G, T (case-insensitive).
    Returns None for unknown/empty/directory values ('-', '?', '').
    """
    s = size_str.strip().replace("\xa0", "").replace(",", "").replace(" ", "")
    if not s or s in ("-", "–", "?"):
        return None
    suffixes = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}
    if s[-1].lower() in suffixes:
        try:
            return int(float(s[:-1]) * suffixes[s[-1].lower()])
        except ValueError:
            return None
    try:
        return int(s)
    except ValueError:
        return None


def _detect_listing_type(content: bytes) -> str:
    """Return 'h5ai' or 'apache' based on page content."""
    snippet = content[:8192].decode("utf-8", errors="ignore")
    if "_h5ai" in snippet or "h5ai" in snippet:
        return "h5ai"
    return "apache"


# ---------------------------------------------------------------------------
# FTP
# ---------------------------------------------------------------------------

class FTPScanner:
    """Recursively scans an FTP server and yields FileEntry objects."""

    def __init__(
        self,
        host: str,
        user: str = "anonymous",
        password: str = "",
        port: int = 21,
        repo_id: str = "",
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.repo_id = repo_id
        self._ftp: Optional[ftplib.FTP] = None

    def connect(self) -> None:
        self._ftp = ftplib.FTP()
        self._ftp.connect(self.host, self.port, timeout=30)
        self._ftp.login(self.user, self.password)
        logger.info(f"Connected to FTP {self.host}:{self.port}")

    def disconnect(self) -> None:
        if self._ftp:
            try:
                self._ftp.quit()
            except Exception:
                self._ftp.close()
            self._ftp = None

    def scan(self, start_path: str = "/") -> Iterator[FileEntry]:
        """Recursively yield FileEntry for every file under start_path."""
        if not self._ftp:
            self.connect()
        yield from self._scan_dir(start_path, "")

    def _scan_dir(self, remote_path: str, relative_folder: str) -> Iterator[FileEntry]:
        entries: list[str] = []
        try:
            self._ftp.retrlines(f"LIST {remote_path}", entries.append)
        except ftplib.Error as e:
            logger.warning(f"Cannot list {remote_path}: {e}")
            return

        for entry in entries:
            parts = entry.split(None, 8)
            if len(parts) < 9:
                continue
            permissions, _, _, _, size_str, *_, name = parts
            full_path = f"{remote_path.rstrip('/')}/{name}"

            if permissions.startswith("d"):
                sub_folder = f"{relative_folder}/{name}".lstrip("/")
                yield from self._scan_dir(full_path, sub_folder)
            else:
                try:
                    size = int(size_str)
                except ValueError:
                    size = None
                yield FileEntry(
                    name=name,
                    path=full_path,
                    folder_path=relative_folder,
                    size=size,
                    repo_id=self.repo_id,
                )


# ---------------------------------------------------------------------------
# HTTP (Apache AutoIndex + h5ai)
# ---------------------------------------------------------------------------

class ApacheHTTPScanner:
    """Recursively scans Apache AutoIndex and h5ai directory listings."""

    def __init__(
        self,
        base_url: str,
        repo_id: str = "",
        verify_ssl: bool = False,
    ):
        self.base_url = base_url.rstrip("/")
        self.repo_id = repo_id
        self.verify_ssl = verify_ssl
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def scan(self, start_path: str = "/") -> Iterator[FileEntry]:
        """Recursively yield FileEntry for every file reachable from start_path."""
        url = urljoin(self.base_url + "/", start_path.lstrip("/"))
        yield from self._scan_url(url, "")

    # ------------------------------------------------------------------
    # Internal dispatch
    # ------------------------------------------------------------------

    def _fetch(self, url: str) -> Optional[requests.Response]:
        try:
            resp = self._session.get(url, timeout=30, verify=self.verify_ssl)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"Cannot fetch {url}: {e}")
            return None

    def _scan_url(self, url: str, relative_folder: str) -> Iterator[FileEntry]:
        resp = self._fetch(url)
        if not resp:
            return
        listing_type = _detect_listing_type(resp.content)
        if listing_type == "h5ai":
            yield from self._scan_h5ai(url, resp.content, relative_folder)
        else:
            yield from self._scan_apache_autoindex(url, resp.content, relative_folder)

    # ------------------------------------------------------------------
    # Apache AutoIndex
    # ------------------------------------------------------------------

    def _scan_apache_autoindex(
        self, url: str, content: bytes, relative_folder: str
    ) -> Iterator[FileEntry]:
        """Parse standard Apache AutoIndex HTML.

        Extracts file links from <tr> rows and sizes from the 4th <td> column.
        Falls back to plain <a> link extraction for non-table layouts.
        """
        tree = html.fromstring(content)
        base_path = urlparse(url).path
        seen: set = set()

        # --- Table layout (most common Apache autoindex) ---
        for a_elem in tree.xpath("//tr//td//a"):
            href = a_elem.get("href", "")
            if not href or href in seen:
                continue
            if href in ("../", "./", "/") or href.startswith("?"):
                continue
            if href.startswith("/") and not href.startswith(base_path):
                continue
            seen.add(href)

            name = unquote(href.rstrip("/").split("/")[-1])
            if not name:
                continue
            full_url = urljoin(url, href)
            size = self._size_from_row(a_elem)

            if href.endswith("/"):
                sub = f"{relative_folder}/{name}".lstrip("/")
                yield from self._scan_url(full_url, sub)
            else:
                yield FileEntry(
                    name=name, path=full_url,
                    folder_path=relative_folder, size=size,
                    repo_id=self.repo_id,
                )

        # --- Fallback: generic <a href> scan (pre-based or minimal listings) ---
        if not seen:
            for href in tree.xpath("//a/@href"):
                if href in ("../", "./", "/") or href.startswith("?"):
                    continue
                if href.startswith("/") and not href.startswith(base_path):
                    continue
                name = unquote(href.rstrip("/").split("/")[-1])
                if not name:
                    continue
                full_url = urljoin(url, href)
                if href.endswith("/"):
                    sub = f"{relative_folder}/{name}".lstrip("/")
                    yield from self._scan_url(full_url, sub)
                else:
                    yield FileEntry(
                        name=name, path=full_url,
                        folder_path=relative_folder, size=None,
                        repo_id=self.repo_id,
                    )

    def _size_from_row(self, a_elem) -> Optional[int]:
        """Walk up from <a> to its <tr> and read the size cell (index 3)."""
        node = a_elem.getparent()
        while node is not None and node.tag != "tr":
            node = node.getparent()
        if node is None:
            return None
        cells = node.findall(".//td")
        # Apache standard: icon | name | date | size | description
        if len(cells) >= 4:
            return _parse_size_str(cells[3].text_content())
        return None

    # ------------------------------------------------------------------
    # h5ai
    # ------------------------------------------------------------------

    def _scan_h5ai(
        self, url: str, content: bytes, relative_folder: str
    ) -> Iterator[FileEntry]:
        """Parse an h5ai directory listing.

        Strategy:
        1. Try the h5ai JSON API (requires PHP access — may be blocked).
        2. Fall back to parsing the server-rendered <ul id="items"> HTML, which
           h5ai always emits for non-JS clients.  Each <li class="item file">
           carries the file href and a <span class="size" data-bytes="N">.
           Folders have class "item folder"; parent-dir links also have
           "folder-parent" and are skipped.
        """
        h5ai_root = self._find_h5ai_root(content)
        if h5ai_root:
            items = self._fetch_h5ai_json(url, h5ai_root)
            if items is not None:
                yield from self._yield_h5ai_items(items, url, relative_folder)
                return

        # API blocked or unavailable — parse the server-rendered HTML.
        logger.debug(f"h5ai API unavailable for {url}, parsing h5ai HTML")
        tree = html.fromstring(content)
        li_items = tree.xpath("//ul[@id='items']/li[contains(@class,'item')]")
        if li_items:
            yield from self._parse_h5ai_li_items(li_items, url, relative_folder)
        else:
            # <ul id="items"> is JS-populated and empty in the initial response.
            # Fall back to the generic link scanner, which works on whatever
            # the server rendered (noscript fallback, plain hrefs, etc.).
            logger.debug(f"h5ai items list empty, using autoindex fallback for {url}")
            yield from self._scan_apache_autoindex(url, content, relative_folder)

    def _parse_h5ai_li_items(
        self, li_items: list, url: str, relative_folder: str
    ) -> Iterator[FileEntry]:
        """Yield FileEntry objects from a list of h5ai <li class='item'> elements."""
        for li in li_items:
            classes = li.get("class", "")

            if "folder-parent" in classes:
                continue

            a_elem = li.find(".//a")
            if a_elem is None:
                continue
            href = a_elem.get("href", "")
            if not href:
                continue

            name = unquote(href.rstrip("/").split("/")[-1])
            if not name:
                continue

            full_url = urljoin(url, href)

            size: Optional[int] = None
            span = li.find(".//span[@class='size']")
            if span is not None:
                db = span.get("data-bytes", "")
                try:
                    size = int(db)
                except (ValueError, TypeError):
                    pass

            if "item file" in classes:
                yield FileEntry(
                    name=name,
                    path=full_url,
                    folder_path=relative_folder,
                    size=size,
                    repo_id=self.repo_id,
                )
            elif "item folder" in classes:
                sub = f"{relative_folder}/{name}".lstrip("/")
                yield from self._scan_url(full_url, sub)

    def _find_h5ai_root(self, content: bytes) -> Optional[str]:
        """Return the h5ai installation root path (e.g. '/_h5ai'), or None."""
        snippet = content[:8192].decode("utf-8", errors="ignore")
        m = re.search(r"(/_h5ai)", snippet)
        return m.group(1) if m else None

    def _fetch_h5ai_json(self, url: str, h5ai_root: str) -> Optional[List[dict]]:
        """Request directory items from the h5ai JSON API.

        Tries both known endpoint paths across h5ai versions.
        """
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        path = parsed.path

        endpoints = [
            f"{h5ai_root}/server/php/index.php",   # h5ai ≤ 0.28
            f"{h5ai_root}/public/json.php",         # h5ai ≥ 0.29
        ]
        payload = json.dumps({"action": "get", "items": {"href": path, "what": 1}})
        headers = {"Content-Type": "application/json"}

        for endpoint in endpoints:
            api_url = urljoin(origin + "/", endpoint.lstrip("/"))
            try:
                resp = self._session.post(
                    api_url, data=payload, headers=headers,
                    timeout=15, verify=self.verify_ssl,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if "items" in data:
                        logger.info(f"h5ai JSON API answered at {api_url}")
                        return data["items"]
            except Exception as e:
                logger.debug(f"h5ai API {api_url} failed: {e}")

        return None

    def _yield_h5ai_items(
        self, items: List[dict], base_url: str, relative_folder: str
    ) -> Iterator[FileEntry]:
        """Yield FileEntry objects from h5ai JSON API response items."""
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        for item in items:
            href = item.get("href", "")
            if not href:
                continue
            name = unquote(href.rstrip("/").split("/")[-1])
            if not name:
                continue

            full_url = urljoin(origin + "/", href.lstrip("/"))
            size: Optional[int] = item.get("size")  # None for directories

            is_folder = href.endswith("/") or size is None
            if is_folder:
                sub = f"{relative_folder}/{name}".lstrip("/")
                yield from self._scan_url(full_url, sub)
            else:
                yield FileEntry(
                    name=name, path=full_url,
                    folder_path=relative_folder, size=size,
                    repo_id=self.repo_id,
                )
