"""Scanners for FTP servers, Apache AutoIndex and h5ai directory listings."""
import ftplib
import logging
from dataclasses import dataclass
from typing import Iterator, Optional
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
        """Parse an h5ai directory listing from its server-rendered HTML.

        h5ai pre-renders all items inside <ul id="items"> as <li> elements.
        Each <li class="item file"> is a file; <li class="item folder"> is a
        subdirectory.  Parent-dir entries carry the extra "folder-parent" class
        and are skipped.  An empty items list means the directory is empty.
        """
        tree = html.fromstring(content)
        for li in tree.xpath("//ul[@id='items']/li[contains(@class,'item')]"):
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

            if "item file" in classes:
                size: Optional[int] = None
                span = li.find(".//span[@class='size']")
                if span is not None:
                    try:
                        size = int(span.get("data-bytes", ""))
                    except (ValueError, TypeError):
                        pass
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
