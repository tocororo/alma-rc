"""Scanners for FTP servers and Apache HTTP directory listings."""
import ftplib
import logging
from dataclasses import dataclass
from typing import Iterator, Optional
from urllib.parse import urljoin, urlparse

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



class ApacheHTTPScanner:
    """Recursively scans an Apache HTTP directory listing and yields FileEntry objects."""

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

    def scan(self, start_path: str = "/") -> Iterator[FileEntry]:
        """Recursively yield FileEntry for every file reachable from start_path."""
        url = urljoin(self.base_url + "/", start_path.lstrip("/"))
        yield from self._scan_url(url, "")

    def _scan_url(self, url: str, relative_folder: str) -> Iterator[FileEntry]:
        try:
            resp = self._session.get(url, timeout=30, verify=self.verify_ssl)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Cannot fetch {url}: {e}")
            return

        tree = html.fromstring(resp.content)
        base_path = urlparse(url).path

        for href in tree.xpath("//a/@href"):
            # Skip parent-directory links and query strings
            if href in ("../", "./", "/") or href.startswith("?"):
                continue
            # Skip absolute links that leave the base path
            if href.startswith("/") and not href.startswith(base_path):
                continue

            full_url = urljoin(url, href)
            name = href.rstrip("/").split("/")[-1]

            if href.endswith("/"):
                sub_folder = f"{relative_folder}/{name}".lstrip("/")
                yield from self._scan_url(full_url, sub_folder)
            else:
                yield FileEntry(
                    name=name,
                    path=full_url,
                    folder_path=relative_folder,
                    size=None,
                    repo_id=self.repo_id,
                )

