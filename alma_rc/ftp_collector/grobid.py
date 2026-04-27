"""Grobid-based metadata extractor for text documents.

Grobid (https://github.com/kermitt2/grobid) runs as a REST service.
Start it with:  podman-compose up -d grobid
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import requests
from lxml import etree

logger = logging.getLogger(__name__)

# Extensions that Grobid can process (PDF is best supported; others via conversion)
SUPPORTED_EXTENSIONS = {".pdf", ".doc", ".docx", ".odt", ".rtf"}

# TEI XML namespace used in all Grobid responses
_TEI_NS = "http://www.tei-c.org/ns/1.0"
_NS = {"tei": _TEI_NS}


def is_processable(filename: str) -> bool:
    """Return True if the file type is supported by Grobid."""
    return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS


class GrobidExtractor:
    """Calls the Grobid REST API to extract metadata from document files.

    Only the document header is processed (processHeaderDocument endpoint),
    which is significantly faster than full-text analysis and sufficient to
    obtain title, authors, abstract, date and keywords.
    """

    DEFAULT_URL = "http://localhost:8070"

    def __init__(self, base_url: str = DEFAULT_URL, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        """Return True if the Grobid service is reachable."""
        try:
            resp = self._session.get(f"{self.base_url}/api/isalive", timeout=5)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def extract_from_url(
        self, file_url: str, verify_ssl: bool = False
    ) -> Optional[Dict]:
        """Download file_url, send to Grobid, return extracted metadata dict.

        Returns a dict with any subset of:
            title, authors (list), abstract, date (year string), keywords (list).
        Returns None if download or extraction fails.
        """
        suffix = Path(file_url.split("?")[0]).suffix or ".bin"
        tmp_path = None
        try:
            with self._session.get(
                file_url, stream=True, timeout=60, verify=verify_ssl
            ) as r:
                r.raise_for_status()
                with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                    tmp_path = tmp.name
                    for chunk in r.iter_content(chunk_size=8192):
                        tmp.write(chunk)
        except Exception as e:
            logger.warning(f"Grobid: could not download {file_url}: {e}")
            return None

        try:
            return self.extract_from_file(tmp_path)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    def extract_from_file(self, file_path: str) -> Optional[Dict]:
        """Send a local file to Grobid and return extracted metadata dict."""
        try:
            with open(file_path, "rb") as f:
                resp = self._session.post(
                    f"{self.base_url}/api/processHeaderDocument",
                    files={
                        "input": (
                            os.path.basename(file_path),
                            f,
                            "application/octet-stream",
                        )
                    },
                    timeout=self.timeout,
                )
        except Exception as e:
            logger.warning(f"Grobid: request failed for {file_path}: {e}")
            return None

        if resp.status_code != 200:
            logger.warning(
                f"Grobid: HTTP {resp.status_code} for {file_path}"
            )
            return None

        return self._parse_tei(resp.text)

    # ------------------------------------------------------------------
    # TEI XML parser
    # ------------------------------------------------------------------

    def _parse_tei(self, tei_xml: str) -> Dict:
        """Parse a Grobid TEI XML response into a flat metadata dict."""
        try:
            root = etree.fromstring(tei_xml.encode("utf-8"))
        except etree.XMLSyntaxError as e:
            logger.warning(f"Grobid: could not parse TEI response: {e}")
            return {}

        result: Dict = {}

        # Title
        title_el = root.find(
            ".//tei:titleStmt/tei:title[@type='main']", _NS
        )
        if title_el is None:
            # Some versions use a different path
            title_el = root.find(".//tei:analytic/tei:title[@type='main']", _NS)
        if title_el is not None:
            text = "".join(title_el.itertext()).strip()
            if text:
                result["title"] = text

        # Authors
        authors: List[str] = []
        for author_el in root.findall(".//tei:analytic/tei:author", _NS):
            forename = author_el.find(".//tei:forename", _NS)
            surname = author_el.find(".//tei:surname", _NS)
            parts = [
                p.text.strip()
                for p in (forename, surname)
                if p is not None and p.text
            ]
            if parts:
                authors.append(" ".join(parts))
        if authors:
            result["authors"] = authors

        # Abstract
        abstract_el = root.find(".//tei:abstract", _NS)
        if abstract_el is not None:
            text = " ".join(abstract_el.itertext()).strip()
            if text:
                result["abstract"] = text

        # Publication date (keep only the year if partial)
        date_el = root.find(
            ".//tei:publicationStmt/tei:date[@type='published']", _NS
        )
        if date_el is not None:
            when = date_el.get("when", "")
            if when:
                result["date"] = when[:4]

        # Keywords
        keywords = [
            t.text.strip()
            for t in root.findall(".//tei:keywords/tei:term", _NS)
            if t.text
        ]
        if keywords:
            result["keywords"] = keywords

        return result


# ---------------------------------------------------------------------------
# Record merging
# ---------------------------------------------------------------------------

def apply_grobid_metadata(record_dict: dict, grobid_meta: dict) -> None:
    """Merge Grobid-extracted fields into a record dict in-place.

    Grobid results take precedence for title; all other fields are additive
    (creators, description, publication_date, keywords as subjects).
    """
    if not grobid_meta:
        return

    meta = record_dict.setdefault("metadata", {})

    if grobid_meta.get("title"):
        meta["title"] = grobid_meta["title"]

    if grobid_meta.get("authors"):
        creators = []
        for name in grobid_meta["authors"]:
            # Best-effort split: last token is family name
            tokens = name.rsplit(" ", 1)
            person: dict = {"name": name, "type": "personal"}
            if len(tokens) == 2:
                person["given_name"] = tokens[0]
                person["family_name"] = tokens[1]
            creators.append({"person_or_org": person})
        meta["creators"] = creators

    if grobid_meta.get("abstract"):
        meta["description"] = grobid_meta["abstract"]

    if grobid_meta.get("date"):
        meta["publication_date"] = grobid_meta["date"]

    if grobid_meta.get("keywords"):
        existing = meta.get("subjects", [])
        seen = {s.get("subject", "") for s in existing}
        for kw in grobid_meta["keywords"]:
            if kw not in seen:
                existing.append({"subject": kw})
                seen.add(kw)
        meta["subjects"] = existing
