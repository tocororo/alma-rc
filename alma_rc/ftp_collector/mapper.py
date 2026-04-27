"""Maps FTP/HTTP file entries to InvenioRDM record metadata."""
import logging
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

from alma_rc.ftp_collector.scanner import FileEntry
from alma_rc.insert_data.invenio_record import (
    Access,
    Files,
    FilesSimple,
    Identifier,
    IdentifiersWithScheme,
    InveniordmRecordSchemaV600,
    Language,
    Metadata,
    Record,
    ResourceType,
    Scheme,
    Subject,
    Subjects,
)

logger = logging.getLogger(__name__)

# File extension → InvenioRDM resource type id
EXTENSION_RESOURCE_TYPE: Dict[str, str] = {
    # Documents
    ".pdf": "publication",
    ".doc": "publication",
    ".docx": "publication",
    ".odt": "publication",
    ".rtf": "publication",
    ".txt": "publication",
    # Presentations
    ".ppt": "presentation",
    ".pptx": "presentation",
    ".odp": "presentation",
    # Spreadsheets / datasets
    ".xls": "dataset",
    ".xlsx": "dataset",
    ".ods": "dataset",
    ".csv": "dataset",
    # Images
    ".jpg": "image",
    ".jpeg": "image",
    ".png": "image",
    ".gif": "image",
    ".tif": "image",
    ".tiff": "image",
    ".svg": "image",
    # Video
    ".mp4": "video",
    ".avi": "video",
    ".mov": "video",
    ".mkv": "video",
    ".wmv": "video",
    # Audio
    ".mp3": "audio",
    ".wav": "audio",
    ".ogg": "audio",
    ".flac": "audio",
    # Software / archives
    ".zip": "software",
    ".rar": "software",
    ".tar": "software",
    ".gz": "software",
    ".py": "software",
    ".java": "software",
    ".js": "software",
}

# Folder name (normalised) → InvenioRDM resource type id.
# Checked before extension — a file inside "Libros/" is a book even if it's a PDF.
FOLDER_RESOURCE_TYPE: Dict[str, str] = {
    "libros": "publication-book",
    "libro": "publication-book",
    "books": "publication-book",
    "tesis": "publication-thesis",
    "tesinas": "publication-thesis",
    "thesis": "publication-thesis",
    "articulos": "publication-article",
    "articulos-cientificos": "publication-article",
    "articles": "publication-article",
    "revistas": "publication-journal",
    "journals": "publication-journal",
    "presentaciones": "presentation",
    "presentations": "presentation",
    "videos": "video",
    "imagenes": "image",
    "images": "image",
    "fotos": "image",
    "fotografias": "image",
    "software": "software",
    "programas": "software",
    "aplicaciones": "software",
    "datos": "dataset",
    "data": "dataset",
    "datasets": "dataset",
    "informes": "publication-report",
    "reports": "publication-report",
    "working-papers": "publication-workingpaper",
    "conferencias": "publication-conferencepaper",
    "conferences": "publication-conferencepaper",
    "actas": "publication-conferencepaper",
}


def _normalize(text: str) -> str:
    """Lowercase, strip accents, replace separators with hyphens."""
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    return ascii_str.lower().replace("_", "-").replace(" ", "-").strip("-")


def infer_resource_type(entry: FileEntry) -> str:
    """Return InvenioRDM resource type id using folder heuristics then file extension."""
    for part in entry.folder_path.replace("\\", "/").split("/"):
        if not part:
            continue
        key = _normalize(part)
        if key in FOLDER_RESOURCE_TYPE:
            return FOLDER_RESOURCE_TYPE[key]

    ext = Path(entry.name).suffix.lower()
    return EXTENSION_RESOURCE_TYPE.get(ext, "other")


def folder_path_to_subjects(folder_path: str) -> List[Subject]:
    """Convert each folder segment in the path into a Subject keyword."""
    subjects = []
    for part in folder_path.replace("\\", "/").split("/"):
        part = part.strip()
        if part:
            subjects.append(Subject(subject=part))
    return subjects


def filename_to_title(filename: str) -> str:
    """Derive a human-readable title from a filename stem."""
    stem = Path(filename).stem
    return stem.replace("_", " ").replace("-", " ").strip().capitalize() or filename


def build_invenio_record(
    entry: FileEntry,
    community_id: Optional[str] = None,
    publisher: Optional[str] = None,
    language: Optional[str] = None,
) -> InveniordmRecordSchemaV600:
    """Build an InveniordmRecordSchemaV600 from a FileEntry.

    The source URL (entry.path) is stored as an identifier with scheme 'url'.
    Files are disabled — no binary upload takes place.
    community_id is stored in custom_fields for later community association.
    """
    resource_type_id = infer_resource_type(entry)
    title = filename_to_title(entry.name)
    subjects = folder_path_to_subjects(entry.folder_path)

    identifiers = [
        IdentifiersWithScheme(
            identifier=Identifier(__root__=entry.path),
            scheme=Scheme(__root__="url"),
        )
    ]

    metadata = Metadata(
        resource_type=ResourceType(id=resource_type_id),
        title=title,
        subjects=Subjects(__root__=subjects) if subjects else None,
        publisher=publisher,
        languages=[Language(id=language)] if language else None,
        identifiers=identifiers,
    )

    custom_fields: Optional[dict] = None
    if community_id:
        custom_fields = {"ftp_source_community": community_id}

    return InveniordmRecordSchemaV600(
        metadata=metadata,
        access=Access(record=Record.public, files=Files.public),
        files=FilesSimple(enabled=False),
        custom_fields=custom_fields,
    )
