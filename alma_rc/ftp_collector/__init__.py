"""FTP/HTTP repository collector for InvenioRDM."""
from alma_rc.ftp_collector.collector import FTPCollector, RepoConfig
from alma_rc.ftp_collector.mapper import build_invenio_record, infer_resource_type
from alma_rc.ftp_collector.scanner import ApacheHTTPScanner, FileEntry, FTPScanner

__all__ = [
    "FTPCollector",
    "RepoConfig",
    "FTPScanner",
    "ApacheHTTPScanner",
    "FileEntry",
    "build_invenio_record",
    "infer_resource_type",
]
