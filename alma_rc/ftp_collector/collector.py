"""Orchestrates scanning FTP/HTTP repositories and inserting records into InvenioRDM."""
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from alma_rc.ftp_collector.mapper import build_invenio_record
from alma_rc.ftp_collector.scanner import ApacheHTTPScanner, FileEntry, FTPScanner
from alma_rc.insert_data.invenio_actions import InvenioClient

logger = logging.getLogger(__name__)


@dataclass
class RepoConfig:
    """Configuration for a single FTP or HTTP repository.

    Each repo maps to one community in InvenioRDM.
    """

    repo_id: str
    host: str
    type: str                        # 'ftp' or 'http'
    community_id: Optional[str] = None
    start_path: str = "/"
    # FTP credentials
    user: str = "anonymous"
    password: str = ""
    port: int = 21
    # HTTP options
    base_url: Optional[str] = None   # full base URL if different from http://{host}
    verify_ssl: bool = False
    # Record defaults
    publisher: Optional[str] = None
    language: str = "spa"
    # Safety limit — set to None to process all files
    max_files: Optional[int] = None
    # When True, build records and save to JSON but do NOT publish to InvenioRDM
    dry_run: bool = False


class FTPCollector:
    """Scans FTP/HTTP repositories and inserts metadata records into InvenioRDM.

    Files are NOT downloaded. The remote URL is stored as an identifier on each record.
    """

    def __init__(self, invenio_client: Optional[InvenioClient] = None):
        self.client = invenio_client or InvenioClient()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect_repo(
        self, config: RepoConfig, output_path: Optional[str] = None
    ) -> List[Dict]:
        """Scan one repository and insert all records.

        Args:
            config: Repository configuration.
            output_path: If provided, save results as JSON to this path.

        Returns:
            List of result dicts with keys: source_url, invenio_id, title, resource_type.
        """
        mode = "dry-run" if config.dry_run else "live"
        logger.info(
            f"Starting collection [{mode}] for repo '{config.repo_id}' "
            f"({config.type}://{config.host})"
        )
        scanner = self._build_scanner(config)
        results: List[Dict] = []
        count = 0

        for entry in scanner.scan(config.start_path):
            if config.max_files is not None and count >= config.max_files:
                logger.info(f"Reached max_files={config.max_files}, stopping")
                break

            result = self._process_entry(entry, config)
            if result:
                results.append(result)
                if config.dry_run:
                    logger.info(f"[{config.repo_id}][dry-run] {result['source_url']}")
                else:
                    logger.info(
                        f"[{config.repo_id}] {result['source_url']} → {result['invenio_id']}"
                    )
            count += 1

        action = "prepared (dry-run)" if config.dry_run else "inserted"
        logger.info(
            f"Repo '{config.repo_id}': scanned {count} files, "
            f"{action} {len(results)} records"
        )

        total_bytes = sum(r.get("size_bytes") or 0 for r in results)
        if output_path:
            _save_json(
                {
                    "repo_id": config.repo_id,
                    "total": len(results),
                    "total_size_bytes": total_bytes,
                    "total_size": _format_bytes(total_bytes),
                    "records": results,
                },
                output_path,
            )

        return results

    def collect_all(
        self, repos: List[RepoConfig], output_path: Optional[str] = None
    ) -> Dict[str, List[Dict]]:
        """Collect from multiple repositories sequentially.

        Args:
            repos: List of repository configurations.
            output_path: If provided, save the aggregated results as JSON to this path.

        Returns:
            Mapping repo_id → list of result dicts.
        """
        all_results: Dict[str, List[Dict]] = {}
        for config in repos:
            try:
                all_results[config.repo_id] = self.collect_repo(config)
            except Exception as e:
                logger.error(f"Failed to collect repo '{config.repo_id}': {e}")
                all_results[config.repo_id] = []

        if output_path:
            summary = {
                repo_id: {
                    "total": len(records),
                    "total_size_bytes": sum(r.get("size_bytes") or 0 for r in records),
                    "total_size": _format_bytes(sum(r.get("size_bytes") or 0 for r in records)),
                    "records": records,
                }
                for repo_id, records in all_results.items()
            }
            _save_json(summary, output_path)

        return all_results

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_scanner(self, config: RepoConfig):
        if config.type == "ftp":
            return FTPScanner(
                host=config.host,
                user=config.user,
                password=config.password,
                port=config.port,
                repo_id=config.repo_id,
            )
        if config.type == "http":
            base_url = config.base_url or f"http://{config.host}"
            return ApacheHTTPScanner(
                base_url=base_url,
                repo_id=config.repo_id,
                verify_ssl=config.verify_ssl,
            )
        raise ValueError(f"Unknown repo type '{config.type}'. Use 'ftp' or 'http'.")

    def _process_entry(self, entry: FileEntry, config: RepoConfig) -> Optional[Dict]:
        """Build a metadata record for one file and optionally publish it to InvenioRDM."""
        try:
            record = build_invenio_record(
                entry,
                community_id=config.community_id,
                publisher=config.publisher,
                language=config.language,
            )
            record_dict = json.loads(record.json(exclude_none=True))
            metadata = record_dict.get("metadata", {})

            result = {
                "source_url": entry.path,
                "title": metadata.get("title", ""),
                "resource_type": metadata.get("resource_type", {}).get("id", ""),
                "folder_path": entry.folder_path,
                "size_bytes": entry.size,
                "size": _format_bytes(entry.size) if entry.size is not None else None,
                "record": record_dict,
            }

            if config.dry_run:
                result["invenio_id"] = None
                return result

            invenio_id = self.client.create_or_update_record(record=record_dict)
            if invenio_id:
                result["invenio_id"] = invenio_id
                return result

        except Exception as e:
            logger.error(f"Failed to process '{entry.path}': {e}")
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_bytes(n: Optional[int]) -> Optional[str]:
    """Return a human-readable size string (e.g. '1.2 MB')."""
    if n is None:
        return None
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _save_json(data: object, path: str) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Output saved to {path}")


def _load_repos_from_json(path: str, dry_run: bool = False) -> List[RepoConfig]:
    """Load repo configurations from a JSON file.

    Expected format::

        [
          {
            "repo_id": "biblioteca-central",
            "host": "ftp.example.cu",
            "type": "ftp",
            "community_id": "biblioteca-central",
            "user": "anonymous",
            "password": "",
            "start_path": "/",
            "publisher": "Universidad X",
            "language": "spa",
            "max_files": 100
          },
          {
            "repo_id": "repositorio-apache",
            "host": "repo.example.cu",
            "type": "http",
            "base_url": "http://repo.example.cu/files/",
            "community_id": "repositorio-apache"
          }
        ]
    """
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    configs = [RepoConfig(**item) for item in raw]
    if dry_run:
        for c in configs:
            c.dry_run = True
    return configs


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def setup_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Scan FTP/HTTP repositories and insert metadata records into InvenioRDM. "
            "Files are NOT downloaded — the remote URL is stored on each record. "
            "Results are saved to a JSON file."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- scan sub-command (single repo, ad-hoc) ---
    scan_cmd = subparsers.add_parser("scan", help="Scan a single repository")
    scan_cmd.add_argument("--host", required=True, help="FTP hostname or HTTP hostname")
    scan_cmd.add_argument("--type", choices=["ftp", "http"], default="ftp")
    scan_cmd.add_argument("--repo-id", default="", help="Repository identifier")
    scan_cmd.add_argument("--community-id", default=None)
    scan_cmd.add_argument("--start-path", default="/")
    scan_cmd.add_argument("--user", default="anonymous")
    scan_cmd.add_argument("--password", default="")
    scan_cmd.add_argument("--port", type=int, default=21)
    scan_cmd.add_argument("--base-url", default=None, help="Full base URL for HTTP repos")
    scan_cmd.add_argument("--publisher", default=None)
    scan_cmd.add_argument("--language", default="spa")
    scan_cmd.add_argument("--max-files", type=int, default=None)
    scan_cmd.add_argument(
        "--output",
        default=None,
        help="Path for the JSON output file (default: <repo-id>_<timestamp>.json)",
    )
    scan_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Build records and save to JSON without publishing to InvenioRDM",
    )
    scan_cmd.add_argument("--log-level", default="INFO")

    # --- batch sub-command (JSON config file) ---
    batch_cmd = subparsers.add_parser("batch", help="Run from a JSON config file")
    batch_cmd.add_argument("config_file", help="Path to JSON repos config file")
    batch_cmd.add_argument(
        "--output",
        default=None,
        help="Path for the JSON output file (default: batch_<timestamp>.json)",
    )
    batch_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Build records and save to JSON without publishing to InvenioRDM",
    )
    batch_cmd.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    setup_logging(args.log_level)

    collector = FTPCollector()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if args.command == "scan":
        repo_id = args.repo_id or args.host
        output_path = args.output or f"{repo_id}_{timestamp}.json"
        config = RepoConfig(
            repo_id=repo_id,
            host=args.host,
            type=args.type,
            community_id=args.community_id,
            start_path=args.start_path,
            user=args.user,
            password=args.password,
            port=args.port,
            base_url=args.base_url,
            publisher=args.publisher,
            language=args.language,
            max_files=args.max_files,
            dry_run=args.dry_run,
        )
        results = collector.collect_repo(config, output_path=output_path)
        action = "Prepared (dry-run)" if args.dry_run else "Inserted"
        print(f"\n{action} {len(results)} records. Output: {output_path}")

    elif args.command == "batch":
        output_path = args.output or f"batch_{timestamp}.json"
        repos = _load_repos_from_json(args.config_file, dry_run=args.dry_run)
        all_results = collector.collect_all(repos, output_path=output_path)
        total = sum(len(v) for v in all_results.values())
        action = "prepared (dry-run)" if args.dry_run else "inserted"
        print(f"\nTotal {action}: {total} records across {len(repos)} repos. Output: {output_path}")


if __name__ == "__main__":
    main()
