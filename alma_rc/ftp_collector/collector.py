"""Orchestrates scanning FTP/HTTP repositories and inserting records into InvenioRDM."""
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from alma_rc.ftp_collector.grobid import (
    GrobidExtractor,
    apply_grobid_metadata,
    is_processable as grobid_processable,
)
from alma_rc.ftp_collector.mapper import build_invenio_record
from alma_rc.ftp_collector.scanner import ApacheHTTPScanner, FileEntry, FTPScanner
from alma_rc.insert_data.invenio_actions import InvenioClient

logger = logging.getLogger(__name__)

_W = 68  # display width


# ---------------------------------------------------------------------------
# Display helpers  (write to stderr so they share the stream with the logger)
# ---------------------------------------------------------------------------

def _out(msg: str = "") -> None:
    print(msg, file=sys.stderr, flush=True)


def _print_repo_header(config: "RepoConfig") -> None:
    mode = "DRY-RUN" if config.dry_run else "LIVE"
    url = config.base_url or f"{config.type}://{config.host}"
    _out()
    _out("=" * _W)
    _out(f"  Repo   : {config.repo_id}")
    _out(f"  URL    : {url}{config.start_path.rstrip('/') or '/'}")
    _out(f"  Modo   : {mode}   Inicio: {datetime.now().strftime('%H:%M:%S')}")
    _out("=" * _W)


def _print_folder_change(folder: str) -> None:
    label = folder.replace("/", "  /  ") if folder else "(raiz)"
    _out()
    _out(f"  >> {label}")
    _out()


def _print_file_ok(count: int, result: Dict) -> None:
    rtype = (result.get("resource_type") or "other")[:15]
    size  = (result.get("size") or "?").rjust(10)
    title = result.get("title") or result.get("source_url", "")
    title = title[:38]
    tag   = "  [grobid]" if result.get("grobid_enriched") else ""
    _out(f"    #{count:04d}  {rtype:<16} {size}  {title}{tag}")


def _print_file_error(count: int, entry: FileEntry) -> None:
    _out(f"    #{count:04d}  {'ERROR':<16} {'?':>10}  {entry.name}  [fallo al procesar]")


def _print_repo_summary(
    config: "RepoConfig",
    scanned: int,
    results: List[Dict],
    errors: int,
    total_bytes: int,
    output_path: Optional[str],
) -> None:
    action = "Preparados (dry)" if config.dry_run else "Insertados"
    _out()
    _out("-" * _W)
    _out(f"  RESUMEN  {config.repo_id}")
    _out(f"  Escaneados  : {scanned}")
    _out(f"  {action:<12}: {len(results)}")
    if errors:
        _out(f"  Errores     : {errors}")
    _out(f"  Peso total  : {_format_bytes(total_bytes)}")
    if output_path:
        _out(f"  Guardado en : {output_path}")
    _out("-" * _W)
    _out()


def _print_batch_summary(
    all_results: Dict[str, List[Dict]],
    output_path: Optional[str],
    dry_run: bool,
) -> None:
    total_files  = sum(len(v) for v in all_results.values())
    total_bytes  = sum(
        sum(r.get("size_bytes") or 0 for r in v) for v in all_results.values()
    )
    action = "preparados (dry)" if dry_run else "insertados"
    _out("=" * _W)
    _out(f"  RESUMEN BATCH  —  {len(all_results)} repositorios")
    for repo_id, records in all_results.items():
        repo_bytes = sum(r.get("size_bytes") or 0 for r in records)
        _out(f"    {repo_id:<30} {len(records):>5} registros  {_format_bytes(repo_bytes):>10}")
    _out()
    _out(f"  Total {action}: {total_files}  —  {_format_bytes(total_bytes)}")
    if output_path:
        _out(f"  Guardado en : {output_path}")
    _out("=" * _W)
    _out()


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RepoConfig:
    """Configuration for a single FTP or HTTP repository."""

    repo_id: str
    host: str
    type: str                        # 'ftp' or 'http'
    community_id: Optional[str] = None
    start_path: str = "/"
    user: str = "anonymous"
    password: str = ""
    port: int = 21
    base_url: Optional[str] = None
    verify_ssl: bool = False
    publisher: Optional[str] = None
    language: str = "spa"
    max_files: Optional[int] = None
    dry_run: bool = False
    # Grobid metadata extraction for text documents
    use_grobid: bool = False
    grobid_url: str = "http://localhost:8070"


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------

class FTPCollector:
    """Scans FTP/HTTP repositories and inserts metadata records into InvenioRDM."""

    def __init__(self, invenio_client: Optional[InvenioClient] = None):
        self.client = invenio_client or InvenioClient()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect_repo(
        self, config: RepoConfig, output_path: Optional[str] = None
    ) -> List[Dict]:
        """Scan one repository and insert all records."""
        _print_repo_header(config)

        grobid = self._init_grobid(config)
        scanner = self._build_scanner(config)
        results: List[Dict] = []
        errors = 0
        count = 0
        current_folder: Optional[str] = None

        for entry in scanner.scan(config.start_path):
            if config.max_files is not None and count >= config.max_files:
                logger.warning(f"Limite max_files={config.max_files} alcanzado")
                break

            if entry.folder_path != current_folder:
                current_folder = entry.folder_path
                _print_folder_change(current_folder)

            count += 1
            result = self._process_entry(entry, config, grobid)
            if result:
                results.append(result)
                _print_file_ok(count, result)
            else:
                errors += 1
                _print_file_error(count, entry)

        total_bytes = sum(r.get("size_bytes") or 0 for r in results)
        _print_repo_summary(config, count, results, errors, total_bytes, output_path)

        if output_path:
            _save_json(
                {
                    "repo_id": config.repo_id,
                    "total": len(results),
                    "errors": errors,
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
        """Collect from multiple repositories sequentially."""
        all_results: Dict[str, List[Dict]] = {}
        for config in repos:
            try:
                all_results[config.repo_id] = self.collect_repo(config)
            except Exception as e:
                logger.error(f"Fallo en repo '{config.repo_id}': {e}")
                all_results[config.repo_id] = []

        _print_batch_summary(all_results, output_path, dry_run=any(r.dry_run for r in repos))

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

    def _init_grobid(self, config: RepoConfig) -> Optional[GrobidExtractor]:
        """Return a ready GrobidExtractor, or None if disabled / unavailable."""
        if not config.use_grobid:
            return None
        extractor = GrobidExtractor(base_url=config.grobid_url)
        if extractor.is_available():
            logger.debug(f"Grobid disponible en {config.grobid_url}")
            return extractor
        logger.warning(
            f"Grobid activado pero no disponible en {config.grobid_url}. "
            "Inicia el servicio con: podman-compose up -d grobid"
        )
        return None

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
        raise ValueError(f"Tipo desconocido '{config.type}'. Usa 'ftp' o 'http'.")

    def _process_entry(
        self,
        entry: FileEntry,
        config: RepoConfig,
        grobid: Optional[GrobidExtractor] = None,
    ) -> Optional[Dict]:
        """Build a metadata record for one file and optionally publish to InvenioRDM."""
        try:
            record = build_invenio_record(
                entry,
                community_id=config.community_id,
                publisher=config.publisher,
                language=config.language,
            )
            record_dict = json.loads(record.json(exclude_none=True))

            # Grobid enrichment for supported document types
            grobid_enriched = False
            if grobid and grobid_processable(entry.name):
                grobid_meta = grobid.extract_from_url(
                    entry.path, verify_ssl=config.verify_ssl
                )
                if grobid_meta:
                    apply_grobid_metadata(record_dict, grobid_meta)
                    grobid_enriched = True

            metadata = record_dict.get("metadata", {})
            result = {
                "source_url": entry.path,
                "title": metadata.get("title", ""),
                "resource_type": metadata.get("resource_type", {}).get("id", ""),
                "folder_path": entry.folder_path,
                "size_bytes": entry.size,
                "size": _format_bytes(entry.size) if entry.size is not None else None,
                "grobid_enriched": grobid_enriched,
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
            logger.error(f"Error procesando '{entry.name}': {e}")
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_bytes(n: Optional[int]) -> Optional[str]:
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
    logger.debug(f"JSON guardado en {path}")


def _load_repos_from_json(path: str, dry_run: bool = False) -> List[RepoConfig]:
    """Load repo configurations from a JSON file."""
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
    """Configure logging with a compact format.

    Display output (progress, summaries) is handled separately via _out()
    and goes to stderr as plain text.  The logger is reserved for warnings,
    errors, and optional debug messages.
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(fmt="%(asctime)s  %(levelname)-7s  %(message)s",
                          datefmt="%H:%M:%S")
    )
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    root.handlers.clear()
    root.addHandler(handler)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Escanea repositorios FTP/HTTP e inserta los metadatos en InvenioRDM. "
            "Los ficheros NO se descargan — la URL se guarda como identificador. "
            "Los resultados se guardan en un fichero JSON."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- scan ---
    scan_cmd = subparsers.add_parser("scan", help="Escanear un repositorio")
    scan_cmd.add_argument("--host", required=True)
    scan_cmd.add_argument("--type", choices=["ftp", "http"], default="ftp")
    scan_cmd.add_argument("--repo-id", default="")
    scan_cmd.add_argument("--community-id", default=None)
    scan_cmd.add_argument("--start-path", default="/")
    scan_cmd.add_argument("--user", default="anonymous")
    scan_cmd.add_argument("--password", default="")
    scan_cmd.add_argument("--port", type=int, default=21)
    scan_cmd.add_argument("--base-url", default=None)
    scan_cmd.add_argument("--publisher", default=None)
    scan_cmd.add_argument("--language", default="spa")
    scan_cmd.add_argument("--max-files", type=int, default=None)
    scan_cmd.add_argument("--output", default=None)
    scan_cmd.add_argument("--dry-run", action="store_true")
    scan_cmd.add_argument(
        "--use-grobid", action="store_true",
        help="Extraer metadatos de documentos de texto via Grobid",
    )
    scan_cmd.add_argument(
        "--grobid-url", default="http://localhost:8070",
        help="URL base del servicio Grobid (default: http://localhost:8070)",
    )
    scan_cmd.add_argument("--log-level", default="WARNING")

    # --- batch ---
    batch_cmd = subparsers.add_parser("batch", help="Ejecutar desde fichero JSON")
    batch_cmd.add_argument("config_file")
    batch_cmd.add_argument("--output", default=None)
    batch_cmd.add_argument("--dry-run", action="store_true")
    batch_cmd.add_argument("--log-level", default="WARNING")

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
            use_grobid=args.use_grobid,
            grobid_url=args.grobid_url,
        )
        collector.collect_repo(config, output_path=output_path)

    elif args.command == "batch":
        output_path = args.output or f"batch_{timestamp}.json"
        repos = _load_repos_from_json(args.config_file, dry_run=args.dry_run)
        collector.collect_all(repos, output_path=output_path)


if __name__ == "__main__":
    main()
