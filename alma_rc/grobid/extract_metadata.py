#!/usr/bin/env python3
"""
Extrae metadatos de PDFs usando Grobid.
Lee PDFs de ./input/, escribe resultados en ./output/results.json
Uso: python main.py --metadata title authors abstract
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

from tqdm import tqdm

from grobid_client import call_grobid
from tei_parser import parse_tei


def process_pdfs(input_dir: Path, output_dir: Path, grobid_url: str, fields: List[str]) -> List[Dict[str, Any]]:
    """
    Procesa todos los PDFs en input_dir, extrae los metadatos solicitados
    y guarda los resultados en output_dir/results.json.
    Retorna la lista de resultados (también se guarda en disco).
    """
    output_dir.mkdir(exist_ok=True)

    if not input_dir.exists():
        raise FileNotFoundError(f"La carpeta de entrada '{input_dir}' no existe.")

    pdf_files = list(input_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"No se encontraron archivos PDF en '{input_dir}'")
        return []

    results = []
    for pdf_path in tqdm(pdf_files, desc="Procesando PDFs"):
        xml_content = call_grobid(pdf_path, grobid_url)
        if xml_content is None:
            results.append({"file": pdf_path.name, "error": "Grobid request failed"})
            continue
        try:
            metadata = parse_tei(xml_content, fields)
            metadata["file"] = pdf_path.name
            results.append(metadata)
        except Exception as e:
            print(f"Error parseando XML para {pdf_path.name}: {e}")
            results.append({"file": pdf_path.name, "error": "Invalid XML response"})

    output_file = output_dir / "results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nResultados guardados en: {output_file}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Extraer metadatos de PDFs con Grobid (carpeta input/ -> output/results.json)")
    parser.add_argument("--metadata", nargs="+", required=True,
                        choices=["title", "authors", "abstract", "date", "journal",
                                 "volume", "issue", "pages", "doi"],
                        help="Lista de metadatos a extraer")
    parser.add_argument("--grobid-url", default="http://localhost:8070",
                        help="URL base del servicio Grobid (por defecto: http://localhost:8070)")
    args = parser.parse_args()

    input_dir = Path("input")
    output_dir = Path("output")

    try:
        process_pdfs(input_dir, output_dir, args.grobid_url, args.metadata)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()