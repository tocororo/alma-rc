import os
import logging
import time  # <-- Importamos la librería time
from sickle import Sickle
from datetime import datetime

# Configuración
OAI_ENDPOINT = "https://rc.upr.edu.cu/oai/request"  # Reemplaza con tu URL
OUTPUT_DIR = "metadatos_dspace"
LOG_FILE = "harvest.log"
DELAY_SECONDS = 3  # Espera 3 segundos entre documentos

# Configurar logging (terminal + archivo)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def harvest_all_metadata():
    sickle = Sickle(OAI_ENDPOINT)
    formats = [fmt.metadataPrefix for fmt in sickle.ListMetadataFormats()]
    logger.info(f"Formatos detectados: {formats}")

    # Estadísticas
    total_docs = 0
    failed_docs = set()  # IDs de documentos con errores en TODOS los formatos
    partial_failures = {}  # {doc_id: [formatos_fallidos]}

    for record in sickle.ListIdentifiers(metadataPrefix='oai_dc'):
        doc_id = record.identifier.split(":")[-1]
        total_docs += 1
        doc_dir = os.path.join(OUTPUT_DIR, doc_id)
        os.makedirs(doc_dir, exist_ok=True)

        logger.info(f"\nProcesando documento: {doc_id}")
        failed_formats = []

        for fmt in formats:
            try:
                metadata = sickle.GetRecord(identifier=record.identifier, metadataPrefix=fmt)
                with open(os.path.join(doc_dir, f"metadata_{fmt}.xml"), "w", encoding="utf-8") as f:
                    f.write(metadata.raw)
                logger.info(f"  - {fmt}: OK")
            except Exception as e:
                logger.error(f"  - {fmt}: Error ({str(e)})")
                failed_formats.append(fmt)

        if total_docs % 10 == 0:  # Cada 10 documentos
            logger.warning(f"Pausa de {DELAY_SECONDS} segundos...")
            time.sleep(DELAY_SECONDS)

        # Actualizar estadísticas
        if len(failed_formats) == len(formats):
            failed_docs.add(doc_id)
        elif failed_formats:
            partial_failures[doc_id] = failed_formats

    # Generar reporte final
    with open("harvest_summary.txt", "w") as f:
        f.write(f"Resumen de extracción - {datetime.now()}\n")
        f.write("="*50 + "\n")
        f.write(f"Total de documentos procesados: {total_docs}\n")
        f.write(f"Documentos fallidos (ningún formato descargado): {len(failed_docs)}\n")
        f.write(f"IDs fallidos: {', '.join(failed_docs) if failed_docs else 'Ninguno'}\n")
        f.write("\nDocumentos con errores parciales:\n")
        for doc_id, formats in partial_failures.items():
            f.write(f"  - {doc_id}: Formatos fallidos: {', '.join(formats)}\n")

    logger.info("\n¡Proceso completado! Verifica los archivos:\n"
                f"- Log detallado: {LOG_FILE}\n"
                f"- Resumen estadístico: harvest_summary.txt")

if __name__ == "__main__":
    harvest_all_metadata()