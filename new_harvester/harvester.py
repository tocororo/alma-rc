import os
import logging
import time  # <-- Importamos la librería time
from sickle import Sickle
from datetime import datetime
import requests
from lxml import html
import urllib.parse

def get_agent():
    return {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0'}

# Configuración
OAI_ENDPOINT = "https://rc.upr.edu.cu/oai/request"  # Reemplaza con tu URL
OUTPUT_DIR = "metadatos_dspace"
LOG_FILE = "harvest.log"
DELAY_SECONDS = 3  # Espera 3 segundos entre documentos
PREFIX_URL = "https://rc.upr.edu.cu/handle/DICT/"

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

def get_urls_download_dspace(url):
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    sess = requests.Session()
    sess.headers.update(get_agent())
    sess.verify = False
    timeout = 30
    cont = 0
    print(cont,"COUNTTTTT")
    
    dictionary = {}
    
    response = sess.get(url, timeout = timeout)
    doc1 = html.fromstring(response.text)
    element = doc1.xpath('//div[@class="panel panel-info"]//a')
    parsed_url = urllib.parse.urlparse(url)
    url_download = ''
    url_mod = url.replace("handle","bitstream")
    url_dom = parsed_url.scheme +'://'+parsed_url.netloc
    
    for e in element:
        if(e.get('href')):
            url_href = url_dom + e.get('href')
            if(url_mod in str(url_href) and url_download != url_href):
                print(e.get('href'), "references")
                
                url_download = url_href
                dictionary['download'+str(cont)] = url_download
                cont = cont+1
    return dictionary

def get_article_download_dspace(dictionary, save_dir):
    
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    timeout = 30
    ext = ''
    dir_open = save_dir + '/'
    cont=0
    os.makedirs(save_dir, exist_ok=True)
    for key in dictionary:
        response = requests.get(dictionary[key], verify = False, timeout = timeout)
        #print('-------------',dictionary[key])
        #print(urllib.parse.urlparse(dictionary[key]))
        #print(response.headers)
        allowed = ['application/pdf', 'text/html']
        if(response.text != ''):
            if('Content-Disposition' in response.headers):
                content_disposition = response.headers['Content-Disposition']
                indice_1 = content_disposition.index('"') #obtenemos la posición del primer carácter "
                indice_2 = content_disposition.rfind('"') #obtenemos la posición del ultimo carácter "
                filename = content_disposition[indice_1 + 1:indice_2]
            else:
                #url_part = urllib.parse.urlparse(dictionary[key]).path
                #url_filename = url_part.replace("%20"," ")
                url_filename = "'" + dictionary[key] + "'"
                print(url_filename)
                indice_1 = url_filename.rfind('/') #obtenemos la posición del primer carácter "
                indice_2 = url_filename.rfind("'") #obtenemos la posición del ultimo carácter "
                filename = url_filename[indice_1 + 1 : indice_2]

            export_file = open(dir_open + filename, 'wb')
            export_file.write(response.content)
            export_file.close()
        cont = cont+1
    return 'ok'

def get_record_files(url, save_dir):
    res = get_urls_download_dspace(url)
    get_article_download_dspace(res, save_dir)

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
        article_dir = os.path.join(doc_dir, "articles")
        os.makedirs(article_dir, exist_ok=True)

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
        
        get_record_files("https://rc.upr.edu.cu/handle/" + str(doc_id), article_dir)

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