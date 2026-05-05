import requests
from pathlib import Path
from typing import Optional

def call_grobid(pdf_path: Path, grobid_url: str, timeout: int = 60) -> Optional[str]:
    """
    Envía un PDF al servicio Grobid (endpoint processHeaderDocument)
    y retorna el XML de respuesta como string, o None si falla.
    """
    url = f"{grobid_url.rstrip('/')}/api/processHeaderDocument"
    headers = {"Accept": "application/xml"}
    files = {"input": (pdf_path.name, open(pdf_path, "rb"), "application/pdf")}
    data = {"consolidateHeader": 1}
    try:
        response = requests.post(url, headers=headers, files=files, data=data, timeout=timeout)
        response.raise_for_status()
        print(response.text)
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error al procesar {pdf_path.name}: {e}")
        return None
    finally:
        files["input"][1].close()