# ftp_collector

Módulo para escanear repositorios FTP y Apache HTTP e insertar sus registros en InvenioRDM.

Los ficheros **no se descargan**. En su lugar, la URL de acceso al fichero se guarda como identificador en el registro de InvenioRDM. Los resultados de cada ejecución se guardan en un fichero JSON.

---

## Estructura del módulo

```
alma_rc/ftp_collector/
├── scanner.py     # Conectores FTP y Apache HTTP
├── mapper.py      # Mapeo archivo/carpeta → metadatos InvenioRDM
├── collector.py   # Orquestador y CLI
└── __init__.py
```

---

## Lógica de mapeo

Cada fichero encontrado en el repositorio se convierte en un registro de InvenioRDM siguiendo estas reglas:

| Campo InvenioRDM | Origen |
|---|---|
| `metadata.title` | Nombre del fichero sin extensión (guiones y barras bajas → espacios) |
| `metadata.resource_type` | Nombre de carpeta (heurística) o extensión del fichero |
| `metadata.subjects` | Cada segmento de la ruta de carpetas padre |
| `metadata.identifiers` | URL completa de descarga (scheme `url`) |
| `metadata.publisher` | Parámetro de configuración del repo |
| `metadata.languages` | Parámetro de configuración del repo (por defecto `spa`) |
| `files.enabled` | Siempre `false` — sin subida de binarios |

### Tipos de recurso por carpeta (prioridad sobre extensión)

Si algún segmento de la ruta coincide con una de estas palabras clave (insensible a mayúsculas y acentos), se asigna el tipo correspondiente:

| Carpeta | Tipo |
|---|---|
| `Libros`, `Books` | `publication-book` |
| `Tesis`, `Tesinas`, `Thesis` | `publication-thesis` |
| `Articulos`, `Articles` | `publication-article` |
| `Revistas`, `Journals` | `publication-journal` |
| `Presentaciones`, `Presentations` | `presentation` |
| `Informes`, `Reports` | `publication-report` |
| `Conferencias`, `Actas` | `publication-conferencepaper` |
| `Videos` | `video` |
| `Imagenes`, `Fotos` | `image` |
| `Software`, `Programas` | `software` |
| `Datos`, `Data`, `Datasets` | `dataset` |

### Tipos de recurso por extensión (fallback)

| Extensiones | Tipo |
|---|---|
| `.pdf` `.doc` `.docx` `.odt` `.txt` | `publication` |
| `.ppt` `.pptx` `.odp` | `presentation` |
| `.xls` `.xlsx` `.csv` `.ods` | `dataset` |
| `.jpg` `.png` `.gif` `.tif` `.svg` | `image` |
| `.mp4` `.avi` `.mov` `.mkv` | `video` |
| `.mp3` `.wav` `.ogg` `.flac` | `audio` |
| `.zip` `.rar` `.tar` `.gz` `.py` | `software` |
| (cualquier otra) | `other` |

---

## Uso por línea de comandos

El módulo registra el comando `alma-rc-ftp` al instalar el paquete.

### Escanear un único repositorio

```bash
alma-rc-ftp scan \
  --host ftp.ejemplo.cu \
  --type ftp \
  --repo-id biblioteca-central \
  --community-id biblioteca-central \
  --publisher "Universidad de Pinar del Río" \
  --language spa \
  --output resultados.json
```

Para un repositorio Apache HTTP:

```bash
alma-rc-ftp scan \
  --host repo.ejemplo.cu \
  --type http \
  --base-url http://repo.ejemplo.cu/archivos/ \
  --repo-id repositorio-web \
  --max-files 50 \
  --output repositorio-web.json
```

Si no se indica `--output`, el fichero se crea automáticamente con el nombre `<repo-id>_<timestamp>.json`.

#### Parámetros de `scan`

| Parámetro | Descripción | Por defecto |
|---|---|---|
| `--host` | Hostname del servidor | *requerido* |
| `--type` | Tipo: `ftp` o `http` | `ftp` |
| `--repo-id` | Identificador del repositorio | valor de `--host` |
| `--community-id` | ID de la comunidad destino en InvenioRDM | — |
| `--start-path` | Directorio raíz desde el que escanear | `/` |
| `--user` | Usuario FTP | `anonymous` |
| `--password` | Contraseña FTP | `` |
| `--port` | Puerto FTP | `21` |
| `--base-url` | URL base completa (modo `http`) | `http://<host>` |
| `--publisher` | Editor por defecto para todos los registros | — |
| `--language` | Idioma ISO 639-3 | `spa` |
| `--max-files` | Límite de ficheros a procesar | sin límite |
| `--output` | Ruta del fichero JSON de salida | auto |
| `--log-level` | Nivel de log (`DEBUG`, `INFO`, …) | `INFO` |

---

### Procesar varios repositorios (modo batch)

```bash
alma-rc-ftp batch repos.json --output todos_los_resultados.json
```

El fichero `repos.json` contiene un array con la configuración de cada repositorio:

```json
[
  {
    "repo_id": "biblioteca-central",
    "host": "ftp.ejemplo.cu",
    "type": "ftp",
    "community_id": "biblioteca-central",
    "user": "anonymous",
    "password": "",
    "start_path": "/",
    "publisher": "Universidad de Pinar del Río",
    "language": "spa",
    "max_files": 500
  },
  {
    "repo_id": "repositorio-apache",
    "host": "repo.ejemplo.cu",
    "type": "http",
    "base_url": "http://repo.ejemplo.cu/archivos/",
    "community_id": "repositorio-apache",
    "language": "spa"
  }
]
```

Todos los campos son los mismos que los parámetros del comando `scan`. Solo `repo_id`, `host` y `type` son obligatorios.

---

## Formato del JSON de salida

### Salida de `scan` (un repositorio)

```json
{
  "repo_id": "biblioteca-central",
  "total": 3,
  "records": [
    {
      "source_url": "ftp://ftp.ejemplo.cu/Libros/Python_avanzado.pdf",
      "invenio_id": "abc12-34def",
      "title": "Python avanzado",
      "resource_type": "publication-book",
      "folder_path": "Libros"
    },
    {
      "source_url": "ftp://ftp.ejemplo.cu/Presentaciones/Seminario_2024.pptx",
      "invenio_id": "xyz98-76wvu",
      "title": "Seminario 2024",
      "resource_type": "presentation",
      "folder_path": "Presentaciones"
    }
  ]
}
```

### Salida de `batch` (varios repositorios)

```json
{
  "biblioteca-central": {
    "total": 2,
    "records": [ ... ]
  },
  "repositorio-apache": {
    "total": 5,
    "records": [ ... ]
  }
}
```

---

## Uso como librería Python

```python
from alma_rc.ftp_collector import FTPCollector, RepoConfig

collector = FTPCollector()

# Un solo repositorio FTP
config = RepoConfig(
    repo_id="biblioteca-central",
    host="ftp.ejemplo.cu",
    type="ftp",
    community_id="biblioteca-central",
    publisher="Universidad de Pinar del Río",
    language="spa",
    max_files=100,
)
results = collector.collect_repo(config, output_path="salida.json")

# Varios repositorios
repos = [
    RepoConfig(repo_id="repo-ftp",  host="ftp.ejemplo.cu",  type="ftp"),
    RepoConfig(repo_id="repo-http", host="web.ejemplo.cu",  type="http",
               base_url="http://web.ejemplo.cu/docs/"),
]
all_results = collector.collect_all(repos, output_path="batch.json")
```

El método `collect_repo` devuelve una lista de dicts con las claves `source_url`, `invenio_id`, `title`, `resource_type` y `folder_path`.

---

## Variables de entorno requeridas

El colector usa el mismo cliente InvenioRDM que el resto del paquete. Se configuran en el fichero `.env` del proyecto:

```
INVENIO_API_BASE_URL=https://alma.upr.edu.cu
INVENIO_API_TOKEN=<tu-token>
```

---

## Nota sobre comunidades

InvenioRDM asocia registros a comunidades mediante una llamada API separada (`POST /api/communities/{id}/records`), que no está incluida en el cliente actual. El `community_id` se almacena provisionalmente en el campo `custom_fields.ftp_source_community` de cada registro para facilitar su asociación posterior.
