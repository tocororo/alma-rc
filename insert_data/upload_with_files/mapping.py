import xml.etree.ElementTree as ET
import json
import sys

from mapping_classes import *


def getCreators():
    

def oai_dc_to_invenio(input_xml: str, output_json: str):
    # Namespaces de OAI-DC
    ns = {
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "dc": "http://purl.org/dc/elements/1.1/"
    }

    # Parsear el XML
    tree = ET.parse(input_xml)
    root = tree.getroot()

    # Extraer valores DC
    dc = {
        "title": [el.text for el in root.findall(".//dc:title", ns) if el.text],
        "creator": [el.text for el in root.findall(".//dc:creator", ns) if el.text],
        "subject": [el.text for el in root.findall(".//dc:subject", ns) if el.text],
        "description": [el.text for el in root.findall(".//dc:description", ns) if el.text],
        "publisher": [el.text for el in root.findall(".//dc:publisher", ns) if el.text],
        "contributor": [el.text for el in root.findall(".//dc:contributor", ns) if el.text],
        "date": [el.text for el in root.findall(".//dc:date", ns) if el.text],
        "type": [el.text for el in root.findall(".//dc:type", ns) if el.text],
        "format": [el.text for el in root.findall(".//dc:format", ns) if el.text],
        "identifier": [el.text for el in root.findall(".//dc:identifier", ns) if el.text],
        "source": [el.text for el in root.findall(".//dc:source", ns) if el.text],
        "language": [el.text for el in root.findall(".//dc:language", ns) if el.text],
        "relation": [el.text for el in root.findall(".//dc:relation", ns) if el.text],
        "coverage": [el.text for el in root.findall(".//dc:coverage", ns) if el.text],
        "rights": [el.text for el in root.findall(".//dc:rights", ns) if el.text],
    }
    access: Access =Access(embargo=Embargo())
    
    mapped  = InveniordmRecordSchemaV600(
        access=Access(files=Files.public, record=Record.public),
        metadata=Metadata(
            title=dc['title']if dc['title'] else 'N/A'
            creators=
        )                                 
                                         )

    # Construcción del JSON con el schema de InvenioRDM
    record = {
        "$schema": "local://records/record-v2.0.0.json",
        "id": "rec-001",
        "pid": {"pk": 1, "status": "R"},
        "pids": {
            "doi": {
                "identifier": "10.1234/example.doi",
                "provider": "datacite",
                "client": "datacite"
            }
        },
        "parent": {
            "id": "parent-001",
            "access": {"owned_by": {"user": 1}}
        },
        "access": {
            "record": "public",
            "files": "restricted",
            "embargo": {"active": False}
        },
        "metadata": {
            "resource_type": {"id": "article"},
            "title": dc["title"][0] if dc["title"] else "Título no disponible",
            "publication_date": dc["date"][-1] if dc["date"] else "N/A",
            "creators": [
                {"person_or_org": {"type": "personal", "name": c}}
                for c in dc["creator"]
            ],
            "subjects": [{"subject": s} for s in dc["subject"]],
            "description": dc["description"][0] if dc["description"] else None,
            "publisher": dc["publisher"][0] if dc["publisher"] else None,
            "contributors": [
                {"person_or_org": {"type": "personal", "name": c}}
                for c in dc["contributor"]
            ],
            "languages": [{"id": dc["language"][0]}] if dc["language"] else [],
            "rights": [{"title": {"en": r}} for r in dc["rights"]],
            "identifiers": [{"identifier": i, "scheme": "other"} for i in dc["identifier"]],
            "formats": dc["format"],
            "version": "v1.0.0"
        },
        "custom_fields": {},
        "files": {"enabled": False, "entries": {}},
        "tombstone": None,
        "created": dc["date"][0] if dc["date"] else "N/A",
        "updated": dc["date"][1] if len(dc["date"]) > 1 else dc["date"][0] if dc["date"] else "N/A"
    }

    # Guardar el JSON en archivo
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2, ensure_ascii=False)

    print(f"✅ Archivo convertido: {output_json}")


if __name__ == "__main__":
    # if len(sys.argv) != 3:
    #     print("Uso: python convert.py metadata_oai_dc.xml salida.json")
    # else:
        oai_dc_to_invenio('metadata_oai_dc.xml', 'newJson.json')