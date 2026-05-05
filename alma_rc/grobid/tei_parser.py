import xml.etree.ElementTree as ET
from typing import List, Dict, Any

TEI_NAMESPACE = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NAMESPACE}

def get_element_text(element) -> str:
    """Extrae todo el texto de un elemento y sus descendientes."""
    if element is None:
        return ""
    return " ".join(element.itertext()).strip()

def extract_authors(root) -> List[str]:
    authors = []
    for author in root.findall(".//tei:author", namespaces=NS):
        pers_name = author.find(".//tei:persName", namespaces=NS)
        if pers_name is not None:
            forename = pers_name.find("tei:forename", namespaces=NS)
            surname = pers_name.find("tei:surname", namespaces=NS)
            name_parts = []
            if forename is not None and forename.text:
                name_parts.append(forename.text.strip())
            if surname is not None and surname.text:
                name_parts.append(surname.text.strip())
            authors.append(" ".join(name_parts))
        elif author.text:
            authors.append(author.text.strip())
    return authors

def parse_tei(xml_str: str, fields: List[str]) -> Dict[str, Any]:
    """
    Parsea el XML TEI devuelto por Grobid y extrae los campos solicitados.
    """
    root = ET.fromstring(xml_str)
    result = {}
    if "title" in fields:
        title_el = root.find(".//tei:titleStmt/tei:title", namespaces=NS)
        result["title"] = get_element_text(title_el)
    if "authors" in fields:
        result["authors"] = extract_authors(root)
    if "abstract" in fields:
        abstract_el = root.find(".//tei:abstract", namespaces=NS)
        result["abstract"] = get_element_text(abstract_el)
    if "date" in fields:
        date_el = root.find(".//tei:publicationStmt/tei:date", namespaces=NS)
        if date_el is None:
            date_el = root.find(".//tei:biblStruct/tei:monogr/tei:date", namespaces=NS)
        result["date"] = get_element_text(date_el)
    if "journal" in fields:
        journal_el = root.find(".//tei:biblStruct/tei:monogr/tei:title", namespaces=NS)
        result["journal"] = get_element_text(journal_el)
    if "volume" in fields:
        vol_el = root.find(".//tei:biblStruct/tei:monogr/tei:imprint/tei:biblScope[@unit='volume']", namespaces=NS)
        result["volume"] = get_element_text(vol_el)
    if "issue" in fields:
        issue_el = root.find(".//tei:biblStruct/tei:monogr/tei:imprint/tei:biblScope[@unit='issue']", namespaces=NS)
        result["issue"] = get_element_text(issue_el)
    if "pages" in fields:
        pages_el = root.find(".//tei:biblStruct/tei:monogr/tei:imprint/tei:biblScope[@unit='page']", namespaces=NS)
        result["pages"] = get_element_text(pages_el)
    if "doi" in fields:
        doi_el = root.find(".//tei:idno[@type='DOI']", namespaces=NS)
        result["doi"] = get_element_text(doi_el)
    return result