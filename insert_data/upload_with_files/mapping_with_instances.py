from lxml import etree
from mapping_classes import *

def xml_to_invenio_record(xml_path: str) -> InveniordmRecordSchemaV600:
    ns = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
    }

    tree = etree.parse(xml_path)
    root = tree.getroot()

    # ID desde OAI
    oai_id = root.find(".//oai:identifier", namespaces=ns)
    record_id = Identifier(__root__=oai_id.text) if oai_id is not None else None

    # Title
    title_el = root.find(".//dc:title", namespaces=ns)
    title = title_el.text if title_el is not None else None

    # Creators
    creators = []
    for el in root.findall(".//dc:creator", namespaces=ns):
        creators.append(Creator(person_or_org=PersonOrOrg(name=el.text)))

    # Subjects
    subjects = []
    for el in root.findall(".//dc:subject", namespaces=ns):
        subjects.append(Subject(subject=el.text))
    subjects = Subjects(__root__=subjects) if subjects else None

    # Description
    desc_el = root.find(".//dc:description", namespaces=ns)
    description = desc_el.text if desc_el is not None else None

    # Dates
    dates = []
    for el in root.findall(".//dc:date", namespaces=ns):
        dates.append(Date(date=el.text))

    # Resource type
    type_el = root.find(".//dc:type", namespaces=ns)
    resource_type = (
        ResourceType(id=Identifier(__root__=type_el.text)) if type_el is not None else None
    )

    # Identifiers
    identifiers = []
    for el in root.findall(".//dc:identifier", namespaces=ns):
        identifiers.append(IdentifiersWithScheme(identifier=Identifier(__root__=el.text)))

    # Construcción del objeto final
    record = InveniordmRecordSchemaV600(
        metadata=Metadata(
            title=title,
            creators=creators or None,
            subjects=subjects,
            description=description,
            dates=dates or None,
            resource_type=resource_type,
            identifiers=identifiers or None,
        ),
        files=FilesSimple()
    )

    return record
