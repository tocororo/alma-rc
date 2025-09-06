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
        family_name, given_name = get_names_from_str(el.text)
        creators.append(Creator(person_or_org=PersonOrOrg(name=el.text, type=NameType.personal, family_name=family_name, given_name=given_name)))

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
        dates.append(Date(date=el.text,type=DateType(id=Identifier(__root__='other'), ) ))

    # Resource type
    type_el = root.find(".//dc:type", namespaces=ns)
    resource_type = (
        ResourceType(id=Identifier(__root__='dataset')) if type_el is not None else None
    )
    
    publisher = root.find(".//oai:identifier", namespaces=ns).text

    # Identifiers
    identifiers = []
    for el in root.findall(".//dc:identifier", namespaces=ns):
        identifiers.append(IdentifiersWithScheme(identifier=Identifier(__root__=el.text), scheme=Scheme(__root__='other')))

    # Construcción del objeto final
    record = InveniordmRecordSchemaV600(
        metadata=Metadata(
            title=title,
            creators=creators or None,
            subjects=subjects,
            description=description,
            dates=dates or None,
            publication_date=str(date.today()),
            resource_type=resource_type,
            identifiers=identifiers or None,
            publisher=publisher or "Unknown Publisher",
        ),
        access=Access(record=Record.public, files=Files.public),
        # files=FilesSimple()
    )

    return record


def get_names_from_str(full_name: str) :
    
    parts = [p.strip() for p in full_name.split(",")]
    if len(parts) == 2:
        family_name, given_name = parts
        
    return family_name, given_name