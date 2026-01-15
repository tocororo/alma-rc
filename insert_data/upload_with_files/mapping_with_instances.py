import os
from reprlib import recursive_repr
import idutils
import idutils.detectors
from lxml import etree
from insert_data.invenio_record import *
import requests

from rapidfuzz import process, fuzz
import re

def load_invenio_from_folder(path:str) -> InveniordmRecordSchemaV600:
    rc_handle = ''
    record = None
    dc_path = os.path.join(path, 'metadata_oai_dc.xml')
    if os.path.exists(dc_path):
        rc_handle, record = xml_oai_dc_to_invenio_record(dc_path)

    
    mets_path = os.path.join(path, 'metadata_mets.xml')
    if os.path.exists(mets_path):
        extract_mods_dates(mets_path, record=record)
    
    # TODO: basado en los setSpec the oaipmh, asignar el vocabulario especial de materias de la upr... 
    # TODO: basado en el publisher asignar el vocabulario especial sobre facultades..  si esta en el setSpec de programas externo.. tambien..  


    return rc_handle, record

def extract_mods_dates(xml_path: str, record: InveniordmRecordSchemaV600):
    # Define the namespaces
    namespaces = {
        'mets': 'http://www.loc.gov/METS/',
        'mods': 'http://www.loc.gov/mods/v3'
    }

    # Parse the XML string
    tree = etree.parse(xml_path)
    root = tree.getroot()

    # Find the xmlData element which contains the MODS data using the METS namespace
    xml_data = root.xpath('.//mets:xmlData', namespaces=namespaces)
    if not xml_data:
        print("Could not find xmlData element containing MODS metadata.")
        return []
    # Assuming the first xmlData element is the correct one
    mods_root = xml_data[0]

    # Find all date-related elements within the MODS namespace using xpath
    # This looks for any mods element whose local name starts with 'date'
    date_elements = mods_root.xpath('.//mods:*[starts-with(local-name(), "date")]', namespaces=namespaces)
    
    dates_info = []
    date_map = {
        'dateAccessioned': 'accepted',
        'dateAvailable': 'available',
        'dateCaptured': 'collected',
        'copyrightDate': 'copyrighted',
        'dateCreated': 'created',
        'dateIssued': 'issued',
        'dateSubmitted': 'submitted',
        'dateModified': 'updated',
        'dateValid': 'valid',
        'dateWithdrawn': 'withdrawn',
        'dateOther': 'other'
    }
    dates = []
    pub_date = None
    for elem in date_elements:
        date_value = elem.text.strip() if elem.text else ''
        date_type = etree.QName(elem).localname

        
        
        dates.append(
            Date(
                date=date_value,
                type=DateType(id=Identifier(__root__=date_map.get(date_type) if date_type is not None and date_type in date_map else 'other') ) ))
        if date_type == 'dateIssued':
            pub_date = date_value

    record.metadata.dates = dates
    record.metadata.publication_date = pub_date


def get_entity_upr_from_publisher(publisher: str) -> str:
    upr_entities = {
        'fcyt': 'Facultad de Ciencias Técnicas',
        'fhum': 'Facultad de Humanidades',
        'fmedia': 'Facultad de Enseñanza Media',
        'fprimaria': 'Facultad de Educación General',
        'fdeport': 'Facultad de Deportes',
        'fforestal': 'Facultad de Ciencias Forestales',
        'fagronomia': 'Facultad de Agronomía',
        'externo': 'Programas externos a la universidad',
        'CEF': 'Centro de Estudios Forestales',
        'CECEPRI': 'Centro de Estudios de Ciencias de la Educación',
        'CEEDAR': 'Centro de Estudios del Entrenamiento Deportivo en el Alto Rendimiento',
        'CE_GESTA': 'Centro de Estudios de Dirección, Desarrollo Local, Turismo y Cooperativismo',
        'CEMARNA': 'Centro de Estudios de Medio Ambiente y Recurso Naturales'
    }

    publisher_lower = publisher.lower().strip()
    
    
    required_terms = ['universidad de pinar del río', 'upr', 'universidad de pinar del rio']
    if not any(term in publisher_lower for term in required_terms):
        return None
        

    clean_publisher = re.sub(r'universidad de pinar del río|upr', '', publisher_lower, flags=re.IGNORECASE).strip()
    search_text = clean_publisher or publisher_lower
    
    titles = list(upr_entities.values())
    print('-------------------')
    print(clean_publisher)
    print('-------------------')
    print(titles)
    print('-------------------')
    result = process.extractOne(search_text, titles, 
                               scorer=fuzz.WRatio,  # Weighted Ratio (best overall)
                               score_cutoff=75)
    
    if result:
        matched_title, score, index = result
        for entity_id, title in upr_entities.items():
            if title == matched_title:
                return entity_id
    
    return None


def fix_custom_fields(record: InveniordmRecordSchemaV600, set_spec_values):


    tipos_documentos = {
        "col_DICT_1599": "Artículos",
        "col_DICT_24": "Capitulos de Libros",
        "col_DICT_25": "Libros",
        "col_DICT_680": "Ponencias, Comunicaciones en congresos, Conferencias",
        "com_DICT_2": "Tesis Doctorales",
        "com_DICT_4": "Tesis de Maestría"
    }

    type_map = {
        'col_DICT_1599': 'publication-article',
        'col_DICT_24': 'publication-section',
        'col_DICT_25': 'publication-book',
        'col_DICT_680':'publication-conferencepaper',
        'com_DICT_2': 'thesis-doctoral_thesis',
        'com_DICT_4': 'thesis-master_thesis'
    }
    for spec in set_spec_values:
        if spec in type_map:
            record.metadata.resource_type =  ResourceType(id=Identifier(__root__= type_map.get(spec))) 
            
            
            
    materias_upr = {
        "col_DICT_511": "Aprovechamiento  Forestal",
        "col_DICT_1897": "Atletismo",
        "col_DICT_1898": "Beisbol",
        "col_DICT_1900": "Boxeo",
        "col_DICT_513": "Contabilidad",
        "col_DICT_508": "Cultivo  del Tabaco",
        "col_DICT_506": "Dirección  de Instituciones Educativas",
        "col_DICT_514": "Docencia Psicopedagógica",
        "col_DICT_507": "Gestión Hotelera",
        "col_DICT_512": "Fruticultura Tropical",
        "col_DICT_510": "Silvicultura",
        "col_DICT_509": "Producción  Agroindustrial de Arroz",
        "col_DICT_1899": "Tiro Deportivo",
        "col_DICT_505": "Trabajo Social",
        "col_DICT_1870": "Ciencias Económicas",
        "col_DICT_29": "Desarrollo Sostenible de Bosques Tropicales: Manejos Forestal y Turístico",
        "col_DICT_30": "Geología Regional y Exploración de Recursos Geológicos", 
        "col_DICT_1959": "Actividad Física Comunitaria",
        "col_DICT_31": "Administración de Empresas Agropecuarias",
        "col_DICT_501": "Agroecología",
        "col_DICT_28": "Ciencias Pedagógicas",   
        "col_DICT_33": "Desarrollo Social",      
        "col_DICT_1961": "Didáctica de la Educación Física",
        "col_DICT_34": "Dirección",
        "col_DICT_515": "Educación",
        "col_DICT_502": "Eficiencia Energética",
        "col_DICT_35": "Geología",
        "col_DICT_36": "Gestión Ambiental",
        "col_DICT_1960": "Metodología del Entrenamiento Deportivo",
        "col_DICT_37": "Nuevas Tecnologías para la Educación",
        "col_DICT_503": "Pedagogía Profesional",
        "col_DICT_38": "Sistemas de Telecomunicaciones",   
        "col_DICT_499": "Ciencias Forestales",
        "col_DICT_32": "Ciencias de la Educación",             
        "col_DICT_1902": "Cultura Física",         
    }
    repetidas = {
        "col_DICT_516": "col_DICT_499",
        "col_DICT_1739": "col_DICT_32",
        "col_DICT_1901": "col_DICT_1902",        
    }


    p_externos = {
        "col_DICT_1868": "Programas externos a la universidad",
        "col_DICT_519": "Programas Externos a la Universidad",
    }

    print('*****************************    ', set_spec_values)
    custom_materias = []
    custom_entity = None
    for spec in set_spec_values:
        if spec in materias_upr:
            custom_materias.append({'id': spec})
        elif spec in repetidas:
            custom_materias.append({'id': repetidas.get(spec)})
        elif spec in p_externos:
            custom_entity = spec
    
    record.custom_fields = {}
    if custom_entity is not None:
        custom_entity = get_entity_upr_from_publisher(record.metadata.publisher)
        print('*****************************    ', custom_entity)
    
    if custom_entity is not None:
        record.custom_fields.update({'upr:entidades': {'id': custom_entity}})
    if custom_materias != []:
        record.custom_fields.update({'upr:materias': custom_materias})

def find_person_in_invenio(family_name: str, given_name: str) -> dict | None:
    """
    Busca una persona en InvenioRDM por family_name y given_name.
    Devuelve el registro si hay un único match, en otro caso None.
    """
    NAMES_API= "https://127.0.0.1:5000/api/names"
    query = f'(family_name:"{family_name}")AND(given_name:"{given_name}")'

    response = requests.get(
        NAMES_API,
        params={"q": query},
        verify=False  # ⚠️ solo para localhost con https autofirmado
    )

    response.raise_for_status()
    data = response.json()
    hits = data.get("hits", {}).get("hits", [])

    if len(hits) == 1:
        return hits[0]
    return None


def xml_oai_dc_to_invenio_record(xml_path: str) -> InveniordmRecordSchemaV600:
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
    
    contributors = []
    for el in root.findall(".//dc:contributor", namespaces=ns):
        family_name, given_name = get_names_from_str(el.text)
        person_match = find_person_in_invenio(family_name, given_name)
        if person_match:
            contributors.append(
            Contributor(
                person_or_org=PersonOrOrg(
                    type=NameType.personal,
                    name=person_match["name"],
                    family_name=person_match.get("family_name"),
                    given_name=person_match.get("given_name"),
                    identifiers=person_match.get("identifiers")
                ),
                affiliations=person_match.get("affiliations")
                )
            )
            
        else:
            contributors.append(Contributor(person_or_org=PersonOrOrg(name=el.text, type=NameType.personal, family_name=family_name, given_name=given_name)))
        

    # Creators TODO...
    creators = []
    for el in root.findall(".//dc:creator", namespaces=ns):
        family_name, given_name = get_names_from_str(el.text)
        person_match = find_person_in_invenio(family_name, given_name)
        if person_match:
            creators.append(
            Creator(
                person_or_org=PersonOrOrg(
                    type=NameType.personal,
                    name=person_match["name"],
                    family_name=person_match.get("family_name"),
                    given_name=person_match.get("given_name"),
                    identifiers=person_match.get("identifiers")
                ),
                affiliations=person_match.get("affiliations")
                )
            )
            
        else:
            creators.append(Creator(person_or_org=PersonOrOrg(name=el.text, type=NameType.personal, family_name=family_name, given_name=given_name)))
        
    # TODO: resolver el problema de los roles, pero son mas... 
    

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
    # TODO: todos los tipos...
    type_el = root.find(".//dc:type", namespaces=ns)
    type_map = {
        'Article': 'publication-article',
        # 'Thesis': 'thesis',
        'Other': 'publication',
        'Presentation': 'presentation',
        'Book': 'publication-book'
    }

    resource_type = (
        ResourceType(id=Identifier(__root__= type_map.get(type_el.text) if type_el is not None and type_el.text in type_map else 'publication')) 
    )
    
    if root.find(".//dc:publisher", namespaces=ns):
        publisher = root.find(".//dc:publisher", namespaces=ns).text  
    else: 
        publisher = 'Universidad de Pinar del Río "Hermanos Saíz Montes de Oca"' 

    # Identifiers
    identifiers = []
    rc_handle = ''
    for el in root.findall(".//dc:identifier", namespaces=ns):
        scheme = idutils.detectors.detect_identifier_schemes(el.text)
        if 'https://rc.upr.edu.cu/jspui/handle/DICT' in el.text:
            rc_handle = el.text
            scheme = ['handle']
        if 'issn' in scheme:
            # TODO: poner en el formato de invenio los datos de la revista en particular... 
            # esto puede implicar usar fuentes externas... 
            print("Tratar los datos de la revista.... ")
        if len(scheme) > 0:
            identifiers.append(IdentifiersWithScheme(identifier=Identifier(__root__=el.text), scheme=Scheme(__root__=scheme[0])))


    metadata = Metadata(
            title=title,
            creators=creators or None,
            contributors=contributors or None,
            subjects=subjects,
            description=description,
            dates=dates or None,
            publication_date=str(date.today()),
            resource_type=resource_type,
            identifiers=identifiers or None,
            publisher=publisher,
        )
    # Construcción del objeto final
    record = InveniordmRecordSchemaV600(
        metadata=metadata,
        access=Access(record=Record.public, files=Files.public),
        # files=FilesSimple()
    )

    # Find all setSpec elements
    set_spec_elements = root.xpath('//oai:setSpec', namespaces=ns)
    
    # Extract the text content of each setSpec element
    set_spec_values = [elem.text for elem in set_spec_elements if elem.text]

    fix_custom_fields(record, set_spec_values)



    return rc_handle, record


def get_names_from_str(full_name: str) :
    
    parts = [p.strip() for p in full_name.split(",")]
    if len(parts) == 2:
        family_name, given_name = parts
        
    return family_name, given_name