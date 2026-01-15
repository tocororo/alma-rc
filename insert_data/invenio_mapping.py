
import pycountry
from rapidfuzz import process, fuzz
import idutils
import idutils.detectors
import requests
import pandas as pd
from typing import List, Any
from invenio_record import *

from datetime import date, datetime
import re

def process_dspace_row(row: pd.Series) -> tuple:
    """
    Process a single row from the DSpace CSV and convert to Invenio record.
    
    Args:
        row: Pandas Series containing one row of DSpace data
    
    Returns:
        Tuple of (record_id, InveniordmRecordSchemaV600)
    """
    # Get the main identifier (handle or URI)
    record_id = row.get('id', '')
    handle = row.get('dc.identifier.uri', '')
    
    if pd.isna(record_id) and pd.isna(handle):
        print("Warning: No identifier found for record")
        record_id = f"record_{datetime.now().timestamp()}"
    
    # Create the base record
    record = InveniordmRecordSchemaV600(
        id=Identifier(__root__=str(record_id)),
        metadata=Metadata(),
        access=Access(record=Record.public, files=Files.public),
        files=FilesSimple(enabled=True)
    )
    
    # Process metadata fields
    process_metadata_fields(record, row)
    
    
    # Process custom fields based on collection or other criteria
    collection = row.get('collection', '')
    fix_custom_fields_dspace(record, collection, row)
    
    # Process identifiers
    process_identifiers(record, row)
    
    return record_id, handle, record

def process_metadata_fields(record: InveniordmRecordSchemaV600, row: pd.Series):
    """
    Process metadata fields from DSpace row and populate the Invenio record.
    """
    metadata = record.metadata
    
    # Title
    title = get_first_non_empty(row, [
        'dc.title[es_ES]', 'dc.title', 'dc.title[]', 
        'dc.title.alternative[es_ES]', 'dc.title.alternative', 'dc.title.alternative[]'
    ])
    if title:
        metadata.title = str(title).strip()
    
    # Creators (authors)
    creators = process_contributors(row, [
        'dc.contributor.author[es_ES]', 'dc.contributor.author', 'dc.contributor.author[]',
        'dc.creator'
    ], role_id=None)  # Primary authors don't need a specific role
    if creators:
        metadata.creators = creators
    
    # Contributors (advisors, editors, etc.)
    contributors = []
    
    # Advisors
    advisors = process_contributors(row, [
        'dc.contributor.advisor[es_ES]', 'dc.contributor.advisor', 'dc.contributor.advisor[]'
    ], role_id='supervisor')
    if advisors:
        contributors.extend(advisors)
    
    # Editors
    editors = process_contributors(row, [
        'dc.contributor.editor'
    ], role_id='editor')
    if editors:
        contributors.extend(editors)
    
    # Other contributors
    others = process_contributors(row, [
        'dc.contributor.other[es_ES]', 'dc.contributor.other', 'dc.contributor.other[]'
    ], role_id='other')
    if others:
        contributors.extend(others)
    
    if contributors:
        metadata.contributors = contributors
    
    # Subjects
    subjects = process_subjects(row, [
        'dc.subject[es_ES]', 'dc.subject', 'dc.subject[]',
        'dc.subject.other[es_ES]'
    ])
    if subjects:
        metadata.subjects = subjects
    
    # Description/Abstract
    description = get_first_non_empty(row, [
        'dc.description.abstract[es_ES]', 'dc.description.abstract', 'dc.description.abstract[]',
        'dc.description[es_ES]', 'dc.description'
    ])
    if description:
        metadata.description = str(description).strip()
    
    # Publisher
    publisher = get_first_non_empty(row, [
        'dc.publisher[es_ES]', 'dc.publisher', 'dc.publisher[]'
    ])
    if publisher:
        metadata.publisher = str(publisher).strip()
    else:
        metadata.publisher = 'Universidad de Pinar del Río "Hermanos Saíz Montes de Oca"'
    
    # Publication date
    pub_date = get_first_non_empty(row, [
        'dc.date.issued[es_ES]', 'dc.date.issued', 'dc.date.issued[]',
        'dc.date.available', 'dc.date.accessioned'
    ])
    if pub_date:
        # Try to parse and format the date
        try:
            # Extract year from various date formats
            year_match = re.search(r'(\d{4})', str(pub_date))
            if year_match:
                metadata.publication_date = year_match.group(0)
            else:
                metadata.publication_date = str(pub_date).strip()
        except:
            metadata.publication_date = str(pub_date).strip()
    else:
        metadata.publication_date = str(date.today().year)
    
    # Resource type
    resource_type = get_first_non_empty(row, [
        'dc.type[es_ES]', 'dc.type', 'dc.type[]',
        'dc.type.group[es_ES]', 'dc.type.group'
    ])
    collection = row.get('collection', '')
    if resource_type:
        metadata.resource_type = map_dspace_type_to_invenio(str(resource_type).strip(), collection)
    else:
        metadata.resource_type = ResourceType(id=Identifier(__root__='publication'))
    
    # Languages
    languages = process_languages(row, [
        'dc.language.iso[es_ES]', 'dc.language', 'dc.language[]'
    ])
    if languages:
        metadata.languages =  languages
    
    # Dates (additional dates beyond publication date)
    dates = process_dates(row)
    if dates:
        metadata.dates = dates
    
    # Coverage (spatial)
    locations = process_locations(row, [
        'dc.coverage.spatial[es_ES]', 'dc.coverage.spatial', 'dc.coverage.spatial[]'
    ])
    if locations:
        metadata.locations = locations
    
    # Rights
    rights = process_rights(row)
    if rights:
        metadata.rights = rights
    
    # Format
    formats = process_formats(row)
    if formats:
        metadata.formats = formats
    
    # Size
    sizes = process_sizes(row)
    if sizes:
        metadata.sizes = sizes
    
def process_contributors(row: pd.Series, field_names: List[str], role_id: str = None) -> List[Creator]:
    """
    Process contributor fields from DSpace row.
    
    Args:
        row: Pandas Series containing DSpace data
        field_names: List of field names to check
        role_id: Role ID for the contributors (e.g., 'supervisor', 'editor')
    
    Returns:
        List of Creator/Contributor objects
    """
    creators = []
    
    for field_name in field_names:
        if field_name in row and not pd.isna(row[field_name]):
            value = str(row[field_name]).strip()
            
            # Handle multiple values separated by semicolons or commas
            separator = '||' 
            names = [name.strip() for name in value.split(separator) if name.strip()]
            
            for name in names:
                family_name, given_name = get_names_from_str(name)
                
                # Try to find person in Invenio (similar to existing function)
                person_match = find_person_in_invenio_dspace(family_name, given_name)
                
                person_or_org = PersonOrOrg(
                    name=name,
                    type=NameType.personal,
                    family_name=family_name,
                    given_name=given_name
                )
                creator = Creator(
                    person_or_org=person_or_org
                )

                if person_match:
                    creator.person_or_org.name = person_match.get('name', name)
                    creator.person_or_org.identifiers = person_match.get('identifiers')
                    creator.affiliations= person_match.get('affiliations')
                
                if role_id:
                    creator.role = Role(id=Identifier(__root__=role_id))
                
                creators.append(creator)
    
    return creators if creators else None

def get_names_from_str(full_name: str) -> tuple:
    """
    Parse full name into family name and given name.
    Handles various name formats.
    """
    full_name = full_name.strip()
    
    # Try to split by comma (family name, given name format)
    if ',' in full_name:
        parts = [p.strip() for p in full_name.split(',', 1)]
        if len(parts) == 2:
            return parts[0], parts[1]
    
    # Try to split by space (given name family name format)
    parts = full_name.split()
    if len(parts) > 1:
        # Assume last part is family name
        return parts[-1], ' '.join(parts[:-1])
    
    # Default: treat as family name only
    return full_name, ''

def process_subjects(row: pd.Series, field_names: List[str]) -> Subjects:
    """
    Process subject fields from DSpace row.
    """
    subjects = []
    
    for field_name in field_names:
        if field_name in row and not pd.isna(row[field_name]):
            value = str(row[field_name]).strip()
            
            # Handle multiple subjects separated by ||
            subjects_list = [s.strip() for s in value.split('||') if s.strip()]
            
            for subject in subjects_list:
                subjects.append(Subject(subject=subject))
    
    return Subjects(__root__=subjects) if subjects else None

def process_languages(row: pd.Series, field_names: List[str]) -> List[Language]:
    """
    Process language fields from DSpace row.
    """
    languages = []
    
    for field_name in field_names:
        if field_name in row and not pd.isna(row[field_name]):
            value = str(row[field_name]).strip()
            # Handle multiple languages
            langs = [lang.strip() for lang in value.split(';') if lang.strip()]
            for lang in langs:
                lang_code = map_language_to_iso639_3(lang)
                if lang_code:  # Only add valid language codes
                    languages.append(Language(id=Identifier(__root__=lang_code)))
    
    return languages if languages else None

def map_language_to_iso639_3(language: str) -> str | None:
    """
    Map language names, codes, or variants to ISO 639-3 codes using pycountry.
    
    Args:
        language: Language name (e.g., "Spanish", "Español"), 
                  ISO 639-1 code (e.g., "es"), or 
                  ISO 639-2/3 code (e.g., "spa")
    
    Returns:
        ISO 639-3 code as string, or None if language cannot be identified
    """
    if not language or pd.isna(language):
        return None
    
    language = str(language).strip()
    if not language:
        return None
    
    # Try direct lookup using pycountry's flexible lookup
    try:
        lang_obj = pycountry.languages.lookup(language)
        return lang_obj.alpha_3
    except LookupError:
        pass
    
    # Handle common cases that pycountry might miss
    # (lookup() is usually sufficient, but this adds extra safety)
    language_lower = language.lower()
    
    # Special handling for Spanish variants that might not be recognized
    spanish_variants = ['español', 'espanol', 'castellano', 'castilian']
    if any(variant in language_lower for variant in spanish_variants):
        return 'spa'
    
    # Special handling for English variants
    english_variants = ['inglés', 'ingles', 'english']
    if any(variant in language_lower for variant in english_variants):
        return 'eng'
    
    # If we have a 2-character string, assume it's ISO 639-1
    if len(language) == 2:
        try:
            lang_obj = pycountry.languages.get(alpha_2=language)
            return lang_obj.alpha_3
        except (KeyError, AttributeError):
            pass
    
    # If we have a 3-character string, it might already be ISO 639-3
    if len(language) == 3:
        try:
            lang_obj = pycountry.languages.get(alpha_3=language)
            return lang_obj.alpha_3
        except (KeyError, AttributeError):
            # It might be a valid ISO 639-3 code that pycountry doesn't have
            # In this case, return as-is (but validate it's alphabetic)
            if language.isalpha():
                return language.lower()
    
    # Final fallback: try to extract language name from common patterns
    # Remove common prefixes/suffixes
    clean_lang = re.sub(r'^(language|idioma|lengua)[:\s]*', '', language_lower, flags=re.IGNORECASE)
    clean_lang = re.sub(r'\s*\(.*?\)$', '', clean_lang)  # Remove parenthetical notes
    
    if clean_lang:
        try:
            lang_obj = pycountry.languages.lookup(clean_lang)
            return lang_obj.alpha_3
        except LookupError:
            pass
    
    # Could not identify language
    return None

def process_dates(row: pd.Series) -> List[Date]:
    """
    Process various date fields from DSpace row.
    """
    dates = []
    date_map = {
        'dc.date.accessioned': 'accepted',
        'dc.date.available': 'available',
        'dc.date.issued': 'issued',
        'dc.date.copyright': 'copyrighted',
        'dc.date.created': 'created',
        'dc.date.modified': 'updated',
        'dc.date.submitted': 'submitted'
    }
    
    for dspace_field, date_type in date_map.items():
        if dspace_field in row and not pd.isna(row[dspace_field]):
            date_value = str(row[dspace_field]).strip()
            if date_value:
                dates.append(Date(
                    date=date_value,
                    type=DateType(id=Identifier(__root__=date_type))
                ))
    
    return dates if dates else None

def process_locations(row: pd.Series, field_names: List[str]) -> Locations:
    """
    Process spatial coverage fields from DSpace row.
    """
    features = []
    
    for field_name in field_names:
        if field_name in row and not pd.isna(row[field_name]):
            value = str(row[field_name]).strip()
            
            # Handle multiple locations
            locations = [loc.strip() for loc in value.split(';') if loc.strip()]
            
            for location in locations:
                feature = Feature(
                    place=location,
                    description=f"Location: {location}"
                )
                features.append(feature)
    
    return Locations(features=features) if features else None

def process_rights(row: pd.Series) -> List[Right]:
    """
    Process rights information from DSpace row.
    """
    rights = []
    
    # License
    license_fields = ['dc.rights.license[es_ES]', 'dc.rights.license', 'dc.rights[es_ES]']
    license_value = get_first_non_empty(row, license_fields)
    if license_value:
        license_value = str(license_value).strip()
        rights.append(Right(
            title={'es': license_value},
            description={'es': f'Licencia: {license_value}'}
        ))
    
    # Rights holder
    holder_fields = ['dc.rights.holder[es_ES]', 'dc.rights.holder']
    holder_value = get_first_non_empty(row, holder_fields)
    if holder_value:
        holder_value = str(holder_value).strip()
        rights.append(Right(
            title={'es': f'Derechos: {holder_value}'},
            description={'es': f'Titular de derechos: {holder_value}'}
        ))

    
    return rights if rights else None

def process_formats(row: pd.Series) -> List[str]:
    """
    Process format fields from DSpace row.
    """
    formats = []
    
    for field_name in ['dc.format[]', 'dc.format']:
        if field_name in row and not pd.isna(row[field_name]):
            value = str(row[field_name]).strip()
            if value:
                # Handle multiple formats
                format_list = [f.strip() for f in value.split(';') if f.strip()]
                formats.extend(format_list)
    
    return formats if formats else None

def process_sizes(row: pd.Series) -> List[str]:
    """
    Process size information from DSpace row.
    """
    # Look for page numbers in source fields
    size_fields = [
        'dc.source.initialpage[es_ES]', 'dc.source.endpage[es_ES]',
        'dc.source.initialpage', 'dc.source.endpage'
    ]
    
    start_page = get_first_non_empty(row, size_fields[:2])
    end_page = get_first_non_empty(row, size_fields[2:])
    return [start_page, end_page]

def map_dspace_type_to_invenio(dspace_type: str, collection: str) -> ResourceType:
    """
    Map DSpace resource types to Invenio resource types.
    """

    type_map = {
        'Animation':'video',
        'article':'publication-article',
        'Article':'publication-article',
        'Book':'publication-book',
        'Book chapter':'publication-section',
        'Conference Object':'publication-conferencepaper',
        'Conferencia':'publication-conferencepaper',
        'Learning Object':'learningobject',
        'Other':'other',
        'Otros':'other',
        'ponencia':'publication-conferencepaper',
        'Poster':'poster',
        'Preprint':'publication-preprint',
        'Presentation':'presentation',
        'Technical Report':'publication-report',
        'tesis':'thesis',
        'Tesis':'thesis',
        'Tesis de Doctorado':'thesis-doctoral_thesis',
        'Thesis':'thesis',
        'Working Paper':'publication-workingpaper',
    }
    thesis_map = {
        'DICT/1739': 'thesis-doctoral_thesis',
        'DICT/1870': 'thesis-doctoral_thesis',
        'DICT/516': 'thesis-doctoral_thesis',
        'DICT/28': 'thesis-doctoral_thesis',
        'DICT/1902': 'thesis-doctoral_thesis',
        'DICT/29': 'thesis-doctoral_thesis',
        'DICT/30': 'thesis-doctoral_thesis',
        'DICT/519': 'thesis-doctoral_thesis',
        'DICT/1959': 'thesis-master_thesis',
        'DICT/31': 'thesis-master_thesis',
        'DICT/501': 'thesis-master_thesis',
        'DICT/32': 'thesis-master_thesis',
        'DICT/499': 'thesis-master_thesis',
        'DICT/1901': 'thesis-master_thesis',
        'DICT/33': 'thesis-master_thesis',
        'DICT/1961': 'thesis-master_thesis',
        'DICT/34': 'thesis-master_thesis',
        'DICT/515': 'thesis-master_thesis',
        'DICT/4036': 'thesis-master_thesis',
        'DICT/502': 'thesis-master_thesis',
        'DICT/35': 'thesis-master_thesis',
        'DICT/36': 'thesis-master_thesis',
        'DICT/1960': 'thesis-master_thesis',
        'DICT/37': 'thesis-master_thesis',
        'DICT/503': 'thesis-master_thesis',
        'DICT/1868': 'thesis-master_thesis',
        'DICT/38': 'thesis-master_thesis',
    }

    dspace_type_lower = dspace_type.lower().strip()
    invenio_type = type_map.get(dspace_type_lower, 'publication')
    if collection in thesis_map:
        invenio_type = thesis_map.get(collection, 'thesis')
    
    return ResourceType(id=Identifier(__root__=invenio_type))

def process_identifiers(record: InveniordmRecordSchemaV600, row: pd.Series):
    """
    Process identifier fields from DSpace row.
    """
    identifiers = []
    pids = {}
    
    # Handle URI/Handle
    uri_fields = ['dc.identifier.uri[]', 'dc.identifier.uri']
    uri_value = get_first_non_empty(row, uri_fields)
    if uri_value:
        uri_value = str(uri_value).strip()
        identifiers.append(IdentifiersWithScheme(identifier=Identifier(__root__=uri_value), scheme=Scheme(__root__='handle')))
    
    # DOI
    doi_fields = ['dc.identifier.doi[es_ES]', 'dc.identifier.doi']
    doi_value = get_first_non_empty(row, doi_fields)
    if doi_value:
        doi_value = str(doi_value).strip()
        # identifiers.append(IdentifiersWithScheme(identifier=Identifier(__root__=uri_value), scheme=Scheme(__root__='doi')))
        pids['doi'] = ExternalPid(
            identifier=Identifier(__root__=doi_value),
        )

    # ISBN
    isbn_fields = ['dc.identifier.isbn[es_ES]', 'dc.identifier.isbn', 'dc.identifier.isbn[]']
    isbn_value = get_first_non_empty(row, isbn_fields)
    if isbn_value:
        isbn_value = str(isbn_value).strip()
        identifiers.append(IdentifiersWithScheme(
            identifier=Identifier(__root__=isbn_value),
            scheme=Scheme(__root__='isbn')
        ))
    
    # ISSN
    # TODO: los datos de la revista?
    issn_fields = ['dc.identifier.issn[es_ES]', 'dc.identifier.issn']
    issn_value = get_first_non_empty(row, issn_fields)
    if issn_value:
        issn_value = str(issn_value).strip()
        identifiers.append(IdentifiersWithScheme(
            identifier=Identifier(__root__=issn_value),
            scheme=Scheme(__root__='issn')
        ))

     # arxiv
    arxiv_fields = ['dc.identifier.arxiv[es_ES]', 'dc.identifier.arxiv']
    arxiv_value = get_first_non_empty(row, arxiv_fields)
    if arxiv_value:
        arxiv_value = str(arxiv_value).strip()
        # identifiers.append(IdentifiersWithScheme(
        #     identifier=Identifier(__root__=arxiv_value),
        #     scheme=Scheme(__root__='arxiv')
        # ))  
        pids['arxiv'] = ExternalPid(
            identifier=Identifier(__root__=arxiv_value),
        )

    # Other identifiers
    other_fields = ['dc.identifier.other', 'dc.identifier.other[es_ES]']
    other_value = get_first_non_empty(row, other_fields)
    if other_value:
        other_value = str(other_value).strip()
        scheme = idutils.detectors.detect_identifier_schemes(other_value)
        if scheme:
            identifiers.append(IdentifiersWithScheme(
                identifier=Identifier(__root__=other_value),
                scheme=Scheme(__root__=scheme[0])
            ))
    
    if identifiers:
        record.metadata.identifiers = identifiers
    if pids:
        record.pids = pids

def fix_custom_fields_dspace(record: InveniordmRecordSchemaV600, collection: str, row: pd.Series):
    """
    Process custom fields based on DSpace collection and other metadata.
    Similar to the existing fix_custom_fields function but adapted for DSpace.
    """
    
    record.custom_fields = {}
    custom_entity = None
    
    # determinar el campo especial heredado upr:entidades

    # 1 - determinar si la entidad es externa en base a la coleccion de externos. 
    p_externos = {
        "col_DICT_1868": "Programas externos a la universidad",
        "col_DICT_519": "Programas Externos a la Universidad",
    }
    upr_collection = collection.split('/')[1]
    match_collection = f'col_DICT_{upr_collection}'
    if match_collection in p_externos:
        custom_entity = 'externo'

    # 2 - determinar si la entidad es de la upr en base al texto que aparece en el campo publisher
    if custom_entity is None:
        custom_entity = get_entity_upr_from_publisher(record.metadata.publisher)
        
    if custom_entity is not None:
        record.custom_fields.update({'upr:entidades': {'id': custom_entity}})


    # determinar el campo especial heredado upr:materias
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

    custom_subjects = []
    upr_collection = collection.split('/')[1]
    match_collection = f'col_DICT_{upr_collection}'
    if match_collection in materias_upr:
        custom_subjects.append({'id': match_collection})
    
    record.custom_fields.update({'upr:materias': custom_subjects})
    
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
    result = process.extractOne(search_text, titles, 
                               scorer=fuzz.WRatio,  # Weighted Ratio (best overall)
                               score_cutoff=75)
    
    if result:
        matched_title, score, index = result
        for entity_id, title in upr_entities.items():
            if title == matched_title:
                return entity_id
    
    return None

def get_first_non_empty(row: pd.Series, field_names: List[str]) -> Any:
    """
    Get the first non-empty value from a list of field names in a row.
    """
    for field_name in field_names:
        if field_name in row and not pd.isna(row[field_name]) and str(row[field_name]).strip():
            return row[field_name]
    return None

def find_person_in_invenio_dspace(family_name: str, given_name: str) -> dict | None:
    """
    Placeholder for person lookup - similar to existing function but for DSpace context.
    In a real implementation, this would call the Invenio API.
    """
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
