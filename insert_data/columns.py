"""Constants for DSpace CSV column names and mapping dictionaries."""

# DSpace CSV column names
DSPACE_CSV_COLUMNS = [
    "id",
    "collection",
    "dc.contributor.advisor",
    "dc.contributor.advisor[]",
    "dc.contributor.advisor[es_ES]",
    "dc.contributor.author",
    "dc.contributor.author[]",
    "dc.contributor.author[es_ES]",
    "dc.contributor.editor",
    "dc.contributor.other",
    "dc.contributor.other[]",
    "dc.coverage.spatial",
    "dc.coverage.spatial[]",
    "dc.coverage.spatial[es_ES]",
    "dc.creator",
    "dc.date",
    "dc.date.accessioned",
    "dc.date.available",
    "dc.date.issued",
    "dc.date.issued[]",
    "dc.description",
    "dc.description.abstract",
    "dc.description.abstract[]",
    "dc.description.abstract[es_ES]",
    "dc.description.provenance[en]",
    "dc.description.sponsorship[es_ES]",
    "dc.description[es_ES]",
    "dc.format",
    "dc.format[]",
    "dc.identifier.arxiv[es_ES]",
    "dc.identifier.citation",
    "dc.identifier.citation[]",
    "dc.identifier.citation[es_ES]",
    "dc.identifier.doi",
    "dc.identifier.doi[es_ES]",
    "dc.identifier.eissn[es_ES]",
    "dc.identifier.isbn",
    "dc.identifier.isbn[]",
    "dc.identifier.isbn[es_ES]",
    "dc.identifier.issn",
    "dc.identifier.issn[es_ES]",
    "dc.identifier.other",
    "dc.identifier.uri",
    "dc.identifier.uri[]",
    "dc.language"

# Resource ty,
    "dc.language.iso[es_ES]",
    "dc.language[]",
    "dc.publisher",
    "dc.publisher[]",
    "dc.publisher[es_ES]",
    "dc.relation",
    "dc.relation.ispartofseries",
    "dc.relation.ispartofseries[]",
    "dc.relation.uri",
    "dc.relation.uri[es_ES]",
    "dc.relation[]",
    "dc.rights.holder",
    "dc.rights.holder[es_ES]",
    "dc.rights.license",
    "dc.rights.license[es_ES]",
    "dc.rights.uri[es_ES]",
    "dc.rights[es_ES]",
    "dc.source",

# Resource ty
    "dc.source.author",
    "dc.source.conferencetitle",
    "dc.source.conferencetitle[es_ES]",
    "dc.source.editor",
    "dc.source.endpage",
    "dc.source.endpage[es_ES]",
    "dc.source.initialpage",
    "dc.source.initialpage[es_ES]",
    "dc.source.issue",
    "dc.source.issue[es_ES]",
    "dc.source.journal",
    "dc.source.journal[es_ES]",
    "dc.source.title[es_ES]",
    "dc.source.uri",
    "dc.source.volume",
    "dc.source.volume[es_ES]",
    "dc.source[]",
    "dc.subject",
    "dc.subject.other[es_ES]",
    "dc.subject[]",
    "dc.subject[es_ES]",
    "dc.title",
    "dc.title.alternative",
    "dc.title.alternative[]",
    "dc.title.alternative[es_ES]",
    "dc.title[]",
    "dc.title[es_ES]",
    "dc.type",
    "dc.type.group",
    "dc.type.group[es_ES]",
    "dc.type[]",
    "dc.type[es_ES]"
]

# Resource type mappings
RESOURCE_TYPE_MAP = {
    'Animation': 'video',
    'article': 'publication-article',
    'Article': 'publication-article',
    'Book': 'publication-book',
    'Book chapter': 'publication-section',
    'Conference Object': 'publication-conferencepaper',
    'Conferencia': 'publication-conferencepaper',
    'Learning Object': 'learningobject',
    'Other': 'other',
    'Otros': 'other',
    'ponencia': 'publication-conferencepaper',
    'Poster': 'poster',
    'Preprint': 'publication-preprint',
    'Presentation': 'presentation',
    'Technical Report': 'publication-report',
    'tesis': 'thesis',
    'Tesis': 'thesis',
    'Tesis de Doctorado': 'thesis-doctoral_thesis',
    'Thesis': 'thesis',
    'Working Paper': 'publication-workingpaper',
}

# Thesis collection mappings
THESIS_COLLECTION_MAP = {
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

# External program collections
EXTERNAL_PROGRAM_COLLECTIONS = {
    "col_DICT_1868": "Programas externos a la universidad",
    "col_DICT_519": "Programas Externos a la Universidad",
}

# University subject mappings
UPR_SUBJECT_MAP = {
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

# University entities
UPR_ENTITIES = {
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

# Language variants for detection
LANGUAGE_VARIANTS = {
    'spanish': ['español', 'espanol', 'castellano', 'castilian', 'es'],
    'english': ['inglés', 'ingles', 'english', 'en']
}

# Date field to type mapping
DATE_TYPE_MAP = {
    'dc.date.accessioned': 'accepted',
    'dc.date.available': 'available',
    'dc.date.issued': 'issued',
    'dc.date.copyright': 'copyrighted',
    'dc.date.created': 'created',
    'dc.date.modified': 'updated',
    'dc.date.submitted': 'submitted'
}

# Field separators
FIELD_SEPARATORS = {
    'multi_value': '||',
    'language': ';'
}

# HTTP Constants
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
MAX_FILENAME_LENGTH = 200

# API Configuration (consider moving to environment variables)
INVENIO_API_CONFIG = {
    'base_url': 'https://127.0.0.1:5000',
    'token': 'tkK2nd6u4jOPjk4KoUOcFLidxlg3IFnSHdM7C5xNrfiuSR9fXNYEheVUKNnt'
}