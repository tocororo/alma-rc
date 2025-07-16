import xml.etree.ElementTree as ET
from collections import defaultdict

def parse_oai_record(metadata_elements):
    """Parse all metadata formats and merge into a single dictionary"""
    result = defaultdict(list)
    
    for elem in metadata_elements:
        # Get the namespace and format from the element
        namespace = elem.tag.split('}')[0].lstrip('{')
        format_name = None
        
        # Determine which format we're parsing
        if 'uketd_dc' in namespace:
            format_name = 'uketd_dc'
            parse_uketd_dc(elem, result)
        elif 'dublincore' in namespace or 'purl.org/dc/terms' in namespace:
            format_name = 'qdc'
            parse_qdc(elem, result)
        elif 'mpeg21' in namespace:
            format_name = 'didl'
            parse_didl(elem, result)
        elif 'loc.gov/mods' in namespace:
            format_name = 'mods'
            parse_mods(elem, result)
        elif 'w3.org/2005/Atom' in namespace:
            format_name = 'ore'
            parse_ore(elem, result)
        elif 'loc.gov/METS' in namespace:
            format_name = 'mets'
            parse_mets(elem, result)
        elif 'openarchives.org/OAI/2.0/oai_dc' in namespace:
            format_name = 'oai_dc'
            parse_oai_dc(elem, result)
        elif 'openarchives.org/OAI/2.0/rdf' in namespace:
            format_name = 'rdf'
            parse_rdf(elem, result)
        elif 'loc.gov/MARC21/slim' in namespace:
            format_name = 'marc'
            parse_marc(elem, result)
        elif 'lyncode.com/xoai' in namespace:
            format_name = 'xoai'
            parse_xoai(elem, result)
        elif 'dspace.org/xmlns/dspace/dim' in namespace:
            format_name = 'dim'
            parse_dim(elem, result)
        elif 'ndltd.org/standards/metadata/etdms' in namespace:
            format_name = 'etdms'
            parse_etdms(elem, result)
    
    # Convert defaultdict to regular dict and consolidate fields
    return consolidate_fields(dict(result))

def parse_uketd_dc(elem, result):
    """Parse uketh_dc format"""
    ns = {'uketd': 'http://naca.central.cranfield.ac.uk/ethos-oai/2.0/'}
    
    # Map uketh_dc fields to common fields
    field_mapping = {
        'uketd:title': 'title',
        'uketd:creator': 'creator',
        'uketd:subject': 'subject',
        'uketd:abstract': 'description',
        'uketd:publisher': 'publisher',
        'uketd:date': 'date',
        'uketd:type': 'type',
        'uketd:identifier': 'identifier',
        'uketd:language': 'language',
        'uketd:rights': 'rights',
        'uketd:qualification_level': 'degree',
        'uketd:qualification_name': 'degree_name',
        'uketd:institution': 'institution'
    }
    
    for tag, field in field_mapping.items():
        for item in elem.findall(tag, ns):
            if item.text and item.text.strip():
                result[field].append(item.text.strip())

def parse_oai_dc(elem, result):
    """Parse standard oai_dc format"""
    ns = {'dc': 'http://purl.org/dc/elements/1.1/'}
    
    field_mapping = {
        'dc:title': 'title',
        'dc:creator': 'creator',
        'dc:subject': 'subject',
        'dc:description': 'description',
        'dc:publisher': 'publisher',
        'dc:date': 'date',
        'dc:type': 'type',
        'dc:identifier': 'identifier',
        'dc:language': 'language',
        'dc:rights': 'rights',
        'dc:contributor': 'contributor',
        'dc:format': 'format',
        'dc:source': 'source',
        'dc:relation': 'relation',
        'dc:coverage': 'coverage'
    }
    
    for tag, field in field_mapping.items():
        for item in elem.findall(tag, ns):
            if item.text and item.text.strip():
                result[field].append(item.text.strip())

def parse_qdc(elem, result):
    """Parse qualified dublin core"""
    ns = {'dcterms': 'http://purl.org/dc/terms/'}
    
    field_mapping = {
        'dcterms:title': 'title',
        'dcterms:creator': 'creator',
        'dcterms:subject': 'subject',
        'dcterms:abstract': 'description',
        'dcterms:publisher': 'publisher',
        'dcterms:issued': 'date',
        'dcterms:type': 'type',
        'dcterms:identifier': 'identifier',
        'dcterms:language': 'language',
        'dcterms:rights': 'rights',
        'dcterms:extent': 'format',
        'dcterms:degree': 'degree',
        'dcterms:institution': 'institution'
    }
    
    for tag, field in field_mapping.items():
        for item in elem.findall(tag, ns):
            if item.text and item.text.strip():
                result[field].append(item.text.strip())

def parse_mods(elem, result):
    """Parse MODS format"""
    ns = {'mods': 'http://www.loc.gov/mods/v3'}
    
    # Title
    for title_info in elem.findall('mods:titleInfo', ns):
        title = title_info.find('mods:title', ns)
        if title is not None and title.text and title.text.strip():
            result['title'].append(title.text.strip())
    
    # Creator/Author
    for name in elem.findall('mods:name', ns):
        name_part = name.find('mods:namePart', ns)
        if name_part is not None and name_part.text and name_part.text.strip():
            if name.get('type') == 'personal':
                result['creator'].append(name_part.text.strip())
            else:
                result['contributor'].append(name_part.text.strip())
    
    # Subjects
    for subject in elem.findall('mods:subject', ns):
        for topic in subject.findall('mods:topic', ns):
            if topic.text and topic.text.strip():
                result['subject'].append(topic.text.strip())
    
    # Other fields...
    # (Implement similar parsing for other MODS fields)

def consolidate_fields(data_dict):
    """Consolidate fields by removing duplicates and empty values"""
    consolidated = {}
    
    for field, values in data_dict.items():
        # Remove duplicates while preserving order
        seen = set()
        unique_values = []
        for value in values:
            if value not in seen:
                seen.add(value)
                unique_values.append(value)
        
        # If only one value, store as single value rather than list
        if len(unique_values) == 1:
            consolidated[field] = unique_values[0]
        elif len(unique_values) > 1:
            consolidated[field] = unique_values
    
    return consolidated

# Example usage:
# record = parse_oai_record(metadata_elements)