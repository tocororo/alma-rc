"""Mapping functions for converting DSpace records to Invenio format."""
import dateparser

import logging
import re
from datetime import date, datetime
from typing import List, Any, Optional, Tuple, Dict
import pandas as pd
import pycountry
import idutils.detectors
from rapidfuzz import process, fuzz
import requests
from columns import *
from invenio_record import *
from columns import (
    RESOURCE_TYPE_MAP, THESIS_COLLECTION_MAP, EXTERNAL_PROGRAM_COLLECTIONS,
    UPR_SUBJECT_MAP, UPR_ENTITIES, LANGUAGE_VARIANTS, DATE_TYPE_MAP,
    FIELD_SEPARATORS
)

logger = logging.getLogger(__name__)


class DSpaceToInvenioMapper:
    """Mapper for converting DSpace records to Invenio format."""
    
    DEFAULT_PUBLISHER = 'Universidad de Pinar del Río "Hermanos Saíz Montes de Oca"'
    NAME_API_URL = "https://127.0.0.1:5000/api/names"
    
    @staticmethod
    def process_dspace_row(row: pd.Series) -> Tuple[Optional[str], Optional[str], 
                                                   Optional[InveniordmRecordSchemaV600]]:
        """Process a single row from DSpace CSV and convert to Invenio record.
        
        Args:
            row: Pandas Series containing one row of DSpace data
            
        Returns:
            Tuple of (record_id, handle, InveniordmRecordSchemaV600)
        """
        try:
            record_id = row.get('id', '')
            handle = row.get('dc.identifier.uri', '')
            
            if pd.isna(record_id) and pd.isna(handle):
                logger.warning("No identifier found for record")
                record_id = f"record_{datetime.now().timestamp()}"
            
            record = DSpaceToInvenioMapper._create_base_record(record_id)
            DSpaceToInvenioMapper._process_metadata(record, row)
            
            collection = row.get('collection', '')
            DSpaceToInvenioMapper._process_custom_fields(record, collection, row)
            DSpaceToInvenioMapper._process_identifiers(record, row)
            
            return str(record_id), str(handle) if not pd.isna(handle) else None, record
            
        except Exception as e:
            logger.error(f"Error processing DSpace row: {e}")
            return None, None, None
    
    @staticmethod
    def _create_base_record(record_id: str) -> InveniordmRecordSchemaV600:
        """Create base Invenio record structure."""
        return InveniordmRecordSchemaV600(
            id=Identifier(__root__=str(record_id)),
            metadata=Metadata(),
            access=Access(record=Record.public, files=Files.public),
            files=FilesSimple(enabled=True)
        )
    
    @staticmethod
    def _process_metadata(record: InveniordmRecordSchemaV600, row: pd.Series) -> None:
        """Process all metadata fields from DSpace row."""
        metadata = record.metadata
        
        # Process each metadata component
        metadata.title = DSpaceToInvenioMapper._get_title(row)
        metadata.creators = DSpaceToInvenioMapper._get_creators(row)
        metadata.contributors = DSpaceToInvenioMapper._get_contributors(row)
        metadata.subjects = DSpaceToInvenioMapper._get_subjects(row)
        metadata.description = DSpaceToInvenioMapper._get_description(row)
        metadata.publisher = DSpaceToInvenioMapper._get_publisher(row)
        metadata.publication_date = DSpaceToInvenioMapper._get_publication_date(row)
        metadata.resource_type = DSpaceToInvenioMapper._get_resource_type(row)
        metadata.languages = DSpaceToInvenioMapper._get_languages(row)
        metadata.dates = DSpaceToInvenioMapper._get_dates(row)
        metadata.locations = DSpaceToInvenioMapper._get_locations(row)
        metadata.rights = DSpaceToInvenioMapper._get_rights(row)
        metadata.formats = DSpaceToInvenioMapper._get_formats(row)
    
    @staticmethod
    def _get_first_non_empty(row: pd.Series, field_names: List[str]) -> Any:
        """Get the first non-empty value from a list of field names."""
        for field_name in field_names:
            if field_name in row and not pd.isna(row[field_name]) and str(row[field_name]).strip():
                return row[field_name]
        return None
    
    @staticmethod
    def _get_title(row: pd.Series) -> Optional[str]:
        """Extract title from DSpace row."""
        title_fields = [
            'dc.title[es_ES]', 'dc.title', 'dc.title[]', 
            'dc.title.alternative[es_ES]', 'dc.title.alternative', 'dc.title.alternative[]'
        ]
        title = DSpaceToInvenioMapper._get_first_non_empty(row, title_fields)
        return str(title).strip() if title else None
    
    @staticmethod
    def _get_creators(row: pd.Series) -> Optional[List[Creator]]:
        """Extract creators (authors) from DSpace row."""
        creator_fields = [
            'dc.contributor.author[es_ES]', 'dc.contributor.author', 'dc.contributor.author[]',
            'dc.creator'
        ]
        return DSpaceToInvenioMapper._process_contributors(row, creator_fields)
    
    @staticmethod
    def _get_contributors(row: pd.Series) -> Optional[List[Contributor]]:
        """Extract all contributors from DSpace row."""
        contributors = []
        
        # Advisors
        advisor_fields = ['dc.contributor.advisor[es_ES]', 'dc.contributor.advisor', 'dc.contributor.advisor[]']
        advisors = DSpaceToInvenioMapper._process_contributors(row, advisor_fields, 'supervisor')
        if advisors:
            contributors.extend(advisors)
        
        # Editors
        editor_fields = ['dc.contributor.editor']
        editors = DSpaceToInvenioMapper._process_contributors(row, editor_fields, 'editor')
        if editors:
            contributors.extend(editors)
        
        # Other contributors
        other_fields = ['dc.contributor.other[es_ES]', 'dc.contributor.other', 'dc.contributor.other[]']
        others = DSpaceToInvenioMapper._process_contributors(row, other_fields, 'other')
        if others:
            contributors.extend(others)
        
        return contributors if contributors else None
    
    @staticmethod
    def _process_contributors(row: pd.Series, field_names: List[str], 
                             role_id: Optional[str] = None) -> Optional[List[Creator]]:
        """Process contributor fields with optional role assignment."""
        creators = []
        
        for field_name in field_names:
            if field_name in row and not pd.isna(row[field_name]):
                value = str(row[field_name]).strip()
                names = [name.strip() for name in value.split(FIELD_SEPARATORS['multi_value']) 
                        if name.strip()]
                
                for name in names:
                    creator = DSpaceToInvenioMapper._create_creator_from_name(name, role_id)
                    if creator:
                        creators.append(creator)
        
        return creators if creators else None
    
    @staticmethod
    def _create_creator_from_name(full_name: str, role_id: Optional[str]) -> Optional[Creator]:
        """Create a Creator object from a full name string."""
        family_name, given_name = DSpaceToInvenioMapper._parse_name(full_name)
        person_match = DSpaceToInvenioMapper._find_person_in_invenio(family_name, given_name)
        
        person_or_org = PersonOrOrg(
            name=full_name,
            type=NameType.personal,
            family_name=family_name,
            given_name=given_name
        )
        
        creator = Creator(person_or_org=person_or_org)
        
        if person_match:
            creator.person_or_org.name = person_match.get('name', full_name)
            creator.person_or_org.identifiers = person_match.get('identifiers')
            creator.affiliations = person_match.get('affiliations')
        
        if role_id:
            creator.role = Role(id=Identifier(__root__=role_id))
        
        return creator
    
    @staticmethod
    def _parse_name(full_name: str) -> Tuple[str, str]:
        """Parse full name into family and given names."""
        full_name = full_name.strip()
        
        if ',' in full_name:
            parts = [p.strip() for p in full_name.split(',', 1)]
            if len(parts) == 2:
                return parts[0], parts[1]
        
        parts = full_name.split()
        if len(parts) > 1:
            return parts[-1], ' '.join(parts[:-1])
        
        return full_name, ''
    
    @staticmethod
    def _find_person_in_invenio(family_name: str, given_name: str) -> Optional[Dict]:
        """Search for person in InvenioRDM names API."""
        query = f'(family_name:"{family_name}")AND(given_name:"{given_name}")'
        
        try:
            response = requests.get(
                DSpaceToInvenioMapper.NAME_API_URL,
                params={"q": query},
                verify=False
            )
            response.raise_for_status()
            
            data = response.json()
            hits = data.get("hits", {}).get("hits", [])
            
            if len(hits) == 1:
                return hits[0]
        except Exception as e:
            logger.debug(f"Error searching for person {given_name} {family_name}: {e}")
        
        return None
    
    @staticmethod
    def _get_subjects(row: pd.Series) -> Optional[Subjects]:
        """Extract subjects from DSpace row."""
        subjects = []
        subject_fields = ['dc.subject[es_ES]', 'dc.subject', 'dc.subject[]', 'dc.subject.other[es_ES]']
        
        for field_name in subject_fields:
            if field_name in row and not pd.isna(row[field_name]):
                value = str(row[field_name]).strip()
                subjects_list = [s.strip() for s in value.split(FIELD_SEPARATORS['multi_value']) 
                               if s.strip()]
                
                for subject in subjects_list:
                    subjects.append(Subject(subject=subject))
        
        return Subjects(__root__=subjects) if subjects else None
    
    @staticmethod
    def _get_description(row: pd.Series) -> Optional[str]:
        """Extract description/abstract from DSpace row."""
        description_fields = [
            'dc.description.abstract[es_ES]', 'dc.description.abstract', 'dc.description.abstract[]',
            'dc.description[es_ES]', 'dc.description'
        ]
        description = DSpaceToInvenioMapper._get_first_non_empty(row, description_fields)
        return str(description).strip() if description else None
    
    @staticmethod
    def _get_publisher(row: pd.Series) -> str:
        """Extract publisher from DSpace row."""
        publisher_fields = ['dc.publisher[es_ES]', 'dc.publisher', 'dc.publisher[]']
        publisher = DSpaceToInvenioMapper._get_first_non_empty(row, publisher_fields)
        return str(publisher).strip() if publisher else DSpaceToInvenioMapper.DEFAULT_PUBLISHER
    
    @staticmethod
    def _get_publication_date(row: pd.Series) -> str:
        """Extract and format publication date from DSpace row."""
        date_fields = [
            'dc.date.issued[es_ES]', 'dc.date.issued', 'dc.date.issued[]',
            'dc.date.available', 'dc.date.accessioned'
        ]
        pub_date = DSpaceToInvenioMapper._get_first_non_empty(row, date_fields)
        
        if pub_date:
            try:
                year_match = re.search(r'(\d{4})', str(pub_date))
                return year_match.group(0) if year_match else str(pub_date).strip()
            except Exception:
                return str(pub_date).strip()
        
        return str(date.today().year)
    
    @staticmethod
    def _get_resource_type(row: pd.Series) -> ResourceType:
        """Map DSpace resource type to Invenio resource type."""
        type_fields = ['dc.type[es_ES]', 'dc.type', 'dc.type[]', 'dc.type.group[es_ES]', 'dc.type.group']
        resource_type = DSpaceToInvenioMapper._get_first_non_empty(row, type_fields)
        collection = row.get('collection', '')
        
        
        invenio_type = DSpaceToInvenioMapper._map_dspace_type_to_invenio(
            str(resource_type).strip(), collection
        )
        if invenio_type:
            return ResourceType(id=Identifier(__root__=invenio_type))
        
        return ResourceType(id=Identifier(__root__='publication'))
    
    @staticmethod
    def _map_dspace_type_to_invenio(dspace_type: str, collection: str) -> str:
        """Map DSpace resource type to Invenio resource type ID."""
        if dspace_type:
            dspace_type_lower = dspace_type.lower().strip()
            invenio_type = RESOURCE_TYPE_MAP.get(dspace_type_lower, 'publication')
        
        if collection in THESIS_COLLECTION_MAP:
            invenio_type = THESIS_COLLECTION_MAP.get(collection, 'thesis')
        
        return invenio_type
    
    @staticmethod
    def _get_languages(row: pd.Series) -> Optional[List[Language]]:
        """Extract and map languages from DSpace row."""
        languages = []
        language_fields = ['dc.language.iso[es_ES]', 'dc.language', 'dc.language[]']
        
        for field_name in language_fields:
            if field_name in row and not pd.isna(row[field_name]):
                value = str(row[field_name]).strip()
                langs = [lang.strip() for lang in value.split(FIELD_SEPARATORS['language']) 
                        if lang.strip()]
                
                for lang in langs:
                    lang_code = DSpaceToInvenioMapper._map_language_to_iso639_3(lang)
                    if lang_code:
                        languages.append(Language(id=Identifier(__root__=lang_code)))
        
        return languages if languages else None
    
    @staticmethod
    def _map_language_to_iso639_3(language: str) -> Optional[str]:
        """Map language names/codes to ISO 639-3 codes."""
        if not language or pd.isna(language):
            return None
        
        language = str(language).strip()
        if not language:
            return None
        
        # Try pycountry lookup first
        try:
            lang_obj = pycountry.languages.lookup(language)
            return lang_obj.alpha_3
        except LookupError:
            pass
        
        # Check language variants
        language_lower = language.lower()
        
        for lang_name, variants in LANGUAGE_VARIANTS.items():
            if any(variant in language_lower for variant in variants):
                return 'spa' if lang_name == 'spanish' else 'eng'
        
        # Try ISO code lookups
        if len(language) == 2:
            try:
                lang_obj = pycountry.languages.get(alpha_2=language)
                return lang_obj.alpha_3
            except (KeyError, AttributeError):
                pass
        
        if len(language) == 3 and language.isalpha():
            try:
                lang_obj = pycountry.languages.get(alpha_3=language)
                return lang_obj.alpha_3
            except (KeyError, AttributeError):
                return language.lower()
        
        return None
    
    @staticmethod
    def _get_dates(row: pd.Series) -> Optional[List[Date]]:
        """Extract additional dates from DSpace row and convert to EDTF format."""
        dates = []
        
        for dspace_field, date_type in DATE_TYPE_MAP.items():
            if dspace_field in row and not pd.isna(row[dspace_field]):
                original_value = str(row[dspace_field]).strip()
                if not original_value:
                    continue
                    
                # Convert the original string to a standard date object
                parsed_date = DSpaceToInvenioMapper._parse_date_string(original_value)
                
                if parsed_date:
                    # Format the date object into an EDTF Level 0 string (YYYY-MM-DD)
                    edtf_date_string = parsed_date.date().isoformat()  # Returns YYYY-MM-DD
                    
                    dates.append(Date(
                        date=edtf_date_string,
                        type=DateType(id=Identifier(__root__=date_type))
                    ))
                else:
                    # Optional: log a warning if parsing fails
                    logger.warning(f"Could not parse date '{original_value}' for field {dspace_field}. Skipping.")
        
        return dates if dates else None
    
    @staticmethod
    def _parse_date_string(date_str: str) -> Optional[datetime]:
        """Parse a variety of date strings into a datetime object using dateparser."""
        try:
            # Use dateparser to handle multiple formats and languages
            # The `settings` ensure a consistent two-digit year threshold and day-first parsing
            parsed = dateparser.parse(
                date_str,
                settings={
                    'DATE_ORDER': 'DMY',  # Optional: Adjust based on your locale (DMY, MDY, YMD)
                    'PREFER_DAY_OF_MONTH': 'first',
                    'RETURN_AS_TIMEZONE_AWARE': False,
                    'REQUIRE_PARTS': ['day', 'month', 'year']  # Ensures a full date is parsed
                }
            )
            return parsed
        except Exception as e:
            logger.debug(f"Date parsing failed for '{date_str}': {e}")
            return None
    
    @staticmethod
    def _get_locations(row: pd.Series) -> Optional[Locations]:
        """Extract spatial coverage locations from DSpace row."""
        features = []
        location_fields = ['dc.coverage.spatial[es_ES]', 'dc.coverage.spatial', 'dc.coverage.spatial[]']
        
        for field_name in location_fields:
            if field_name in row and not pd.isna(row[field_name]):
                value = str(row[field_name]).strip()
                locations = [loc.strip() for loc in value.split(FIELD_SEPARATORS['language']) 
                           if loc.strip()]
                
                for location in locations:
                    feature = Feature(
                        place=location,
                        description=f"Location: {location}"
                    )
                    features.append(feature)
        
        return Locations(features=features) if features else None
    
    @staticmethod
    def _get_rights(row: pd.Series) -> Optional[List[Right]]:
        """Extract rights information from DSpace row."""
        rights = []
        
        # License
        license_fields = ['dc.rights.license[es_ES]', 'dc.rights.license', 'dc.rights[es_ES]']
        license_value = DSpaceToInvenioMapper._get_first_non_empty(row, license_fields)
        if license_value:
            rights.append(Right(
                title={'es': str(license_value).strip()},
                description={'es': f'Licencia: {license_value}'}
            ))
        
        # Rights holder
        holder_fields = ['dc.rights.holder[es_ES]', 'dc.rights.holder']
        holder_value = DSpaceToInvenioMapper._get_first_non_empty(row, holder_fields)
        if holder_value:
            rights.append(Right(
                title={'es': f'Derechos: {holder_value}'},
                description={'es': f'Titular de derechos: {holder_value}'}
            ))
        
        return rights if rights else None
    
    @staticmethod
    def _get_formats(row: pd.Series) -> Optional[List[str]]:
        """Extract format information from DSpace row."""
        formats = []
        
        for field_name in ['dc.format[]', 'dc.format']:
            if field_name in row and not pd.isna(row[field_name]):
                value = str(row[field_name]).strip()
                if value:
                    format_list = [f.strip() for f in value.split(FIELD_SEPARATORS['language']) 
                                 if f.strip()]
                    formats.extend(format_list)
        
        return formats if formats else None
    
    
    @staticmethod
    def _process_identifiers(record: InveniordmRecordSchemaV600, row: pd.Series) -> None:
        """Process identifier fields from DSpace row."""
        identifiers = []
        pids = {}
        
        # Handle URI
        uri_fields = ['dc.identifier.uri[]', 'dc.identifier.uri']
        uri_value = DSpaceToInvenioMapper._get_first_non_empty(row, uri_fields)
        if uri_value:
            identifiers.append(IdentifiersWithScheme(
                identifier=Identifier(__root__=str(uri_value).strip()),
                scheme=Scheme(__root__='handle')
            ))
        
        # DOI
        doi_fields = ['dc.identifier.doi[es_ES]', 'dc.identifier.doi']
        doi_value = DSpaceToInvenioMapper._get_first_non_empty(row, doi_fields)
        if doi_value:
            identifiers.append(IdentifiersWithScheme(
                identifier=Identifier(__root__=str(doi_value).strip()),
                scheme=Scheme(__root__='doi')
            ))
            # pids['doi'] = ExternalPid(
            #     identifier=Identifier(__root__=str(doi_value).strip()),
            # )
        
        # ISBN
        isbn_fields = ['dc.identifier.isbn[es_ES]', 'dc.identifier.isbn', 'dc.identifier.isbn[]']
        isbn_value = DSpaceToInvenioMapper._get_first_non_empty(row, isbn_fields)
        if isbn_value:
            identifiers.append(IdentifiersWithScheme(
                identifier=Identifier(__root__=str(isbn_value).strip()),
                scheme=Scheme(__root__='isbn')
            ))
        
        # ISSN
        issn_fields = ['dc.identifier.issn[es_ES]', 'dc.identifier.issn']
        issn_value = DSpaceToInvenioMapper._get_first_non_empty(row, issn_fields)
        if issn_value:
            issn_value = issn_value.split('||')[0]
            identifiers.append(IdentifiersWithScheme(
                identifier=Identifier(__root__=str(issn_value).strip()),
                scheme=Scheme(__root__='issn')
            ))
        
        # arXiv
        arxiv_fields = ['dc.identifier.arxiv[es_ES]', 'dc.identifier.arxiv']
        arxiv_value = DSpaceToInvenioMapper._get_first_non_empty(row, arxiv_fields)
        if arxiv_value:
            pids['arxiv'] = ExternalPid(
                identifier=Identifier(__root__=str(arxiv_value).strip()),
            )
        
        # Other identifiers
        other_fields = ['dc.identifier.other', 'dc.identifier.other[es_ES]']
        other_value = DSpaceToInvenioMapper._get_first_non_empty(row, other_fields)
        if other_value:
            schemes = idutils.detectors.detect_identifier_schemes(str(other_value).strip())
            if schemes:
                identifiers.append(IdentifiersWithScheme(
                    identifier=Identifier(__root__=str(other_value).strip()),
                    scheme=Scheme(__root__=schemes[0])
                ))
        
        if identifiers:
            record.metadata.identifiers = identifiers
        if pids:
            record.pids = pids
    
    @staticmethod
    def _process_custom_fields(record: InveniordmRecordSchemaV600, collection: str, 
                              row: pd.Series) -> None:
        """Process custom fields based on DSpace collection."""
        record.custom_fields = {}
        
        # Determine journal data
        journal_data = DSpaceToInvenioMapper._journal_data(row)
        if journal_data:
            record.custom_fields['journal:journal'] = journal_data

        # Determine isbn data
        isbn_data = DSpaceToInvenioMapper._isbn_data(row)
        if isbn_data:  # Fixed: was checking 'journal_data' instead of 'isbn_data'
            record.custom_fields['imprint:imprint'] = isbn_data

        # Determine meeting data
        meeting_data = DSpaceToInvenioMapper._meeting_data(row)
        if meeting_data:  # Fixed: was checking 'journal_data' instead of 'meeting_data'
            record.custom_fields['meeting:meeting'] = meeting_data

        # Determine entity
        entity = DSpaceToInvenioMapper._determine_entity(collection, record.metadata.publisher)
        if entity:
            record.custom_fields['upr:entidades'] = {'id': entity}
        
        # Determine subjects
        subjects = DSpaceToInvenioMapper._determine_subjects(collection)
        if subjects:
            record.custom_fields['upr:materias'] = subjects

    @staticmethod
    def _journal_data(row: pd.Series) -> Optional[Dict]:
        """Extract journal information from DSpace row, including ISSN lookup."""
        issn_value = DSpaceToInvenioMapper._get_first_non_empty(
            row, ['dc.identifier.issn[es_ES]', 'dc.identifier.issn']
        )
        title = ''
        # Only attempt API call if we have an ISSN
        if issn_value:
            issn_value = issn_value.split('||')[0]
            # Clean the ISSN value (remove hyphens, whitespace)
            clean_issn = str(issn_value).strip().replace('-', '')
            
            try:                
                # Query ISSN.org API
                url = f"https://portal.issn.org/resource/ISSN/{clean_issn}?format=json"
                response = requests.get(url, timeout=10)
                logger.debug('**************************************')
                if response.status_code == 200:
                    issn_data = response.json()
                    # Parse the graph structure to find the title
                    graph_data = issn_data.get('@graph', [])
                    for item in graph_data:
                        if 'mainTitle' in item:
                            title = item['mainTitle']
                            logger.debug(f"Found journal title '{title}' for ISSN {clean_issn}")
                            break
                else:
                    logger.warning(f"ISSN API returned status {response.status_code} for ISSN {clean_issn}")
            
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout when querying ISSN API for {clean_issn}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Network error querying ISSN API for {clean_issn}: {e}")
            except (KeyError, ValueError) as e:
                logger.warning(f"Failed to parse ISSN API response for {clean_issn}: {e}")
        
        # Get other journal metadata fields
        start_page = DSpaceToInvenioMapper._get_first_non_empty(
            row, ['dc.source.initialpage[es_ES]', 'dc.source.initialpage']
        )
        end_page = DSpaceToInvenioMapper._get_first_non_empty(
            row, ['dc.source.endpage[es_ES]', 'dc.source.endpage']
        )
        
        volume = DSpaceToInvenioMapper._get_first_non_empty(
            row, ['dc.source.volume[es_ES]', 'dc.source.volume']
        )
        
        issue = DSpaceToInvenioMapper._get_first_non_empty(
            row, ['dc.source.issue[es_ES]', 'dc.source.issue']
        )
        
        # Format pages string only if we have at least start page
        pages = ''
        if start_page:
            if end_page:
                pages = f'{start_page}-{end_page}'
            else:
                pages = str(start_page)
        
        # Only return data if we have at least one meaningful field
        if any([title, volume, issue, pages, issn_value]):
            return {
                "title": title,
                "issue": str(issue) if issue else '',
                "volume": str(volume) if volume else '',
                "pages": pages,
                "issn": str(issn_value).strip() if issn_value else ''
            }
        return None

    @staticmethod
    def _isbn_data(row: pd.Series) -> Optional[Dict]:
        isbn_value = DSpaceToInvenioMapper._get_first_non_empty(row, ['dc.identifier.isbn[es_ES]', 'dc.identifier.isbn', 'dc.identifier.isbn[]'])
        # query isbn resources to determine title etc 
        if isbn_value:
            return {
                    "title": '',
                    "isbn": isbn_value,
                    "pages": '',
                    "place": '',
                }

    @staticmethod
    def _meeting_data(row: pd.Series) -> Optional[Dict]:
        meeting_value = DSpaceToInvenioMapper._get_first_non_empty(row, ['dc.source.conferencetitle[es_ES]', 'dc.source.conferencetitle', 'dc.source.conferencetitle[]'])
        # query conference resources somehow..
        if meeting_value: 
            return {
                "acronym": "",
                "dates": "",
                "place": "",
                "session_part": "",
                "session": "",
                "title": meeting_value,
                "url": "",
            }
        else:
            return None


    @staticmethod
    def _determine_entity(collection: str, publisher: str) -> Optional[str]:
        """Determine entity from collection and publisher."""
        # Check external programs
        if collection:
            upr_collection = collection.split('/')[1] if '/' in collection else ''
            match_collection = f'col_DICT_{upr_collection}'
            
            if match_collection in EXTERNAL_PROGRAM_COLLECTIONS:
                return 'externo'
        
        # Check publisher for university entities
        return DSpaceToInvenioMapper._get_entity_from_publisher(publisher)
    
    @staticmethod
    def _get_entity_from_publisher(publisher: str) -> Optional[str]:
        """Extract entity from publisher string using fuzzy matching."""
        publisher_lower = publisher.lower().strip()
        
        # Check if publisher contains university reference
        required_terms = ['universidad de pinar del río', 'upr', 'universidad de pinar del rio']
        if not any(term in publisher_lower for term in required_terms):
            return None
        
        # Clean publisher text
        clean_publisher = re.sub(
            r'universidad de pinar del río|upr',
            '', 
            publisher_lower, 
            flags=re.IGNORECASE
        ).strip()
        
        search_text = clean_publisher or publisher_lower
        titles = list(UPR_ENTITIES.values())
        
        result = process.extractOne(
            search_text, 
            titles, 
            scorer=fuzz.WRatio,
            score_cutoff=75
        )
        
        if result:
            matched_title, score, index = result
            for entity_id, title in UPR_ENTITIES.items():
                if title == matched_title:
                    return entity_id
        
        return None
    
    @staticmethod
    def _determine_subjects(collection: str) -> List[Dict[str, str]]:
        """Determine subjects from collection."""
        subjects = []
        
        if '/' in collection:
            upr_collection = collection.split('/')[1]
            match_collection = f'col_DICT_{upr_collection}'
            
            if match_collection in UPR_SUBJECT_MAP:
                subjects.append({'id': match_collection})
        
        return subjects


# Backward compatibility functions
def process_dspace_row(row: pd.Series) -> Tuple[Optional[str], Optional[str], 
                                               Optional[InveniordmRecordSchemaV600]]:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper.process_dspace_row(row)


def get_first_non_empty(row: pd.Series, field_names: List[str]) -> Any:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper._get_first_non_empty(row, field_names)


def map_dspace_type_to_invenio(dspace_type: str, collection: str) -> ResourceType:
    """Backward compatibility wrapper."""
    invenio_type = DSpaceToInvenioMapper._map_dspace_type_to_invenio(dspace_type, collection)
    return ResourceType(id=Identifier(__root__=invenio_type))


def map_language_to_iso639_3(language: str) -> Optional[str]:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper._map_language_to_iso639_3(language)


def get_names_from_str(full_name: str) -> Tuple[str, str]:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper._parse_name(full_name)


def find_person_in_invenio_dspace(family_name: str, given_name: str) -> Optional[Dict]:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper._find_person_in_invenio(family_name, given_name)


def fix_custom_fields_dspace(record: InveniordmRecordSchemaV600, collection: str, 
                            row: pd.Series) -> None:
    """Backward compatibility wrapper."""
    DSpaceToInvenioMapper._process_custom_fields(record, collection, row)


def get_entity_upr_from_publisher(publisher: str) -> Optional[str]:
    """Backward compatibility wrapper."""
    return DSpaceToInvenioMapper._get_entity_from_publisher(publisher)