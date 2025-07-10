#  Copyright (c) 2022. Universidad de Pinar del Rio
#  This file is part of SCEIBA (sceiba.cu).
#  SCEIBA is free software; you can redistribute it and/or modify it
#  under the terms of the MIT License; see LICENSE file for more details.
#
import datetime
import os
import shutil
import string
import time
import traceback
import uuid
from enum import Enum
from zipfile import BadZipFile, ZipFile
from lxml import etree
from sickle import Sickle

from collections import defaultdict


import logging

logger = logging.getLogger("iroko-harvester")


HARVESTER_DATA_DIRECTORY = 'data'
IROKO_TEMP_DIRECTORY = 'tmp'

XMLParser = etree.XMLParser(
    remove_blank_text=True, recover=True, resolve_entities=False
    )

request_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, '
                  'like Gecko) Chrome/39.0.2171.95 Safari/537.36'
    }

nsmap = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'oai-identifier': 'http://www.openarchives.org/OAI/2.0/oai-identifier',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'xml': 'http://www.w3.org/XML/1998/namespace',
    'nlm': 'http://dtd.nlm.nih.gov/publishing/2.3'
    }


XMLParser = etree.XMLParser(
    remove_blank_text=True, recover=True, resolve_entities=False
    )


class OaiHarvesterFileNames(Enum):
    IDENTIFY = "identify.xml"
    FORMATS = "metadata_formats.xml"
    SETS = "sets.xml"
    ITEM_IDENTIFIER = "id.xml"



class xmlns():
    oai = 'http://www.openarchives.org/OAI/2.0/'

    oai_identifier = 'http://www.openarchives.org/OAI/2.0/oai-identifier'

    dc = 'http://purl.org/dc/elements/1.1/'

    xsi = 'http://www.w3.org/2001/XMLSchema-instance'

    xml = 'http://www.w3.org/XML/1998/namespace'

    nlm = 'http://dtd.nlm.nih.gov/publishing/2.3'


def get_sigle_element(metadata, name, xmlns='dc', language=None):
    # # print('get_sigle_element: '+name)
    result = None
    elements = metadata.findall('.//{' + xmlns + '}' + name)
    if len(elements) > 1:
        for e in elements:
            lang = '{' + nsmap['xml'] + '}lang'
            if language and lang in e.attrib:
                if e.attrib[lang] == language:
                    result = e.text
        # print('self.logger no '+language+' error')
    if len(elements) == 1:
        result = elements[0].text

    if result is None:
        result = ""
    return result
    # # print('self.logger no name error...')


def get_multiple_elements(metadata, name, xmlns='dc', itemname=None, language=None):
    # # print('get_multiple_elements: '+name)
    results = []
    elements = metadata.findall('.//{' + xmlns + '}' + name)
    for e in elements:
        lang = '{' + nsmap['xml'] + '}lang'
        apend = None
        if language and lang in e.attrib:
            if e.attrib[lang] == language:
                if (itemname == ''):
                    apend = e.text
                else:
                    apend = {itemname: e.text}
        else:
            if (itemname):
                apend = {itemname: e.text}
            else:
                apend = e.text
        if e.text is not None and e.text != '' and apend is not None:
            results.append(apend)
    return results


def xml_to_dict(tree, paths=None, nsmap=None, strip_ns=False):
    """Convert an XML tree to a dictionary.

    :param tree: etree Element
    :type tree: :class:`lxml.etree._Element`
    :param paths: An optional list of XPath expressions applied on the XML tree.
    :type paths: list[basestring]
    :param nsmap: An optional prefix-namespaces mapping for conciser spec of paths.
    :type nsmap: dict
    :param strip_ns: Flag for whether to remove the namespaces from the tags.
    :type strip_ns: bool
    """
    paths = paths or ['.//']
    nsmap = nsmap or {}
    fields = defaultdict(list)
    for path in paths:
        elements = tree.findall(path, nsmap)
        for element in elements:
            tag = re.sub(
                r'\{.*\}', '', element.tag
                ) if strip_ns else element.tag
            fields[tag].append(element.text)
    return dict(fields)


def get_iroko_harvester_agent():
    return {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0'
        }

def exist_xml_file(base_directory, file_name, extra_path=""):
    xmlpath = os.path.join(base_directory, extra_path, file_name)
    return os.path.exists(xmlpath)


def get_xml_from_file(base_directory, file_name, extra_path=""):
    """get an lxml tree from a file with the path:
        base_directory + extra_path + file_name
        rise an Exception if the file not exists
    """

    xmlpath = os.path.join(base_directory, extra_path, file_name)
    if not os.path.exists(xmlpath):
        raise Exception(
            "Path: {0} not exists".format(
                xmlpath
                )
            )
    return etree.parse(xmlpath, parser=XMLParser)


def remove_none_from_dict(dictionary:dict):
    for key, value in list(dictionary.items()):
        if value is None:
            del dictionary[key]
        elif isinstance(value, dict):
            remove_none_from_dict(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    remove_none_from_dict(item)

    return dictionary

class ZipHelper:

    @classmethod
    def compress_dir(cls, src_path, dst_path, dst_filename):
        """
        compress the content (files and directory recursivelly) of the directory in the end of
        src_path
        to a zip file in dst_path/dst_filename
        the idea is not compress the full src_path into the zip, but relative to the directory in
        the end of the src_path.
        :param src_path: source path
        :param dst_path: destination path, excluding filename
        :param dst_filename: filename in destination path.
        :return:
        """
        zip_path = os.path.join(
            dst_path,
            dst_filename
            )
        result = []
        if os.path.isdir(src_path):
            cls._get_zip_items(result, src_path, '')
        else:
            head, tail = os.path.split(src_path)
            result.append({'src': src_path, 'arcname': tail})
        with ZipFile(zip_path, 'w') as zipObj:
            for item in result:
                zipObj.write(item['src'], arcname=item['arcname'])

    @classmethod
    def _get_zip_items(cls, result: list, src_path, item_path):
        if os.path.isdir(src_path):
            for item in os.listdir(src_path):
                cls._get_zip_items(
                    result,
                    os.path.join(src_path, item),
                    os.path.join(item_path, item)
                    )
        else:
            result.append({'src': src_path, 'arcname': item_path})



def get_current_data_dir():
    return HARVESTER_DATA_DIRECTORY



class Formatter(object):
    """ A Formatter will return a dict given something
    (xml, html, or something else) """

    def __init__(self):
        self.metadataPrefix = None

    def get_metadata_prefix(self):
        """name of the formatter oai_dc, nlm, jats"""
        return self.metadataPrefix

    def process_item(self, item):
        """given an item return a dict given an item"""
        raise NotImplementedError



class OaiFetcher:
    """ esta clase se encarga de recolectar un OAI endpoint.
    crea una estructura de carpetas donde se almacena todo lo cosechado sin procesar
    dentro de data_dir guarda un zip con un UUID como nombre que dentro tiene:
        1- ficheros con el response de los siguientes verbos:
            - identify.xml
            - metadata_formats.xml
            - sets.xml
        2- carpetas con uuid aleatorios como nombre por cada record, con la forma:
            - id.xml
            - metadata_format_1.xml
            - metadata_format_2.xml
            - fulltext_1.ext
            - fulltext_2.ext
    """

    @classmethod
    def fetch_url(cls, url, data_dir=None, wait_time=3, source_type=OJS):
        fetcher = OaiFetcher(url, data_dir=data_dir, request_wait_time=wait_time)
        return fetcher.start_harvest_pipeline()

    def __init__(self, url, data_dir=None, request_wait_time=3, source_type=OJS):

        max_retries = 3
        timeout = 30

        pid, source_rec = SourceRecord.get_source_by_pid(url)
        source_type = OJS
        if pid and source_rec:
            if "source_type" in source_rec.model.json and source_rec.model.json[
                "source_type"] == "REPOSITORY":
                source_type = DSPACE

        self.url = url
        self.request_wait_time = request_wait_time
        self.id = str(uuid.uuid4())
        self.source_type = source_type

        if not data_dir:
            self.data_dir = get_current_data_dir()
        else:
            self.data_dir = data_dir

        f = open(os.path.join(self.data_dir, self.id + '-url'), "w", encoding='UTF-8')
        f.write(self.url)
        f.close()

        self.harvest_dir = os.path.join(
            IROKO_TEMP_DIRECTORY,
            "iroko-harvest-" + str(self.id)
            )
        shutil.rmtree(self.harvest_dir, ignore_errors=True)
        if not os.path.exists(self.harvest_dir):
            os.mkdir(self.harvest_dir)

        self.formats = []
        self.oai_dc = DubliCoreElements()
        self.nlm = JournalPublishing()

        # args = {'headers':request_headers,'proxies':proxies,'timeout':15, 'verify':False}
        args = {"headers": request_headers, "timeout": timeout, "verify": False}
        self.sickle = Sickle(
            self.url,
            encoding='UTF-8',
            max_retries=max_retries,
            **args
            )

    def start_harvest_pipeline(self):
        """default harvest pipeline, identify, discover, process"""
        try:
            self.identity_source()
            self.get_items()
            return self.compress_harvest_dir()
        except Exception as e:
            f = open(os.path.join(self.data_dir, self.id + '-error'), "w", encoding='UTF-8')
            f.write(traceback.format_exc())
            f.close()
            shutil.rmtree(self.harvest_dir, ignore_errors=True)
            return None

    def compress_harvest_dir(self):
        """compress the harvest_dir to a zip file in harvest_data dir
        and deleted harvest_dir """
        shutil.rmtree(
            os.path.join(self.data_dir, str(self.id)),
            ignore_errors=True
            )
        ZipHelper.compress_dir(self.harvest_dir, self.data_dir, str(self.id))
        shutil.rmtree(self.harvest_dir, ignore_errors=True)
        return os.path.join(self.data_dir, str(self.id))

    def identity_source(self):
        self.get_identify()
        self.get_formats()
        self.get_sets()

    def _write_file(self, name, content, extra_path=""):
        """helper function, always write to f = open(os.path.join(self.harvest_dir, extra_path,
        name),"w")"""

        f = open(os.path.join(self.harvest_dir, extra_path, name), "w", encoding='UTF-8')
        f.write(content)
        f.close()

    def _get_xml_from_file(self, name, extra_path=""):
        return get_xml_from_file(self.harvest_dir, name, extra_path=extra_path)

    # TODO:
    # BUG: Traceback (most recent call last):
    #   File "/opt/iroko/iroko/harvester/oai/harvester.py", line 856, in start_harvest_pipeline
    #     self.identity_source()
    #   File "/opt/iroko/iroko/harvester/oai/harvester.py", line 878, in identity_source
    #     self.get_identify()
    #   File "/opt/iroko/iroko/harvester/oai/harvester.py", line 895, in get_identify
    #     identify = self.sickle.Identify()
    #   File "/usr/local/lib/python3.9/dist-packages/sickle/app.py", line 179, in Identify
    #     return Identify(self.harvest(**params))
    #   File "/usr/local/lib/python3.9/dist-packages/sickle/models.py", line 74, in __init__
    #     super(Identify, self).__init__(identify_response.xml, strip_ns=True)
    #   File "/usr/local/lib/python3.9/dist-packages/sickle/models.py", line 45, in __init__
    #     self._oai_namespace = get_namespace(self.xml)
    #   File "/usr/local/lib/python3.9/dist-packages/sickle/utils.py", line 20, in get_namespace
    #     return re.search('(\{.*\})', element.tag).group(1)
    # AttributeError: 'NoneType' object has no attribute 'group'

    def get_identify(self):
        """get_identity, raise IrokoHarvesterError"""
        identify = self.sickle.Identify()
        xml = identify.xml
        self._write_file("identify.xml", identify.raw)

    def get_formats(self):
        """get_formats, raise IrokoHarvesterError"""

        arguments = {}
        items = self.sickle.ListMetadataFormats(**arguments)
        for f in items:
            self.formats.append(f.metadataPrefix)
        self._write_file("metadata_formats.xml", items.oai_response.raw)

        if "oai_dc" not in self.formats:
            self._write_file(
                'error_no_dublin_core', " oai_dc is not supported by {0} ".format(self.url)
                )

    def get_sets(self):
        """get_sets"""
        arguments = {}
        items = self.sickle.ListSets(**arguments)
        self._write_file("sets.xml", items.oai_response.raw)

    def get_items(self):
        """retrieve all the identifiers of the source, create a directory structure,
        and save id.xml for each identified retrieved.
        Check if the repo object identifier is the same that the directory identifier.
        If a item directory exist, delete it and continue"""

        xml = self._get_xml_from_file("identify.xml")
        identifier = xml.find(
            ".//{" + xmlns.oai_identifier + "}repositoryIdentifier"
            )

        iterator = self.sickle.ListIdentifiers(
            metadataPrefix=self.oai_dc.metadataPrefix
            )
        count = 0
        for item in iterator:
            harvest_item_id = str(uuid.uuid4())
            p = os.path.join(self.harvest_dir, harvest_item_id)
            if not os.path.exists(p):
                os.mkdir(p)
            self._write_file("id.xml", item.raw, harvest_item_id)
            self._get_all_formats(item.identifier, harvest_item_id)

            time.sleep(self.request_wait_time)

    def _get_all_formats(self, identifier, harvest_item_id):
        """retrieve all the metadata of an item and save it to files"""

        for f in self.formats:
            try:
                arguments = {"metadataPrefix": f, "identifier": identifier}
                record = self.sickle.GetRecord(**arguments)
                self._write_file(f + ".xml", record.raw, harvest_item_id)
                time.sleep(self.request_wait_time)
                if f == "oai_dc":
                    xml = get_xml_from_file(
                        self.harvest_dir, f + ".xml", harvest_item_id)
                    data = self.oai_dc.process_item(xml)
                    for id in data['identifiers']:
                        if id['idtype'] == 'url':
                            get_files[self.source_type](
                                id['value'],
                                os.path.join(self.harvest_dir, harvest_item_id, 'files'))

            except Exception as e:
                self._write_file('error', traceback.format_exc(), harvest_item_id)
        time.sleep(self.request_wait_time)

