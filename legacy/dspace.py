#  Copyright (c) 2022. Universidad de Pinar del Rio
#  This file is part of SCEIBA (sceiba.cu).
#  SCEIBA is free software; you can redistribute it and/or modify it
#  under the terms of the MIT License; see LICENSE file for more details.


import os
import urllib.parse

import requests
from lxml import html


def get_agent():
    return {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0'}



###########################
#         DSPACE          #
###########################

def get_urls_download_dspace(url):

    sess = requests.Session()
    sess.headers.update(get_agent())
    sess.verify = False
    timeout = 30
    cont = 0
    dictionary = {}
    response = sess.get(url, timeout = timeout)
    doc1 = html.fromstring(response.text)
    element = doc1.xpath('//div[@class="panel panel-info"]//a')
    parsed_url = urllib.parse.urlparse(url)
    url_download = ''
    url_mod = url.replace("handle","bitstream")
    url_dom = parsed_url.scheme +'://'+parsed_url.netloc
    for e in element:
        if(e.get('href')):
            url_href = url_dom + e.get('href')
            if(url_mod in str(url_href) and url_download != url_href):
                url_download = url_href
                dictionary['download'+str(cont)] = url_download
                cont = cont+1
    return dictionary

def get_article_download_dspace(dictionary, save_dir):
    timeout = 30
    ext = ''
    dir_open = save_dir + '/'
    os.makedirs(save_dir, exist_ok=True)
    for key in dictionary:
        response = requests.get(dictionary[key], verify = False, timeout = timeout)
        #print('-------------',dictionary[key])
        #print(urllib.parse.urlparse(dictionary[key]))
        #print(response.headers)
        allowed = ['application/pdf', 'text/html']
        if(response.text != ''):
            if('Content-Disposition' in response.headers):
                content_disposition = response.headers['Content-Disposition']
                indice_1 = content_disposition.index('"') #obtenemos la posición del primer carácter "
                indice_2 = content_disposition.rfind('"') #obtenemos la posición del ultimo carácter "
                filename = content_disposition[indice_1 + 1:indice_2]
            else:
                #url_part = urllib.parse.urlparse(dictionary[key]).path
                #url_filename = url_part.replace("%20"," ")
                url_filename = "'" + dictionary[key] + "'"
                print(url_filename)
                indice_1 = url_filename.rfind('/') #obtenemos la posición del primer carácter "
                indice_2 = url_filename.rfind("'") #obtenemos la posición del ultimo carácter "
                filename = url_filename[indice_1 + 1 : indice_2]

            export_file = open(dir_open + filename, 'wb')
            export_file.write(response.content)
            export_file.close()
        cont = cont+1
    return 'ok'

def get_record_files(url, save_dir):
    res = get_urls_download_dspace(url)
    get_article_download_dspace(res, save_dir)

