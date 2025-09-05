
import requests
import json
import os

invenio_base_url = 'https://inveniordm.web.cern.ch'  # URL de tu instancia de InvenioRDM
token = 'hnnwcph9ceru5M8oGQQs40XrhihjvAWgOni35mPOCitZ8ubHndcgfgIV6cgl'  # Token de acceso para la API

with open('example.json') as f:
    records = json.load(f)

# files_to_upload = ['path/to/your/file1.txt', 'path/to/your/file2.txt']
files_to_upload = ['photo.png']


# Función para crear un nuevo registro en InvenioRDM
def create_record(record):
    url = f"{invenio_base_url}/api/records"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # print('--------IMPORTANTE-------')
    # print(record)
    # print(record['files'])
    # print('--------IMPORTANTE-------')
    record['files'] = {'enabled': True}  # Activar archivos
    response = requests.post(url, headers=headers, data=json.dumps(record), verify=False)
    if response.status_code == 201:
        record_id = response.json()["id"]
        print("Registro creado con éxito:", record_id)
        return record_id
    else:
        print("Error al crear el registro:", response.json())
        return None

# Función para subir archivos a un registro en InvenioRDM
def upload_files(record_id, files):
    url = f"{invenio_base_url}/api/records/{record_id}/draft/files"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    for file_path in files:
        file_name = os.path.basename(file_path)
        data = [{'key' : file_name}]
        response = requests.post(url, headers=headers, data=json.dumps(data), verify=False)

        if response.status_code == 201:
            print(f"Archivo {file_name} subido con éxito al registro {record_id}.")
        else:
            print(f"Error al subir el archivo {file_name}:", response.json())
            return
        
        content_headers = {
            'Content-Type': 'application/octet-stream',
            'Authorization': f'Bearer {token}'
        }
        url = f"{invenio_base_url}/api/records/{record_id}/draft/files/{file_name}/content"
        with open(file_path, 'rb') as file:
            response = requests.put(url, headers=content_headers, data=file, verify=False)
            
        if response.status_code == 200:
            print(f"Archivo {file_name} confirmado para el registro {record_id}.")
            return file_name
        else:
            print(f"Error al confirmar el archivo {file_name}:", response.json())
            return    

# Función para confirmar la subida de archivos en InvenioRDM
def commit_files(record_id, file):
    url = f"{invenio_base_url}/api/records/{record_id}/draft/files/{file}/commit"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.post(url, headers=headers, verify=False)
    if response.status_code == 200:
        print(f"Archivos confirmados para el registro {record_id}.")
    else:
        print(f"Error al confirmar los archivos para el registro {record_id}:", response.json())

# Función para publicar un registro en InvenioRDM
def publish_record(record_id):
    url = f"{invenio_base_url}/api/records/{record_id}/draft/actions/publish"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    response = requests.post(url, headers=headers, verify=False)
    if response.status_code == 202:
        print(f"Registro {record_id} publicado con éxito.")
    else:
        print(f"Error al publicar el registro {record_id}:", response.json())

# Crear y publicar los registros en InvenioRDM

for record in records:
    record_id = create_record(record)
    if record_id:
        file = upload_files(record_id, files_to_upload)
        if file:
            commit_files(record_id, file)
            publish_record(record_id)