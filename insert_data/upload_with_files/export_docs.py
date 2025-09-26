
import requests
import json
import os

# invenio_base_url = 'https://inveniordm.web.cern.ch'  # URL de tu instancia de InvenioRDM
# token = 'hnnwcph9ceru5M8oGQQs40XrhihjvAWgOni35mPOCitZ8ubHndcgfgIV6cgl'  # Token de acceso para la API


invenio_base_url = 'https://127.0.0.1:5000'  # URL de tu instancia de InvenioRDM
token = 'JKHdrvL49BLjJRRWR3tS41qWwn03C4CrqLW4ODonELAKwjwpBUfJDVJEs28n'  # Token de acceso para la API


def delete_all_records_and_drafts():
    headers = {
        'Authorization': f'Bearer {token}'
    }

    # Fetch all records (published + drafts)
    # Use size=1000 or more if you have many records
    url = f"{invenio_base_url}/api/records"
    params = {'size': 1000}

    response = requests.get(url, headers=headers, params=params, verify=False)
    if response.status_code != 200:
        print("❌ Failed to fetch records:", response.status_code, response.json())
        return

    records = response.json().get('hits', {}).get('hits', [])
    print(f"Found {len(records)} records (published or drafts).")

    for record in records:
        rec_id = record['id']
        is_published = 'pid' in record  # Simplified check; better: check 'is_published' if available
        # In InvenioRDM, drafts have 'is_draft': True; published records don't have that or have 'is_published': True
        is_draft = record.get('is_draft', False)

        print(f"\nProcessing record ID: {rec_id} | Draft: {is_draft}")

        try:
            if is_draft:
                # Delete draft directly
                delete_url = f"{invenio_base_url}/api/records/{rec_id}/draft"
                resp = requests.delete(delete_url, headers=headers, verify=False)
                if resp.status_code == 204:
                    print(f"✅ Draft {rec_id} deleted.")
                else:
                    print(f"❌ Failed to delete draft {rec_id}: {resp.status_code}")
            else:
                # Published record: must create a draft first, then delete it
                print(f"  → Creating draft for published record {rec_id}...")
                draft_url = f"{invenio_base_url}/api/records/{rec_id}/draft"
                resp = requests.post(draft_url, headers=headers, verify=False)
                if resp.status_code not in (201, 400):  # 400 may mean draft already exists
                    print(f"  ⚠️ Warning: Could not create draft (status {resp.status_code})")

                # Now delete the draft (this deletes the entire record)
                delete_url = f"{invenio_base_url}/api/records/{rec_id}/draft"
                resp = requests.delete(delete_url, headers=headers, verify=False)
                if resp.status_code == 204:
                    print(f"✅ Published record {rec_id} deleted via draft.")
                else:
                    print(f"❌ Failed to delete published record {rec_id}: {resp.status_code}")

        except Exception as e:
            print(f"💥 Error processing {rec_id}: {e}")

    print("\n✅ Deletion process completed.")


# with open('example.json') as f:
#     records = json.load(f)

# # files_to_upload = ['path/to/your/file1.txt', 'path/to/your/file2.txt']
# files_to_upload = ['photo.png']

def search_record_exact(value):
    """
    Busca un registro en InvenioRDM con coincidencia exacta en un campo de metadatos.
    
    Parámetros:
        field_path (str): Ruta del campo en el esquema de metadatos (e.g., 'metadata.custom_id', 'metadata.title').
                          Para coincidencia exacta, se recomienda usar '.keyword' si el campo lo soporta.
        value (str): Valor exacto a buscar.
    
    Retorna:
        str or None: ID del primer registro coincidente, o None si no se encuentra.
    """
    # Usar comillas para forzar coincidencia exacta en la API de búsqueda
    # Si el campo está mapeado como 'keyword', usa field_path + '.keyword'


    # Escapar comillas en el valor para evitar inyección en la query
    
    query = f'"{value}"'

    url = f"{invenio_base_url}/api/records"
    headers = {
        'Authorization': f'Bearer {token}'
    }
    params = {
        'q': query,
        'size': 1  # Solo necesitamos un resultado si buscamos por identificador único
    }

    response = requests.get(url, headers=headers, params=params, verify=False)
    
    if response.status_code == 200:
        hits = response.json().get('hits', {}).get('hits', [])
        if hits:
            record_id = hits[0]['id']
            print(f"Registro encontrado con {value} = '{value}': {record_id}")
            return record_id
        else:
            print(f"No se encontró ningún registro con {value} = '{value}'")
            return None
    else:
        print(f"Error en la búsqueda: {response.status_code}", response.json())
        return None

def create_or_update_record(record, record_id=None):
    """
    Crea un nuevo registro o actualiza un borrador existente en InvenioRDM.
    
    Parámetros:
        record (dict): Metadatos del registro en formato JSON.
        record_id (str, optional): ID del registro a actualizar. Si es None, se crea uno nuevo.
    
    Retorna:
        str or None: El ID del registro creado o actualizado, o None si falla.
    """
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    if record_id is None:
        # Crear un nuevo registro (draft)
        url = f"{invenio_base_url}/api/records"
        response = requests.post(url, headers=headers, data=json.dumps(record), verify=False)
        if response.status_code == 201:
            new_record_id = response.json()["id"]
            print(f"Registro creado con éxito: {new_record_id}")
            return new_record_id
        else:
            print("Error al crear el registro:", response.status_code, response.json())
            return None
    else:
        # Actualizar un draft existente
        # Primero, asegurarse de que exista un draft. Si el registro está publicado,
        # se debe crear una nueva versión antes de actualizar.
        draft_url = f"{invenio_base_url}/api/records/{record_id}/draft"
        response = requests.put(draft_url, headers=headers, data=json.dumps(record), verify=False)

        if response.status_code == 200:
            print(f"Borrador actualizado con éxito: {record_id}")
            return record_id
        elif response.status_code == 404:
            # No existe un draft; intentar crear una nueva versión si el registro existe
            print(f"No se encontró un borrador para {record_id}. Intentando crear una nueva versión...")
            version_url = f"{invenio_base_url}/api/records/{record_id}/versions"
            version_response = requests.post(version_url, headers=headers, verify=False)
            if version_response.status_code == 201:
                new_draft_id = version_response.json()["id"]
                # Ahora actualizar el nuevo draft
                draft_url = f"{invenio_base_url}/api/records/{new_draft_id}/draft"
                update_response = requests.put(draft_url, headers=headers, data=json.dumps(record), verify=False)
                if update_response.status_code == 200:
                    print(f"Nueva versión creada y actualizada: {new_draft_id}")
                    return new_draft_id
                else:
                    print("Error al actualizar la nueva versión:", update_response.status_code, update_response.json())
                    return None
            else:
                print("Error al crear nueva versión:", version_response.status_code, version_response.json())
                return None
        else:
            print("Error al actualizar el borrador:", response.status_code, response.json())
            return None


# Función para crear un nuevo registro en InvenioRDM
def create_record(record):
    url = f"{invenio_base_url}/api/records"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    print(json.dumps(record))
    response = requests.post(url, headers=headers, data=json.dumps(record), verify=False)
    if response.status_code == 201:
        record_id = response.json()["id"]
        print("Registro creado con éxito:", record_id)
        return record_id
    else:
        print("Error al crear el registro:", response.status_code)
        print("Error al crear el registro:", response)
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

# for record in records:
#     record_id = create_record(record)
#     if record_id:
#         file = upload_files(record_id, files_to_upload)
#         if file:
#             commit_files(record_id, file)
#             publish_record(record_id)