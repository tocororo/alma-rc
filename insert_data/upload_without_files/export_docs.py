import requests
import json

invenio_base_url = 'https://inveniordm.web.cern.ch'  # URL de tu instancia de InvenioRDM
token = 'hnnwcph9ceru5M8oGQQs40XrhihjvAWgOni35mPOCitZ8ubHndcgfgIV6cgl'  # Token de acceso para la API

with open('example.json') as f:
    records = json.load(f)
    print(records)


def create_record(record):
    url = f"{invenio_base_url}/api/records"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    response = requests.post(url, headers=headers, data=json.dumps(record), verify=False)
    if response.status_code == 201:
        record_id = response.json()["id"]
        print("Registro creado con éxito:", record_id)
        return record_id
    else:
        print("Error al crear el registro:", response.json())
        return None
        
def publish_record(record_id):
    url = f"{invenio_base_url}/api/records/{record_id}/draft/actions/publish"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    response = requests.post(url, headers=headers, verify=False)
    print(response)
    if response.status_code == 202:
        print(f"Registro {record_id} publicado con éxito.")
    else:
        print(f"Error al publicar el registro {record_id}:", response.json())

for record in records:
    record_id = create_record(record)
    if record_id:
        print("valid record")
        publish_record(record_id)