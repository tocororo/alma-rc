import os
import trace
import traceback
from mapping_with_instances import load_invenio_from_folder, xml_oai_dc_to_invenio_record
from insert_data.invenio_actions import create_or_update_record, create_record, delete_all_records_and_drafts, search_record_exact, upload_files, commit_files, publish_record
import json

def start_migration(base_path):

    data_folder = base_path # os.path.join(base_path, 'data')
    if not os.path.isdir(data_folder):
        print(f"data folder not found on: {base_path}")
        return
    count = 0
    bug = []
    vacias = []
    for entry in os.listdir(data_folder):
        try:
            subfolder_path = os.path.join(data_folder, entry)
            if os.path.isdir(subfolder_path):
                dc_path = os.path.join(subfolder_path, 'metadata_oai_dc.xml')
                if not os.path.exists(dc_path):
                    vacias.append(subfolder_path)
                # print(f"Processing data on: {subfolder_path}")
                # if subfolder_path == '/home/malayo/dev/alma-rc/.data/metadatos_dspace/data/2619':
                
                rc_handle, jsonInfo = load_invenio_from_folder(subfolder_path)
                if rc_handle != '' and jsonInfo is not None:
                # print(jsonInfo.json())
                    print(rc_handle)

                    record_id = None
                    record_id = search_record_exact('metadata.identifiers.identifier', rc_handle)
                    if record_id is not None:
                        count+=1
                    else:
                        dc_path = os.path.join(subfolder_path, 'metadata_oai_dc.xml')
                        if os.path.exists(dc_path):
                            bug.append(subfolder_path)

                    record_id = create_or_update_record(record=json.loads(jsonInfo.json()), record_id=record_id, subfolder_path=subfolder_path)
                
                            
        except Exception as ex:
            print(ex)
            traceback.format_exc(ex)
    print(f'{count} of {len(os.listdir(data_folder))}' )
    print(f'bug: {len(bug)}')
    for b in bug:
        print(b)
    print('-------------------------------------')
    print(f'carpetas vacias : {len(vacias)}')
    for b in vacias:
        print(b)
            
        
def load_dc_xml(subfolder):
    for file in os.listdir(subfolder):
        if file.endswith('_dc.xml'):
            file_path = os.path.join(subfolder, file)
            print(f"Cargando fichero: {file_path}")
            return file_path
            
    print(f"No se encontró fichero dc.xml en {subfolder}")
    return None

           

# Ejemplo de uso:
if __name__ == "__main__":
    current_path = "/home/malayo/dev/alma-rc/.data/metadatos_dspace/data/"
    start_migration(current_path)