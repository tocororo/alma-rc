import os
import trace
import traceback
from mapping_with_instances import load_invenio_from_folder, xml_oai_dc_to_invenio_record
from export_docs import create_record, delete_all_records_and_drafts, search_record_exact, upload_files, commit_files, publish_record
import json

def start_migration(base_path):

    data_folder = base_path # os.path.join(base_path, 'data')
    if not os.path.isdir(data_folder):
        print(f"data folder not found on: {base_path}")
        return
    count = 0
    for entry in os.listdir(data_folder):
        try:
            subfolder_path = os.path.join(data_folder, entry)
            if os.path.isdir(subfolder_path):
                # print(f"Processing data on: {subfolder_path}")
                if subfolder_path == '/home/malayo/dev/alma-rc/.data/metadatos_dspace/data/2761':
                    print('aaa')
                    rc_handle, jsonInfo = load_invenio_from_folder(subfolder_path)

                    print(jsonInfo.json())
                    print(rc_handle)
                    # if filePath: 
                    #     rc_handle, jsonInfo = xml_to_invenio_record(filePath)
                    #     # print(json.dumps(json.loads(jsonInfo.json()), indent=4, sort_keys=True, ensure_ascii=False))
                    #     # print(jsonInfo.json())
                    #     rec_oi = None
                    #     rec_oi = search_record_exact(rc_handle)
                    #     if rec_oi:
                    #         count+=1

                    # record_id = create_record(json.loads(jsonInfo.json()))
                    # if record_id:
                    #     file = upload_files(record_id, get_files_in_subfolder(subfolder_path))
                    #     if file:
                    #         commit_files(record_id, file)
                    #         publish_record(record_id)

                        
        except Exception as ex:
            print(ex)
            traceback.format_exc(ex)
    print(count)
            
        
                

def get_files_in_subfolder(subfolder):
    files_folder = os.path.join(subfolder, 'files')
    if not os.path.isdir(files_folder):
        return []
    file_paths = []
    for root, _, files in os.walk(files_folder):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths
            
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