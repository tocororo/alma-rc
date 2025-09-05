import os
from mapping_with_instances import xml_to_invenio_record
from export_docs import create_record, upload_files, commit_files, publish_record

def start_migration(base_path):
    data_folder = os.path.join(base_path, 'data')
    if not os.path.isdir(data_folder):
        print(f"data folder not found on: {base_path}")
        return

    for entry in os.listdir(data_folder):
        subfolder_path = os.path.join(data_folder, entry)
        if os.path.isdir(subfolder_path):
            print(f"Processing data on: {subfolder_path}")
            filePath = load_dc_xml(subfolder_path)
            if filePath: 
                jsonInfo = xml_to_invenio_record(filePath)
                record_id = create_record(jsonInfo.json())
                if record_id:
                    file = upload_files(record_id, get_files_in_subfolder(subfolder_path))
                    if file:
                        commit_files(record_id, file)
                        publish_record(record_id)
                

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
            return file
            
    print(f"No se encontró fichero dc.xml en {subfolder}")
    return None

           

# Ejemplo de uso:
if __name__ == "__main__":
    current_path = os.path.dirname(os.path.abspath(__file__))
    start_migration(current_path)