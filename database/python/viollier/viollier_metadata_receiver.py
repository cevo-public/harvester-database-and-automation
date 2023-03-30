import os
from os import environ
from pathlib import Path

from webdav3.client import Client

from database_connector import DatabaseConnector
from mailer import Mailer
from parser import Parser

for variable in ('VIOLLIER_METADATA_PATH', 'POLYBOX_METADATA_PATH', 'POLYBOX_USER', 'POLYBOX_PASSWORD'):
    if variable not in environ:
        raise Exception(F"Setup the required environment variable: {variable}")

#pangolin path to locate meta data
VIOLLIER_METADATA_ROOTPATH = environ['VIOLLIER_METADATA_PATH']
POLYBOX_PATH = f"{environ['POLYBOX_METADATA_PATH']}/Shared"

WEBDAV3_CLIENT_OPTIONS = {
    'webdav_hostname': environ['WEBDAV_BASE_URL'],
    'webdav_login': environ['POLYBOX_USER'],
    'webdav_password': environ['POLYBOX_PASSWORD'],
}

LAB_DIR_LIST = ('ETHZ-IMV', 'ETHZ-TeamW', 'ETHZ-EOC')

client = Client(WEBDAV3_CLIENT_OPTIONS)

for lab_dir in LAB_DIR_LIST:
    os.makedirs(os.path.join(POLYBOX_PATH, lab_dir, 'metadata'), exist_ok=True)

def get_lab_name(filepath):
    if VIOLLIER_METADATA_ROOTPATH in filepath:
        return 'viollier'
    elif 'IMV' in filepath:
        return 'imv'
    elif 'EOC' in filepath:
        return 'eoc'
    elif 'TeamW' in filepath:
        return 'teamw'

def process_file(file_tuple, labname):
    parseOK = False
    importOK = False
    try:
        parseOK, meta_data = parse_meta_data(file_tuple, labname)
    except Exception as e:
        print("Error - Parsing meta data failed: {e}")
    if parseOK:
        importOK = import_meta_data(meta_data, labname)
        if importOK:
            print(f"INFO - Imported meta data for {len(meta_data)} samples: {file_tuple[1]}")
            handle_ok(file_tuple[0], meta_data, labname)
    if not parseOK or not importOK:
        print(f"Error - Import {file_tuple[1]} failed")
        handle_error(file_tuple[0], labname)


def is_valid_file(file_name, filepath):
    if file_name.startswith('.') or not file_name.endswith('.csv'):
        return False
    if 'TeamW' in filepath and file_name.startswith('2021'):
        return False

    return True

def process_viollier():
    file_list_total = [(f.path.split('/')[-1], f.path) for f in os.scandir(VIOLLIER_METADATA_ROOTPATH) if not f.is_dir()]
    file_list_sanitised = [f for f in file_list_total if is_valid_file(f[0],f[1])]
    file_list_processed = []
    file_list_inprocessing = []
    with DatabaseConnector() as db:
        file_list_processed = db.get_processed_files()
        file_list_inprocessing = db.get_inprocessing_files()
    if len(file_list_processed) > 0:
        file_list_to_process = [f for f in file_list_sanitised if not f[0] in file_list_processed and not f[0] in file_list_inprocessing]
    else:
        file_list_to_process = file_list_sanitised
    if len(file_list_to_process) > 0:    
        for metadata_file_tuple in file_list_to_process:
            process_file(metadata_file_tuple, get_lab_name(metadata_file_tuple[1]))
    return len(file_list_to_process)

def process_polybox():
    for lab in LAB_DIR_LIST:
        local_metadata_path = os.path.join(POLYBOX_PATH, lab, 'metadata')
        remote_metadata_path = os.path.join('Shared', lab, 'metadata')
        file_list_total = [(f, f"{local_metadata_path}/{f}") for f in client.list(remote_metadata_path)[1:]]
        file_list_sanitised = [f for f in file_list_total if is_valid_file(f[0],f[1])]
        file_list_processed = []
        file_list_inprocessing = []
        with DatabaseConnector() as db:
            file_list_processed = db.get_processed_files()
            file_list_inprocessing = db.get_inprocessing_files()
        if len(file_list_processed) > 0:
            file_list_to_process = [f for f in file_list_sanitised if not f[0] in file_list_processed and not f[0] in file_list_inprocessing]
        else:
            file_list_to_process = file_list_sanitised
        if len(file_list_to_process) > 0:
            for metadata_file_tuple in file_list_to_process:
                try:
                    client.download_sync(remote_path=f"{remote_metadata_path}/{metadata_file_tuple[0]}",
                                         local_path=metadata_file_tuple[1])
                    process_file(metadata_file_tuple, get_lab_name(metadata_file_tuple[1]))
                finally:
                    Path(metadata_file_tuple[1]).unlink(missing_ok=True)

    return len(file_list_to_process)

def parse_meta_data(file_tuple, labname):
    parse_result = False
    meta_data = None
    metadata_parser = Parser()
    try:
        meta_data, parse_result = metadata_parser.parse_meta_data(file_tuple[1], labname)
    except Exception as e:
        print(f'Error: data parsing failed for file {file_tuple[1]} ---------- {e}')
        parse_result = False
    return parse_result, meta_data

def import_meta_data(meta_data, labname):
    import_result = False
    with DatabaseConnector() as db_connector:
        import_result = db_connector.import_meta_data(meta_data, labname)
    return import_result

def handle_ok(file_to_process, meta_data, labname):
    mailer = Mailer()     
    with DatabaseConnector() as db_connector:
        WasErrorFile = db_connector.update_status(file_to_process)

    if WasErrorFile:
        mailer.send_correct_meta_error(file_to_process)
    mailer.send_new_data_received(file_to_process, meta_data, labname)
    
    
    
    
def handle_error(file_to_process, labname):    
    with DatabaseConnector() as db_connector:
        isNewError = db_connector.add_error_file(file_to_process)

    if isNewError:
        mailer = Mailer()
        mailer.send_error_file(file_to_process, labname)

    
    
import time, sys



def main():
    interval_max = int(os.environ['WAIT_SECONDS'])
    interval = interval_max
    start_time = time.time()
    while True:
        start_time = time.time()
          
        try:
            len_viollier = process_viollier()
            len_other = process_polybox()
            total_newfiles = len_viollier + len_other
            if  total_newfiles == 0:
                print("Info: Didn't find new files, nothing to do.")              
        except Exception as e:
            print(f"Error: Unexpected error: {e}")
        interval = int(interval_max - time.time() + start_time)
        print(f"Info: Will sleep for {interval} seconds")
        sys.stdout.flush()
        time.sleep(interval)
        

    
if __name__ == "__main__":
    main()
