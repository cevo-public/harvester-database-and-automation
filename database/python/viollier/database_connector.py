import psycopg2, json
import os
from exceptions import *

# sample assgined to same plate + well
# plate already in database
# sample of same plate assgined to different seq centers
# check here G12 and H12 sequencing purpose
# new file but no new samples
# new routine for different labs

db_name=os.environ['DB_NAME']
db_user=os.environ['DB_USER']
db_host=os.environ['DB_HOST']
db_password=os.environ['DB_PASSWORD']

viollier_col_dict = {
    'seq_center': 'Sequencing center',
    'date': 'Order date',
    'zip': 'Zip code',
    'canton': 'Prescriber canton',
    'city': 'Prescriber city',
    'purpose': 'purpose',
    'plate':'PlateID',
    'well': 'DeepWellLocation',
    'ct': "CT Wert",
}

other_col_dict = {
    'seq_center': 'Sequencing center',
    'date': 'Order date',
    'zip': 'Zip code',
    'canton': 'Canton',
    'city': 'City',
    'purpose': 'Sequencing purpose',
    'plate':'PlateID',
    'well': 'DeepWellLocation',
    'ct': 'CT Wert',
}


class DatabaseConnector(object):
    def __init__(self):
        self.connector = None
        self.db_connection = "dbname=\'" + db_name + \
                        "\' user=\'" + db_user + \
                        "\' host=\'" + db_host + \
                        "\' password=\'" + db_password + "\'"

    
    def __enter__(self):
        try:
            self.connector = psycopg2.connect(self.db_connection)   
            return self         
        except Exception as e:
            print(f'exception! {e}')
            raise DataBaseConnectionException

    def __exit__(self, exc_type, exc_value, tb):
        self.connector.close()
        if exc_type is not None:
            print(exc_type, exc_value, tb)
        return True
    def __repr__(self):
        return (f"database is {db_host}:{db_name}")


    def exec_db_cmd(self, sqlstr, getdata=False):
        rows = None
        with self.connector:
            with self.connector.cursor() as curs:
                curs.execute(sqlstr)
                if getdata:
                    rows = curs.fetchall()
        return rows


    def get_state_dict(self):
        fetchAutomationStateSql = "\
            select state\
            from automation_state\
            where program_name = 'viollier_metadata_receiver';"
        try:
            rows = self.exec_db_cmd(fetchAutomationStateSql, getdata=True)
            state_dict = json.loads(rows[0][0])
            if state_dict == None:
                state_dict = {}
            if not "errorFiles" in state_dict:
                state_dict["errorFiles"] = []
            if not "processedFiles" in state_dict:
                state_dict["processedFiles"] = []
            return state_dict 
        except Exception as e:
            print('error getting state dict')
            return None

    def update_automation_state(self, state_str):
        updateAutomationStateSql = f"\
            update automation_state\
            set state = '{state_str}'\
            where program_name = 'viollier_metadata_receiver';"
        #print(updateAutomationStateSql)
        self.exec_db_cmd(updateAutomationStateSql)

    def get_processed_files(self):
        state_dict = self.get_state_dict()
        return state_dict.get("processedFiles") if "processedFiles" in state_dict else []

    def get_inprocessing_files(self):
        state_dict = self.get_state_dict()
        return state_dict.get("filesInProcessing") if "filesInProcessing" in state_dict else []

    def update_status(self, file_to_process):
        WasErrorFile = False
        state_dict = self.get_state_dict()
        if file_to_process in state_dict["errorFiles"]:
            state_dict["errorFiles"].remove(file_to_process)
            WasErrorFile = True
        state_dict["processedFiles"].append(file_to_process)
        state_str = json.dumps(state_dict)
        self.update_automation_state(state_str)
        return WasErrorFile

    def add_error_file(self, file_to_add):
        state_dict = self.get_state_dict()
        isNewError = False
        if not file_to_add in state_dict["errorFiles"]:
            state_dict["errorFiles"].append(file_to_add)
            isNewError = True
        state_str = json.dumps(state_dict)
        self.update_automation_state(state_str)
        return isNewError


    def get_ethid(self, sample_number, labname):       
        table = f"{labname}_metadata"
        selectSql = f"select ethid from {table} where Sample_number='{sample_number}';"
        existingids = self.exec_db_cmd(selectSql, getdata=True)
        if existingids:
            if not existingids[0][0] == None:
                raise SampleNumberExistingException
        insertSql = "select MAX(ethid) as maxid from test_metadata;"
        maxid_rows = self.exec_db_cmd(insertSql, getdata=True)
        
        maxid = maxid_rows[0][0]
        if maxid == None:
            maxid = 30000000            
        return str(int(maxid) + 1)
        

    def add_to_test_metadata(self, sample_data, col_dict, labname):
        test_id = labname + '/' + sample_data['ethid']
        order_date = sample_data[col_dict["date"]]
        zip_code = sample_data[col_dict["zip"]]
        canton = sample_data[col_dict["canton"]]
        city = sample_data[col_dict["city"]]
        purpose = sample_data[col_dict["purpose"]]
        insertSql = f"\
            insert into test_metadata (test_id, ethid, order_date, zip_code, city, canton, is_positive, purpose)\
            values ('{test_id}', '{sample_data['ethid']}', '{order_date}', '{zip_code}', '{city}', '{canton}', true, '{purpose}')\
            on conflict do nothing;\
        "
        self.exec_db_cmd(insertSql)

    def add_to_extraction_plate(self, sample_data, col_dict):
        from datetime import date
        
        extraction_plate_name = sample_data[col_dict["seq_center"]] + '/' + sample_data[col_dict["plate"]]
        left_lab_or_received_metadata_date = str(date.today())
        sequencing_center = sample_data[col_dict["seq_center"]]

        insertSql =f"\
            insert into extraction_plate (extraction_plate_name, left_lab_or_received_metadata_date, sequencing_center, comment)\
            values ('{extraction_plate_name}', '{left_lab_or_received_metadata_date}', '{sequencing_center}',\
            'The left_viollier_date might be inaccurate. It contains the date when we received the file which might not be the same date when the plate was extracted and sent.')\
            on conflict do nothing;\
        "
        self.exec_db_cmd(insertSql)
    '''
    def add_to_sequencing_plate(self, sample_data, col_dict):
        from datetime import date
        sequencing_plate_name = sample_data[col_dict["plate"]]
        sequencing_center = sample_data[col_dict["seq_center"]]
        sequencing_date = str(date.today())
        comment = ""
        insertSql = f"\
            insert into sequencing_plate (sequencing_plate_name, sequencing_center, sequencing_date, comment)\
            values ('{sequencing_plate_name}', '{sequencing_center}', '{sequencing_date}', '{comment}')\
            on conflict do nothing;"
        self.exec_db_cmd(insertSql)
    '''

    def add_to_test_plate_mapping(self, sample_data, col_dict, lab_name):
        test_id = lab_name + '/' + sample_data['ethid']
        extraction_plate = sample_data[col_dict["seq_center"]] + '/' + sample_data[col_dict["plate"]]
        extraction_plate_well = sample_data[col_dict["well"]]
        extraction_e_gene_ct = sample_data[col_dict["ct"]]

        if extraction_e_gene_ct == '-' or extraction_e_gene_ct.isspace() or not extraction_e_gene_ct:
            insertSql = f"\
                insert into test_plate_mapping (test_id, extraction_plate, extraction_plate_well, extraction_e_gene_ct, sequencing_plate, sequencing_plate_well, sample_type)\
                values ('{test_id}', '{extraction_plate}', '{extraction_plate_well}', null, null, null, 'clinical')\
                on conflict do nothing;\
            "   
        else:         
            insertSql = f"\
                insert into test_plate_mapping (test_id, extraction_plate, extraction_plate_well, extraction_e_gene_ct, sequencing_plate, sequencing_plate_well, sample_type)\
                values ('{test_id}', '{extraction_plate}', '{extraction_plate_well}', '{int(float(extraction_e_gene_ct))}', null, null, 'clinical')\
                on conflict do nothing;\
            "
        self.exec_db_cmd(insertSql)

    def add_to_viollier_metadata(self, sample_data):
        ethid = sample_data['ethid']
        Sample_number = sample_data["Sample number"]
        wuhinf_Datum_der_letzten_Infekt = sample_data["61005 wuhinf Datum der letzten Infekt."]
        wuhdat_Datum_der_Impfung = sample_data["61006 wuhdat Datum der Impfung"]
        wuhsta_COVID_19_Impfstatus = sample_data["61014 wuhsta COVID 19 Impfstatus"]
        wuhers_Datum_der_1_Impfung = sample_data["61015 wuhers Datum der 1. Impfung"]
        wuhzwe_Datum_der_2_Impfung = sample_data["61016 wuhzwe Datum der 2. Impfung"]
        wuhdri_Datum_der_3_Impfung = sample_data["61017 wuhdri Datum der 3. Impfung"]
        wuhboo_Booster_Impfung = sample_data["61070 wuhboo Booster-Impfung"]
        vacpfi_COMIRNATY_Pfizer_BioNTech = sample_data["61007 vacpfi COMIRNATY Pfizer/BioNTech"]
        vacmod_COVID_19_vaccine_Moderna = sample_data["61008 vacmod COVID 19 vaccine Moderna"]
        vacjon_COVID_19_vaccine_Johnson_Johnson = sample_data["61012 vacjon COVID 19 vaccine Johnson/Johnson"]
        vacunb_Impstoff_unbekannt = sample_data["61009 vacunb Impstoff unbekannt"]
        
        insertSql = f"\
            insert into viollier_metadata (ethid, Sample_number, wuhinf_Datum_der_letzten_Infekt, wuhdat_Datum_der_Impfung,\
                wuhsta_COVID_19_Impfstatus, wuhers_Datum_der_1_Impfung, wuhzwe_Datum_der_2_Impfung,\
                wuhdri_Datum_der_3_Impfung, wuhboo_Booster_Impfung, vacpfi_COMIRNATY_Pfizer_BioNTech,\
                vacmod_COVID_19_vaccine_Moderna, vacjon_COVID_19_vaccine_Johnson_Johnson,vacunb_Impstoff_unbekannt)\
            values ('{ethid}', '{Sample_number}', '{wuhinf_Datum_der_letzten_Infekt}', '{wuhdat_Datum_der_Impfung}',\
               '{wuhsta_COVID_19_Impfstatus}', '{wuhers_Datum_der_1_Impfung}', '{wuhzwe_Datum_der_2_Impfung}',\
               '{wuhdri_Datum_der_3_Impfung}', '{wuhboo_Booster_Impfung}', '{vacpfi_COMIRNATY_Pfizer_BioNTech}',\
                '{vacmod_COVID_19_vaccine_Moderna}', '{vacjon_COVID_19_vaccine_Johnson_Johnson}', '{vacunb_Impstoff_unbekannt}')\
            on conflict do nothing;"
        self.exec_db_cmd(insertSql)

    def add_to_imv_metadata(self, sample_data):
        ethid = sample_data['ethid']
        Sample_number = sample_data["Sample number"] if sample_data["Sample number"] else sample_data["sample_name_anonymised"]
        isolation_source_description = sample_data["isolation_source_description"]
        host_sex = sample_data["host_sex"]
        host_age = sample_data["host_age"]
        collecting_lab_name = sample_data["collecting_lab_name"]
        collecting_lab_code = sample_data["collecting_lab_code"]
        sample_name_anonymised = sample_data["sample_name_anonymised"]
        insertSql = f"\
            insert into imv_metadata (ethid, Sample_number, isolation_source_description, host_sex, host_age, collecting_lab_name, collecting_lab_code, sample_name_anonymised)\
            values ('{ethid}', '{Sample_number}', '{isolation_source_description}', '{host_sex}',\
                 '{host_age}', '{collecting_lab_name}', '{collecting_lab_code}', '{sample_name_anonymised}')\
            on conflict do nothing;\
        "
        self.exec_db_cmd(insertSql)

    def add_to_teamw_metadata(self, sample_data):
        ethid = sample_data['ethid']
        Sample_number = sample_data["Sample number"]
        insertSql = f"\
            insert into teamw_metadata (ethid, Sample_number)\
            values ('{ethid}', '{Sample_number}')\
            on conflict do nothing;\
        "
        self.exec_db_cmd(insertSql)

    def add_to_eoc_metadata(self, sample_data):
        ethid = sample_data['ethid']
        Sample_number = sample_data["Sample number"]
        insertSql = f"\
            insert into eoc_metadata (ethid, Sample_number)\
            values ('{ethid}', '{Sample_number}')\
            on conflict do nothing;\
        "
        self.exec_db_cmd(insertSql)

    def is_control_sample(self, sample_data, labname, col_dict):
        if labname == "viollier":
            if sample_data[col_dict['well']] in ["G12", "H12"]:
                return True
        elif sample_data[col_dict['purpose']] == "NA":
            return True
        return False

    def import_meta_data(self, meta_data, labname):
        print(f'INFO - Importing meta data...')
        try:
            for sample_data in meta_data:
                # add case to generate ethid here based on labname
                # add sanity check to see if sample exists in DB
                if labname == 'viollier':
                    col_dict = viollier_col_dict
                else:
                    col_dict = other_col_dict                
                if self.is_control_sample(sample_data, labname, col_dict):
                    sample_data['ethid'] = 'ETHID'
                    continue

                ethid = self.get_ethid(sample_data["Sample number"], labname)
                sample_data['ethid'] = ethid
                self.add_to_test_metadata(sample_data, col_dict, labname.lower())
                self.add_to_extraction_plate(sample_data, col_dict)
                self.add_to_test_plate_mapping(sample_data, col_dict, labname.lower())
                if labname == 'viollier':
                    self.add_to_viollier_metadata(sample_data)
                elif labname == 'imv':
                    self.add_to_imv_metadata(sample_data)
                elif labname == 'teamw':
                    self.add_to_teamw_metadata(sample_data)
                elif labname == 'eoc':
                    self.add_to_eoc_metadata(sample_data)

            return True
        except SampleNumberExistingException as e:
            print(f"Error - Sample number already exists: {sample_data['Sample number']}")
            return False
        except Exception as e:
            print(f'Error - Cannot import sample {sample_data["Sample number"]}: {e}')
            return False
            
def main():
    try:
        dbconnector = DatabaseConnector()
        with dbconnector as db:
            dbconnector.get_processed_files()
        data_list = []
        with open('test_data.json', 'r') as fin:
            data_list = json.load(fin)
            #import_meta_data(data_list)

    except DataBaseConnectionException:
        print('cannot connect to db')

if __name__ == "__main__":
	main()
