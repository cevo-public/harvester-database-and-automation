from turtle import isvisible
import pandas as pd
import re
from exceptions import *

class Parser():
    '''
    parse meta data file from different labs
    return error if
    the format of file is incorrect 
    or required value is missing 
    or unknown sequencing center is detected
    '''
    def __init__(self):
        self.KNOWN_SEQUENCING_CENTER = {
            "viollier": "viollier",
               "gfb":"gfb",
               "fgcz":"fgcz",
               "health2030": "h2030",
        }
        self.viollierRequiredColumns = (
            "Prescriber city",
            "Zip code",
            "Prescriber canton",
            "Sequencing center",
            "Sample number",
            "Order date",
            "PlateID",
            "CT Wert",
            "DeepWellLocation",
        )
        self.viollierMandatoryData = (
            "Prescriber canton",
            "Zip code",
            "Sample number",
            "Sequencing center",
            "Order date",
        )
        self.imvRequiredColumns = (
            "City",
            "Zip code",
            "Canton",
            "Sequencing center",
            "Sample number",
            "Order date",
            "PlateID",
            "CT Wert",
            "DeepWellLocation",
            "Sequencing purpose",
            "sample_name_anonymised",
        )
        self.imvMandatoryData = (
            "Zip code",
            "sample_name_anonymised",
            "Sequencing center",
            "Order date",      
            "Sequencing purpose",      
        )
        self.otherrRequiredColumns = (
            "City",
            "Zip code",
            "Canton",
            "Sequencing center",
            "Sample number",
            "Order date",
            "PlateID",
            "CT Wert",
            "DeepWellLocation",
            "Sequencing purpose",
        )
        self.otherMandatoryData = (
            "Zip code",
            "Sample number",
            "Sequencing center",
            "Order date", 
            "Sequencing purpose",           
        )
        # TODO: what is this for?
        self.viollierToleratedColumns = (
            "Author list for GISAID",
            "60997 wuha20", #??
        )


    def parse_meta_data(self, file_to_parse, labname):

        def validate_date(df):
            def wrong_dates(date_string):
                pattern01 = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                pattern02 = '^[0-9]{2}.[0-9]{2}.[0-9]{4}$'
                if re.search(pattern01, date_string) == None and re.search(pattern02, date_string) == None:
                    return True
                return False

            wrongdates = df.loc[df["Order date"].apply(wrong_dates)]
            if len(wrongdates) > 0 :
                return False, len(wrongdates)
            else:
                return True, 0

        def validate_data_completion(df, labname):
            def is_val_null(x):
                return x.isspace()
            # check if all required columns exist
            requiredColumn = []
            mandatoryData = []
            if labname == "viollier":
                requiredColumn = self.viollierRequiredColumns
                mandatoryData = self.viollierMandatoryData
            elif labname == "imv":
                requiredColumn = self.imvRequiredColumns
                mandatoryData = self.imvMandatoryData
            else: 
                requiredColumn = self.otherrRequiredColumns
                mandatoryData = self.otherMandatoryData

            for key in requiredColumn:
                if not key in list(df.columns):
                    print(f'Error: {key} not in {list(df.columns)}')
                    raise DataIncompleteException
            # check if data is null for columns in mandatoryData
            for key in mandatoryData:
                nullval =df.loc[df[key].apply(is_val_null)]
                if len(nullval) > 0:
                    print(f'Error: {key} val is null')
                    raise DataIncompleteException

        def remove_quotes_from_value(x):
            return str(x).strip('"')

        def remove_quotes_from_column_name(old_cols):
            col_dict = {col:col.strip('"') for col in old_cols}
            return col_dict

        def calibrate_date(x):
            if '.' in x:
                token_list = x.split('.')[::-1]
                return '-'.join(token_list)
            else:
                return x

        def calibrate_sequencing_center(x):
            for center in self.KNOWN_SEQUENCING_CENTER.keys():
                if center in  x.lower():
                    return self.KNOWN_SEQUENCING_CENTER[center]
            else:
                print(f"Error: {x.lower()} is not in sequencing center list {self.KNOWN_SEQUENCING_CENTER.keys()}")
                raise UnKnownSequencingCenterException

        def calibrate_sequencing_purpose(x):
            if x == "res":
                return "other"
            elif x.isspace() or not x:
                return "surveillance"
            elif x.lower() in ["surveillance", "surveillance_hosp", "screening"]:
                return x.lower()
            elif x == 'NA':
                return x
            else: 
                print(f"Error - unknown sequencing purpose: {x}")
                raise UnKnownSequencingPurposeException
                
        def calibrate_plate_name(x):
            #upper-case and (A01 -> A1)
            return x[0].upper() + str(int(x[1:]))

        def add_purpose(df, labname):
            if labname == "viollier":
                if not "60997 wuha20" in df.columns:
                    df["60997 wuha20"] = " "
                df["purpose"] = df["60997 wuha20"].apply(calibrate_sequencing_purpose)
            else:
                df["purpose"] = df["Sequencing purpose"].apply(calibrate_sequencing_purpose)

        print(f"INFO - Found new metadata file {file_to_parse}, parsing...")
        if labname == "viollier":
            df = pd.read_csv(file_to_parse, sep = ';"', engine='python')
        else:
            df = pd.read_csv(file_to_parse, sep = ';', engine='python', keep_default_na=False)
        df = df.applymap(remove_quotes_from_value)
        newcolnames = remove_quotes_from_column_name(list(df.columns))
        df.rename(newcolnames,axis=1, inplace=True)
        # drop empty rows
        df.dropna(how='all')
        try:            
            validate_data_completion(df, labname)
            valid_date, wrong_dates = validate_date(df)
            if not valid_date:
                if not (wrong_dates == 1 and labname == 'imv'):
                    raise WrongDateFormatException
        except DataIncompleteException:
            print('Error - data incomplete')
            return {}, False
        except WrongDateFormatException:
            print('Error - date format incorrect')
            return {}, False
        try:
            df["Sequencing center"] = df["Sequencing center"].apply(calibrate_sequencing_center)
            df["DeepWellLocation"] = df["DeepWellLocation"].apply(calibrate_plate_name)   
            df["Order date"] = df["Order date"].apply(calibrate_date)
            add_purpose(df, labname)         
        except UnKnownSequencingCenterException:
            print('Error - unknown sequencing center')
            return {}, False
        except UnKnownSequencingPurposeException:
            print('Error - unknown sequecing purpose')
            return {}, False

        # convert df in to list of dictionarys
        data_dict = df.to_dict('records')

        return data_dict, True


def main():
    import sys
    myparser = Parser()
    data_list = myparser.parse_meta_data(sys.argv[1])
    import json
    with open('test_data.json', 'w') as fout:
        json.dump(data_list, fout)
if __name__ == "__main__":
    main()






