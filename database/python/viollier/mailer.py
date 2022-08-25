import smtplib, ssl, json, os
import pandas as pd
from exceptions import *

EMAILS_SENDER_SMTP_USERNAME=os.environ['SMTP_USER']
EMAILS_SENDER_SMTP_PASSWORD=os.environ['SMTP_PASSWORD']
EMAILS_SENDER_EMAIL=os.environ['EMAIL_ADDRESS']
EMAILS_SENDER_SMTP_HOST=os.environ['SMTP_HOST']
EMAILS_SENDER_SMTP_PORT=os.environ['SMTP_PORT']
admin_receiver_email = os.environ['ADMIN_EMAILS']
meta_receiver_email = os.environ['META_EMAILS']

class Mailer():
    def __init__(self):
        pass
    def __repr__(self):
        return (f"using {EMAILS_SENDER_SMTP_HOST}:{EMAILS_SENDER_SMTP_PORT} from {EMAILS_SENDER_EMAIL} as {EMAILS_SENDER_SMTP_USERNAME}")
    
    def send_mail(self, receiver_email, subject, message_text, attachment=None):
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email import encoders



        message = MIMEMultipart()
        message['From'] = EMAILS_SENDER_EMAIL
        message['To'] = receiver_email
        message['Subject'] = subject
        message.attach(MIMEText(message_text, 'plain'))

        if not attachment == None:
            attach_file_name = attachment
            attach_file = open(attach_file_name, 'rb') # Open the file as binary mode
            payload = MIMEBase('application', 'octate-stream')
            payload.set_payload((attach_file).read())
            encoders.encode_base64(payload) #encode the attachment
            #add payload header with filename
            payload.add_header('Content-Disposition', f"attachment; filename={attach_file_name}")
            message.attach(payload)



        context = ssl.create_default_context()
        with smtplib.SMTP(EMAILS_SENDER_SMTP_HOST, EMAILS_SENDER_SMTP_PORT) as server:
            server.ehlo()  # Can be omitted
            server.starttls(context=context)
            server.ehlo()  # Can be omitted
            server.login(EMAILS_SENDER_SMTP_USERNAME, EMAILS_SENDER_SMTP_PASSWORD)
            text = message.as_string()
            server.sendmail(EMAILS_SENDER_EMAIL, receiver_email.split(','), text)

    def send_correct_meta_error(self, file_name):
        def create_email_text(file_name):
            html_text = f"Hi there, metadata file {file_name} is correct now and the data have been imported."
            subject = f"[Harvester] Corrected: Metadata file {file_name}"
            return subject, html_text       
        subject, html_text = create_email_text(file_name)
        receiver_email = self.get_receiver_emails('admin')
        self.send_mail(receiver_email, subject, html_text)

    def get_receiver_emails(self, sequencing_center):
        if sequencing_center == "admin":
            return admin_receiver_email
        else:
            return meta_receiver_email

    def send_new_data_received(self, file_name, meta_data, labname):
            
        def order_data_list(meta_data):
            meta_data.sort(key=lambda x: int(x['DeepWellLocation'][1:]))
            '''
            final_list = [[None for x in range(8)] for x in range(12)]
            for item in meta_data:
                row_index = int(item['DeepWellLocation'][1:])-1
                col_index = ord(item['DeepWellLocation'][0]) - ord('A')
                final_list[row_index][col_index] = item

            '''
            
            return meta_data
        
        def get_plateID(sample_data, labname):
            return sample_data["PlateID"] if labname in ['viollier', 'imv'] else "PLATE"

        def get_wellID(sample_data, lab_name):
            return sample_data["DeepWellLocation"] if labname in ['viollier', 'imv'] else "WELL"

        def get_type(lab_name, well_location):
            if well_location == 'G12':
                return 'positiveControl'
            elif well_location == 'H12':
                return 'negativeControl'
            else:
                return "TYPE"

        def add_control_sample(hasPC, hasNC, html_data_list, labname):
            if labname in ['viollier', 'imv']:
                if not hasPC:
                    new_item = {
                        "ethid": 'ETHID',
                        "order_date": '',
                        "ct": '',
                        "plate_name": html_data_list[0]['plate_name'],
                        "well_position": 'G12',
                        "ethid--plate--well--type--opt": f"ETHID--{html_data_list[0]['plate_name']}--G12--positiveControl--opt",
                        "sample_name": '',
                    }
                    html_data_list.append(new_item)
                if not hasNC:
                    new_item = {
                        "ethid": 'ETHID',
                        "order_date": '',
                        "ct": '',
                        "plate_name": html_data_list[0]['plate_name'],
                        "well_position": 'H12',
                        "ethid--plate--well--type--opt": f"ETHID--{html_data_list[0]['plate_name']}--H12--negativeControl--opt",
                        "sample_name": '',
                    }
                    html_data_list.append(new_item)
            else:
                new_item = {
                    "ethid": 'ETHID',
                    "order_date": '',
                    "ct": '',
                    "plate_name": '',
                    "well_position": '',
                    "ethid--plate--well--type--opt": f"ETHID--PLATE--WELL--positiveControl--opt",
                    "sample_name": '',
                }
                html_data_list.append(new_item)
                new_item = {
                    "ethid": 'ETHID',
                    "order_date": '',
                    "ct": '',
                    "plate_name": '',
                    "well_position": '',
                    "ethid--plate--well--type--opt": f"ETHID--PLATE--WELL--negativeControl--opt",
                    "sample_name": '',
                }
                html_data_list.append(new_item)

                    
        def calibrate_data(final_list, labname):
            html_data_dict = {
                'sequencing_center':final_list[0]["Sequencing center"],
                'PlateID': final_list[0]["PlateID"],
                'data': [],
            }
            hasPC = False
            hasNC = False

            for sample_data in final_list:
                PlateID = get_plateID(sample_data, labname)
                wellID = get_wellID(sample_data, labname)
                if wellID == 'G12' and labname == 'viollier':
                    hasPC = True
                elif wellID == 'H12' and labname == 'viollier':
                    hasNC = True
                mytype = get_type(labname, wellID)
                new_item = {
                    "ethid": sample_data["ethid"],
                    "order_date": sample_data["Order date"],
                    "ct": sample_data["CT Wert"],
                    "plate_name": sample_data["PlateID"],
                    "well_position": sample_data["DeepWellLocation"],
                    "ethid--plate--well--type--opt": f"{sample_data['ethid']}--{PlateID}--{wellID}--{mytype}--opt",
                    "sample_name": sample_data["Sample number"],
                }
                html_data_dict['data'].append(new_item)
            add_control_sample(hasPC, hasNC, html_data_dict['data'], labname)

            return html_data_dict

        def create_attachment(sequencing_center, data_list):
            from datetime import date
            
            today = str(date.today())
            file_name = f"sars-cov-2_samples_{sequencing_center}_{today}.csv"

            df = pd.DataFrame(data_list)
            df.to_csv(file_name, index=False)

            return file_name

        def create_email_text(sequencing_center, PlateID, data, file_name, labname):
            html_text = ["Hi there,"]
            str1 = f"\nSequencing center: {sequencing_center}"
            html_text.append(str1)
            str2 = f"The plate(s): {PlateID}\n"
            html_text.append(str2)
            html_text = html_text[0:1] + [f"\nWe received new metadata for {len(data)} samples from {labname}. Please find attached the sample list."] + html_text[1:] 
            html_text.append("Best,")
            html_text.append("Harvester,")
            html_text.append("(On behalf of the ETH Zurich SARS-CoV-2 sequencing surveillance team)")
            subject = f"[Harvester] Received {len(data)} samples from {labname}: {file_name}"
            return subject, '\n'.join(html_text)

        final_list = order_data_list(meta_data)
        html_data_dict = calibrate_data(final_list, labname)
        # divide data for different seq center and add multiple attachments
        attachment = create_attachment(html_data_dict['sequencing_center'], html_data_dict['data'])
        subject, html_text = create_email_text(html_data_dict['sequencing_center'], html_data_dict['PlateID'], html_data_dict['data'], file_name, labname)
        receiver_email = self.get_receiver_emails(html_data_dict['sequencing_center'])
        self.send_mail(receiver_email, subject, html_text, attachment)

        
    def send_error_file(self, file_name, labname):
        def create_email_text(file_name, labname):
            html_text = f"Hi there, metadata file {file_name} contains error and cannot be imported."
            subject = f"[Harvester] Error in {labname} metadata file {file_name}"
            return subject, html_text       
        subject, html_text = create_email_text(file_name, labname)
        receiver_email = self.get_receiver_emails('admin')
        self.send_mail(receiver_email, subject, html_text)

def main():
    import smtplib, ssl, json

    data_list = []
    with open('test_data.json', 'r') as fin:
        data_list = json.load(fin)

    mymailer = Mailer()
    mymailer.send_new_data_received('uploaded_meta.csv', data_list)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(e)