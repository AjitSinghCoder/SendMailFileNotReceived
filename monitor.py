import pdb
import os
import time
import smtplib
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

class FileMonitor:
    
    def __init__(self, master_file_path, watch_directory, check_interval, email_config):
        self.master_file_path = master_file_path
        self.watch_directory = watch_directory
        self.check_interval = check_interval
        self.email_config = email_config
        self.master_list = self.load_master_list()
        
    def load_master_list(self):
        return pd.read_csv(self.master_file_path)

    def send_email_alert(self, subject, body, to_email):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['user']
            msg['To'] = to_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))

            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.starttls()
                server.login(self.email_config['user'], self.email_config['password'])
                server.sendmail(self.email_config['user'], to_email, msg.as_string())
            print(f"Email sent to {to_email}")

        except smtplib.SMTPException as e:
            print(f"Failed to send email: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def check_file_arrivals(self):
        now = datetime.now()
        files_in_directory = os.listdir(self.watch_directory)

        for index, row in self.master_list.iterrows():
            expected_time = datetime.strptime(row['expected_time'], '%Y-%m-%d %H:%M:%S')
            threshold = row['threshold']
            file_name = row['file_name']
            stakeholder_email = row['stakeholder_email']
            if file_name not in files_in_directory:
                if now > (expected_time + timedelta(minutes=threshold)):
                    subject = f"Alert: File {file_name} not arrived"
                    body = f"The file {file_name} was expected to arrive by {expected_time} but has not arrived within the threshold of {threshold} minutes."
                    self.send_email_alert(subject, body, stakeholder_email)

    def run(self):
        while True:
            self.check_file_arrivals()
            time.sleep(self.check_interval)

if __name__ == "__main__":
    email_config = {
        'smtp_server': os.getenv('SMTP_SERER'),
        'smtp_port': os.getenv("SMTP_PORT"),
        'user': os.getenv('EMAIL_USER'),
        'password':  os.getenv('EMAIL_PASSWORD')
    }

    monitor = FileMonitor(                      
        master_file_path= os.getenv('MASTER_FILE_PATH'),
        watch_directory=os.getenv('WATCH_DIRECTORY'),
        check_interval=int(os.getenv('CHECK_INTERVAL')),
        email_config=email_config
    )

    monitor.run()
