from configparser import ConfigParser
from pathlib import Path
import csv
from p.connect import Database, S3


class Setup:
    parent_dir = str(Path(__file__).parents[1]) + '/'
    
    def __init__(self, table_name, file_name):
        self.table_name = table_name
        self.file_name = file_name
        self.file_path = Extract.parent_dir + 'data/' + file_name

    def get_config(self, config_name, config_params):
        parser = ConfigParser()
        config_path = Extract.parent_dir + 'pipeline.conf'
        parser.read(config_path)
        config = {x: parser.get(config_name, x) for x in config_params}
        
        return config

    def database(self, config_name, config_params):
        configs = self.get_config(config_name, config_params)
        config_val = [x for x in configs.values()]
        db = Database(*config_val)

        return db

    def s3_bucket(self, config_name, config_params):
        configs = self.get_config(config_name, config_params)
        config_val = [x for x in configs.values()]
        s3 = S3(*config_val)

        return s3


class Extract(Setup):
    def __init__(self, table_name, file_name):
        super().__init__(table_name, file_name)

    def extract(self, config_name, config):   
        db = super().database(config_name, config)

        query = f'SELECT * FROM {self.table_name};'
        m_cursor = db.conn.cursor()
        m_cursor.execute(query)
        results = m_cursor.fetchall()

        with open(self.file_path, 'w') as f:
            csv_w = csv.writer(f)
            csv_w.writerows(results)

        m_cursor.close()

    def stage_to_s3(self, config_name, config):
        s3 = super().s3_bucket(config_name, config)

        s3.s3.upload_file(
            self.file_path,
            s3.bucket_name,
            self.file_name
        )

class Load(Setup):
    def __init__(self, table_name, file_name):
        super().__init__(table_name, file_name)

    def upload(self, config_name, config):
        db = super().database(config_name, config)
        aws = self.get_config('aws_creds', ['iam_role'])
        boto = self.get_config(
            'aws_boto_credentials',
            ['account_id', 'bucket_name']
        )

        s3_file_path = f"s3://{boto['bucket_name']}/{self.file_name}"
        role_string = f"arn:aws:iam::{boto['account_id']}:role/{aws['iam_role']}"

        query = (f"COPY public.{self.table_name} "
                 f"FROM %s iam_role %s DELIMITER ',';")

        cur = db.conn.cursor()
        cur.execute(query, (s3_file_path, role_string))

        cur.close()
 