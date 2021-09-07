from configparser import ConfigParser
from pathlib import Path
import csv
from p.connect import Postgres


class Setup:
    parent_dir = str(Path(__file__).parents[1]) + '/'
    
    def __init__(self, table_name, file_name):
        self.table_name = table_name
        self.file_name = file_name
        self.file_path = Extract.parent_dir + 'data/' + file_name

    def get_config(self, config_name, args_list):
        parser = ConfigParser()
        config_path = Extract.parent_dir + 'pipeline.conf'
        parser.read(config_path)
        config = {x: parser.get(config_name, x) for x in args_list}
        
        return config

    def database(self):
        postgres_config = ['database', 'username', 'password', 'host', 'port']
        postgres = self.get_config('postgres_config', postgres_config)

        db = Postgres(
            postgres['database'], postgres['username'], postgres['password'],
            postgres['host'], postgres['port'],
        )

        return db


class Extract(Setup):
    def __init__(self, table_name, file_name):
        super().__init__(table_name, file_name)

    def extract(self):   
        db = super().database()

        m_query = f'SELECT * FROM {self.table_name};'
        m_cursor = db.conn.cursor()
        m_cursor.execute(m_query)
        results = m_cursor.fetchall()

        with open(self.file_path, 'w') as f:
            csv_w = csv.writer(f)
            csv_w.writerows(results)

        m_cursor.close()


class Copy(Setup):
    def __init__(self, table_name, file_name):
        super().__init__(table_name, file_name)

    def upload(self):
        db = super().database()
        m_query = f"copy {self.table_name} from '{self.file_path}' delimiter ','"
        m_cursor = db.conn.cursor()
        m_cursor.execute(m_query)

        m_cursor.close()

