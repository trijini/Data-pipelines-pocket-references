import psycopg2
import csv
import boto3
import configparser
from pathlib import Path


PARENT_DIR = str(Path(__file__).parents[1]) + '/'
LOCAL_FILENAME = 'customer_extract.csv'
FILE_PATH = PARENT_DIR + 'data/' + LOCAL_FILENAME


class Postgres:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

	    conn = psycopg2.connect(
		    'dbname=' + self.dbname +
		    ' user=' + self.user + 
            ' password=' + self.password + 
            ' host=' + self.host,
		    port=self.port
	    )
    
    def execute(self):    
    	m_query = 'SELECT * FROM Customers;'
        m_cursor = self.conn.cursor()
        m_cursor.execute(m_query)
        results = m_cursor.fetchall()

    	with open(FILE_PATH, 'w') as f:
    		csv_w = csv.writer(f)
    		csv_w.writerows(results)

    	m_cursor.close()
    	self.conn.close()
    

class S3:
    def __init__(self, access_key, secret_key, bucket_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name

    def upload(self):
	    s3 = boto3.client(
		    's3',
		    aws_access_key_id=self.access_key,
		    aws_secret_access_key=self.secret_key
    	)
    	s3_file = LOCAL_FILENAME

    	s3.upload_file(
    		FILE_PATH,
    		self.bucket_name,
    		s3_file
    	)


parser = configparser.ConfigParser()
config_path = PARENT_DIR + 'pipeline.conf'
parser.read(config_path)

dbname = parser.get('postgres_config', 'database')
user = parser.get('postgres_config', 'username')
password = parser.get('postgres_config', 'password')
host = parser.get('postgres_config', 'host')
port = parser.get('postgres_config', 'port')

access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')
