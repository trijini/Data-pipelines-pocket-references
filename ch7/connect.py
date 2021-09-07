import psycopg2
import boto3


class Postgres:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        
        self.conn = psycopg2.connect(
		    'dbname=' + self.dbname +
		    ' user=' + self.user + 
            ' password=' + self.password + 
            ' host=' + self.host,
		    port=self.port
	    )

    def __del__(self):
        self.conn.commit()
        self.conn.close()
    

class S3:
    def __init__(self, access_key, secret_key, bucket_name):
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name

    def upload(self, file_name, file_path):
        s3 = boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
    	)

        s3.upload_file(
    		file_path,
    		self.bucket_name,
    		file_name
    	)
