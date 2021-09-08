import psycopg2
import boto3


class Database:
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
        self.bucket_name = bucket_name
        
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
    	)
