import pymysql
import csv
import boto3
import configparser


# get configurations
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get('mysql_config', 'hostname')
port = parser.get('mysql_config', 'port')
username = parser.get('mysql_config', 'username')
password = parser.get('mysql_config', 'password')
dbname = parser.get('mysql_config', 'database')

# initialize database connection
conn = pymysql.connect(host=hostname,
    user=username,
    password=password,
    db=dbname,
    port=int(port))

if conn is None:
    print('Error connecting to the MySQL database')
else:
    print('MySQL connection established!')

# extraction
m_query = 'SELECT * FROM Orders;'
local_filename = 'data/order_extract.csv'

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as f:
    csv_w = csv.writer(f)
    csv_w.writerows(results)

m_cursor.close()
conn.close()

# aws_boto_credentials
access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

s3 = boto3.client('s3',
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key)
s3_file = local_filename
s3.upload_file(local_filename, bucket_name, s3_file)
