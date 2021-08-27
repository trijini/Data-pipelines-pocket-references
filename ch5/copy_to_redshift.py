import boto3
import configparser
import psycopg2


parser = configparser.ConfigParser()
parser.read('pipeline.conf')
dbname = parser.get('aws_creds', 'database')
user = parser.get('aws_creds', 'username')
password = parser.get('aws_creds', 'password')
host = parser.get('aws_creds', 'host')
port = parser.get('aws_creds', 'port')

rs_conn = psycopg2.connect(
	"dbname=" + dbname
	+ " user=" + user
	+ " password=" + password
	+ " host=" + host
	+ " port=" + port
)

iam_role = parser.get('aws_creds', 'iam_role')
account_id = parser.get('aws_boto_credentials', 'account_id')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

file_path = ('s3://'+bucket_name+'/order_extract.csv')
role_string = ('arn:aws:iam::'+account_id+':role/'+iam_role)

sql = "COPY public.Orders"
sql = sql + " FROM %s"
sql = sql + " iam_role %s DELIMITER ',';"

cur = rs_conn.cursor()
cur.execute(sql, (file_path, role_string))

cur.close()
rs_conn.commit()
rs_conn.close()
