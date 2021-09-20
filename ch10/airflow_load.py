import boto3
import psycopg2
from connect import Psycopg2, S3


rs_config = Psycopg2('aws_creds')
iam_role = rs_config.get_config()['iam_role']
rs_conn = rs_config.connect()

s3_config = S3('aws_boto_credentials')
bucket_name = s3_config.get_config()['bucket_name']
account_id = s3_config.get_config()['account_id']

file_path = f's3://{bucket_name}/dag_run_extract.csv'

sql = """COPY dag_run_history
        (id, dag_id, execution_date, state, run_id, external_trigger,
        end_date, start_date)"""
sql = sql + " from %s "
sql = sql + " iam_role 'arn:aws:iam:%s:role/$s';"

cur = rs_conn.cursor()
cur.execute(sql, (file_path, account_id, iam_role))

cur.close()
rs_conn.commit()
rs_conn.close()
