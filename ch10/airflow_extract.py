import csv
import boto3
import psycopg2
from connect import Psycopg2, S3


# redshift
aws_config = Psycopg2('aws_creds')
rs_conn = aws_config.connect()
rs_cursor = rs_conn.cursor()

rs_sql = '''SELECT COALESCE(MAX(id), -1)
            FROM dag_run_history;'''
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

last_id = result[0]
rs_cursor.close()
rs_conn.commit()

# airflow
af_config = Psycopg2('airflowdb_config')
af_conn = af_config.connect()
af_cursor = af_conn.cursor()

af_query = '''SELECT
                id,
                dag_id,
                execution_date,
                state,
                run_id,
                external_trigger,
                end_date,
                start_date
            FROM dag_run
            WHERE id > %s
            AND state <> \'running\';
            '''

af_cursor.execute(m_query, (last_id))
results = af_cursor.fetchall()

local_filename = "dag_run_extract.csv"

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)

fp.close()
af_cursor.close()
af_conn.close()

# s3
s3_config = S3('aws_boto_credentials')
s3 = s3_config.connect()
bucket_name = s3_config.get_config()['bucket_name']
s3_file = local_filename
s3.upload_file(local_filename, bucket_name, s3_file)
