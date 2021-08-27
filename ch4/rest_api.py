import requests
import json
import configparser
import csv
import boto3


lat = 42.36
lon = 71.05

lat_log_params = {"lat": lat, "lon": lon}
url = 'http://api.open-notify.org/iss-pass.json'
api_response = requests.get(url, params=lat_log_params)
response_json = json.loads(api_response.content)
all_passes = []

for response in response_json['response']:
	current_pass = []

	current_pass.append(lat)
	current_pass.append(lon)

	current_pass.append(response['duration'])
	current_pass.append(response['risetime'])

	all_passes.append(current_pass)

export_file = 'data/export_file.csv'

with open(export_file, 'w') as f:
	csvw = csv.writer(f)
	csvw.writerows(all_passes)

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

s3 = boto3.client(
	's3',
	aws_access_key_id=access_key,
	aws_secret_access_key=secret_key
)
s3.upload_file(
	export_file,
	bucket_name,
	export_file
)