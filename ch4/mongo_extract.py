from pymongo import MongoClient
import csv
import boto3
from datetime import datetime
from datetime import timedelta
import configparser


# load mongoDB configurations
parser = configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get('mongo_config', 'hostname')
username = parser.get('mongo_config', 'username')
password = parser.get('mongo_config', 'password')
database_name = parser.get('mongo_config', 'database')
collection_name = parser.get('mongo_config', 'collection')


# initialize connection
mongo_client = MongoClient(
	'mongodb+srv://' + username +
	':' + password +
	'@' + hostname +
	'/' + database_name +
	'?retryWrites=true&' +
	'w=majority&ssl=true&' +
	'ssl_cert_reqs=CERT_NONE'
)

mongo_db = mongo_client[database_name]
mongo_collection = mongo_db[collection_name]

start_date = datetime.today() + timedelta(days=-1)
end_date = start_date + timedelta(days=1)

mongo_query = {
	'$and': [{
			'event_timestamp': {
				'$gte': start_date
			}
		},
		{
			'event_timestamp': {
				'$lt': end_date
			}
	}]
}

event_docs = mongo_collection.find(mongo_query, batch_size=3000)

all_events = []

for doc in event_docs:
	event_id = str(doc.get('event_id', -1))
	event_timestamp = doc.get('event_timestamp', None)
	event_name = doc.get('event_name', None)

	current_event = []
	current_event.append(event_id)
	current_event.append(event_timestamp)
	current_event.append(event_name)

	all_events.append(current_event)

	export_file = 'data/export_file.csv'

with open(export_file, 'w') as f:
	csvw = csv.writer(f)
	csvw.writerows(all_events)

access_key = parser.get('aws_boto_credentials', 'access_key')
secret_key = parser.get('aws_boto_credentials', 'secret_key')
bucket_name = parser.get('aws_boto_credentials', 'bucket_name')

s3 = boto3.client('s3',
	aws_access_key_id=access_key,
	aws_secret_access_key=secret_key
)
s3_file = export_file
s3.upload_file(export_file, bucket_name, s3_file)
