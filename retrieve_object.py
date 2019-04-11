from pprint import pprint
import boto3


REGION_NAME = ''
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''


def init_keys(region, aws_access_key, aws_secret_access_key):
	REGION_NAME = region
	AWS_ACCESS_KEY_ID = aws_access_key
	AWS_SECRET_ACCESS_KEY = aws_secret_access_key



def get_all_records():

	client = boto3.client('s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID , aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
	s3 = boto3.resource('s3', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

	paginator = client.get_paginator('list_objects_v2')

	response_iterator = paginator.paginate(Bucket='road-monitoring-bucket')


	def parser_split(s):
		l = s.replace('"', '').split('}{')
		l = [map(lambda x: float(x), elem.split('::')[1:-1]) for elem in l]
		return l


	all_records = []

	for response in response_iterator:
		print('BEGIN response')
		pprint(response)

		if not u'Contents' in response:
			continue

		for response_obj in response[u'Contents']:
			print('BEGIN OBJ_KEY')
			obj_key = response_obj[u'Key']
			pprint(obj_key)
			print('END OBJ_KEY')

			obj = s3.Object('road-monitoring-bucket', obj_key)

			res = obj.get()
			body = res[u'Body']

			body_str = body.read()
			print(type(body_str))
			print('BEGIN OBJECT CONTENT')

			l = parser_split(body_str)
			pprint(l)
			all_records.extend(l)
			print('END OBJECT CONTENT')

		print('END response')


	pprint(all_records)
	return all_records

