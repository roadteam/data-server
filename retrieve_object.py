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

	response_iterator = paginator.paginate(Bucket='road-monitoring-data')


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

			obj = s3.Object('road-monitoring-data', obj_key)

			res = obj.get()
			body = res[u'Body']

			body_str = body.read()
			print(type(body_str))
			print('BEGIN OBJECT CONTENT')
	#		print(body_str)
	#		for c in body_str:
	#			print(c)
			l = parser_split(body_str)
			pprint(l)
			all_records.extend(l)
			print('END OBJECT CONTENT')

		print('END response')


	pprint(all_records)
	return all_records

'''
s3 = boto3.resource('s3')
>>> ob = s3.Object('road-monitoring-data', u'2019/03/27/14/road-data-stream-1-2019-03-27-14-10-29-c40242f9-6de7-435e-99b3-0e3192ba1171')
'''
'''
{u'Contents': [{u'ETag': '"ee9f8a8d64653932ebb75d3fd52a1de7"',
                u'Key': u'2019/03/27/14/road-data-stream-1-2019-03-27-14-10-29-c40242f9-6de7-435e-99b3-0e3192ba1171',
                u'LastModified': datetime.datetime(2019, 3, 27, 14, 11, 31, tzinfo=tzlocal()),
                u'Size': 2891,
                u'StorageClass': 'STANDARD'}],
 u'EncodingType': 'url',
 u'IsTruncated': False,
 u'KeyCount': 1,
 u'MaxKeys': 1000,
 u'Name': 'road-monitoring-data',
 u'Prefix': u'',
 'ResponseMetadata': {'HTTPHeaders': {'content-type': 'application/xml',
                                      'date': 'Wed, 27 Mar 2019 15:12:28 GMT',
                                      'server': 'AmazonS3',
                                      'transfer-encoding': 'chunked',
                                      'x-amz-bucket-region': 'eu-west-1',
                                      'x-amz-id-2': 'j8g8NRy1Ll0474VDoqYz20rPh6QYuraVkKOfnWXMBWriGDJE/av/xwBYQ1MeHgHnmZgnl63KrZo=',
                                      'x-amz-request-id': '8256F5999CB0C3D1'},
                      'HTTPStatusCode': 200,
                      'HostId': 'j8g8NRy1Ll0474VDoqYz20rPh6QYuraVkKOfnWXMBWriGDJE/av/xwBYQ1MeHgHnmZgnl63KrZo=',
                      'RequestId': '8256F5999CB0C3D1',
                      'RetryAttempts': 0}}

'''

