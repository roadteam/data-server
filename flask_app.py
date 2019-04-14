import boto3
from pprint import pprint
from base64 import b64encode, b64encode
from flask import Flask, request, jsonify, send_from_directory
app = Flask(__name__)

from retrieve_object import get_all_records, init_keys


REGION_NAME = 'your region'
AWS_ACCESS_KEY_ID = 'your aws key id'
AWS_SECRET_ACCESS_KEY = 'your aws secret key id'


client = boto3.client('firehose', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


@app.route('/')
def redirect_to_map():
    return get_map()


@app.route('/road-data-parsed')
def tmp():
    return open('results.json').read()

@app.route('/anomalies_map')
def get_map():
	return open('data-visualization/anomalies_map.html').read()


@app.route('/data/<path:path>')
def get_data(path):
    return send_from_directory('data-visualization/data', path)

@app.route('/images/<path:path>')
def get_images(path):
    return send_from_directory('data-visualization/images', path)


@app.route('/road-data', methods=['GET', 'PUT'])
def data_handler():
    if request.method == 'GET':
        print('GETting')
        return jsonify(get_all_records())

    print('PUTting')
    # FORMAT = "x;y;z;lon;lat;alt"
    req_json = request.get_json(force=True)
    if req_json == None:
        return 'bad request body'

    print('received request:')
    pprint(req_json)

    payload = req_json['payload_raw']
    print('payload extracted', payload)

    payload = payload.split(';')
    return 'ok'

    # parse the request to give to kinesis
    def stringify_record(r):
        return '::'+str(r[0])+'::'+str(r[1])+'::'+str(r[2])+'::'+str(r[3])+'::'+str(r[4])+'::'+str(r[5])
    req_parsed = [{'Data': stringify_record(r)} for r in payload]
    print('parsed request')
    pprint(req_parsed)
	
    '''format: Records=[
        {
	    'Data': b'::12345::67.89::01.23::4.5677::'
        },
        {
            'Data': b'::12432::78.23::64.56::2.3525::'
        }
      ]'''


    # convert str to bytes for every record
    for elem in req_parsed:
        if type(elem['Data']) == str:
            elem['Data'] = elem['Data'].encode()

    response = client.put_record_batch(
        DeliveryStreamName='road-data-stream',
        Records=req_parsed)

    pprint(response)

    return 'sent'



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=False)


