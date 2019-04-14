
import json
from base64 import b64encode, b64decode

def lambda_handler(event, context):
    
    output = []
    
    for record in event['records']:
        
        
        payload = b64decode(record['data']).decode()
        
        to_discard = is_to_discard(payload)
        
        if is_to_discard:
            response_record = {
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': b64encode('out of bounds'.encode('utf-8')).decode('utf-8')
            }
            
        
        else:
            data_out = {'payload': payload, 'res': 'filtered'}
            payload_out = json.dumps(data_out)
            
            response_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': b64encode(payload_out.encode('utf-8')).decode('utf-8')
            }

        output.append(response_record)


     
    return {'records': output}



# Check whether or not to discard a received record
def is_to_discard(payload):
    tokens = payload.split(';')
    if len(tokens) != 6:
        return True
    lat, lon = tokens[3], tokens[4]
    return not _is_inside_rome(lat, lon)
    
    
# Check whether the coordinates 'lat' 'lon' are inside Rome
def _is_inside_rome(lat, lon):
    in_lat = 41.979354 > lat > 41.794818
    in_lon = 12.610463 > lon > 12.350225
    return in_lat and in_lon
    
    
