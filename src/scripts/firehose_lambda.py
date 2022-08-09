import base64
import os
import json


debug = os.environ.get("debug")
if debug == "False":
    import firehose as fh
else:
    from src.scripts import firehose as fh

print('Loading function')


def lambda_handler(event, context):
    output = []
    print(event)

    for record in event['records']:
        print("loading record")
        print(record['recordId'])
        payload = base64.b64decode(record['data']).decode('utf-8')
        payload = json.loads(payload)
        print(payload)
        print("cleaning record")

        cleaned = fh.main_clean(payload)
        print("cleaned")

        cleaned = json.dumps(cleaned)

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(cleaned.encode('utf-8')).decode('utf-8')
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
