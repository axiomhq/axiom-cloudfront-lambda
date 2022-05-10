import json
import urllib
import boto3
import gzip
import os

print('Loading function')

s3 = boto3.client('s3')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        # get and decompress object
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        decompressed_body = gzip.decompress(body)

        # parse TSV
        lines = decompressed_body.split('\n')
        columns = []
        events = []
        for i, line in enumerate(lines):
            values = line.split('\t')

            if i == 0:
                columns = values
            else:
                event = {}
                for j, value in enumerate(values):
                    event[columns[j]] = value

        # send to Axiom
        axiom_url = os.getenv("AXIOM_URL")
        if axiom_url == None:
            axiom_url = "https://cloud.axiom.co"
        axiom_token = os.getenv("AXIOM_TOKEN")
        axiom_org_id = os.getenv("AXIOM_ORG_ID")
        axiom_dataset = os.getenv("AXIOM_DATASET")

        url = f"{axiom_url}/api/v1/datasets/{axiom_dataset}/ingest"
        data = json.dumps(events)
        result = urllib.request.urlopen(url, data=data, headers={
            'X-Axiom-Org-Id': axiom_org_id,
            "Authorization": f"Bearer {axiom_token}",
        })

        if result.status != 200:
            raise f"Unexpected status {result.status}"
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e