import json
import urllib
import boto3
import gzip
import os

from dateutil.parser import parse

print("Loading function")

s3 = boto3.client("s3")
fields_prefix = "#Fields: "


# Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields c-port time-to-first-byte x-edge-detailed-result-type sc-content-type sc-content-len sc-range-start sc-range-end
def log_to_event(log):
    if not "time" in log:
        return

    d = log["date"] if "date" in log else ""
    t = log["time"] if "time" in log else ""

    dt = parse(d + " " + t)
    ev = {
        "_time": int(dt.timestamp() * 1e9),
        "location": log["x-edge-location"] if log["x-edge-location"] != "-" else None,
        "bytes": log["sc-bytes"] if log["sc-bytes"] != "-" else None,
        "request_ip": log["c-ip"] if log["c-ip"] != "-" else None,
        "method": log["cs-method"]  if log["cs-method"] != "-" else None,
        "host": log["cs(Host)"] if log["cs(Host)"] != "-" else None,
        "uri": log["cs-uri-stem"] if log["cs-uri-stem"] != "-" else None,
        "status": log["sc-status"]  if log["sc-status"] != "-" else None,
        "referrer": log["cs(Referer)"] if log["cs(Referer)"] != "-" else None,
        "user_agent": log["cs(User-Agent)"] if log["cs(User-Agent)"] != "-" else None,
        "query_string": log["cs-uri-query"] if log["cs-uri-query"] != "-" else None,
        "cookie": log["cs(Cookie)"] if log["cs(Cookie)"] != "-" else None,
        "result_type": log["x-edge-result-type"] if log["x-edge-result-type"] != "-" else None,
        "request_id": log["x-edge-request-id"] if log["x-edge-request-id"] != "-" else None,
        "host_header": log["x-host-header"] if log["x-host-header"] != "-" else None,
        "request_protocol": log["cs-protocol"] if log["cs-protocol"] != "-" else None,
        "request_bytes": log["cs-bytes"] if log["cs-bytes"] != "-" else None,
        "time_taken": log["time-taken"] if log["time-taken"] != "-" else None,
        "x_forwarded_for": log["x-forwarded-for"] if log["x-forwarded-for"] != "-" else None,
        "ssl_protocol": log["ssl-protocol"] if log["ssl-protocol"] != "-" else None,
        "ssl_cipher": log["ssl-cipher"] if log["ssl-cipher"] != "-" else None,
        "response_result_type": log["x-edge-response-result-type"] if log["x-edge-response-result-type"] != "-" else None,
        "http_version": log["cs-protocol-version"] if log["cs-protocol-version"] != "-" else None,
        "fle_status": log["fle-status"] if log["fle-status"] != "-" else None,
        "fle_encrypted_fields": log["fle-encrypted-fields"] if log["fle-encrypted-fields"] != "-" else None,
        "c_port": log["c-port"] if log["c-port"] != "-" else None,
        "time_to_first_byte": log["time-to-first-byte"] if log["time-to-first-byte"] != "-" else None,
        "x_edge_detailed_result_type": log["x-edge-detailed-result-type"] if log["x-edge-detailed-result-type"] != "-" else None,
        "content_type": log["sc_content-type"] if log["sc-content-type"] != "-" else None,
        "content_len": log["sc-content-len"] if log["sc-content-len"] != "-" else None,
        "range_start": log["sc-range-start"] if log["sc-range-start"] != "-" else None,
        "range_end": log["sc-range-end"] if log["sc-range-end"] != "-" else None,
    }
    return ev


def lambda_handler(event, context):
    for record in event["Records"]:
        # Get the object from the event and show its content type
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"], encoding="utf-8")
        try:
            # get and decompress object
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()
            decompressed_body = gzip.decompress(body)

            # parse TSV
            lines = str(decompressed_body, "utf-8").split("\n")
            columns = []
            events = []
            for line in lines:
                if line.startswith(fields_prefix):
                    columns = line[len(fields_prefix) :].split(" ")
                    continue
                elif line.startswith("#"):
                    continue

                values = line.split("\t")
                ev = log_to_event(dict(zip(columns, values)))
                if ev != None:
                    events.append(ev)

            # send to Axiom
            axiom_url = os.getenv("AXIOM_URL")
            if axiom_url == None:
                axiom_url = "https://cloud.axiom.co"
            axiom_token = os.getenv("AXIOM_TOKEN")
            axiom_org_id = os.getenv("AXIOM_ORG_ID")
            axiom_dataset = os.getenv("AXIOM_DATASET")

            url = f"{axiom_url}/api/v1/datasets/{axiom_dataset}/ingest"
            data = json.dumps(events)
            req = urllib.request.Request(
                url,
                data=bytes(data, "utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "X-Axiom-Org-Id": axiom_org_id,
                    "Authorization": f"Bearer {axiom_token}",
                },
            )
            result = urllib.request.urlopen(req)

            if result.status != 200:
                raise f"Unexpected status {result.status}"
            else:
                print(f"Ingested {len(events)} events")
        except Exception as e:
            print(e)
            print(
                "Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.".format(
                    key, bucket
                )
            )
            raise e
