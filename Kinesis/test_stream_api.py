import requests
import json


# test streams
def test_stream(suffix):
    aws_user = '0a1153066525'
    example_df = {"index": 1, "name": "Dan", "age": 35, "role": "engineer"}

    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "StreamName": f"streaming-{aws_user}-{suffix}",
        "Data":
                example_df,
                # {
                # "index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]
                # },
                "PartitionKey": "partition-1"
                })
    print(payload)
    # invoke url for one record, if you want to put more records replace record with records
    invoke_url = f"https://dc6k35vnyj.execute-api.us-east-1.amazonaws.com/staging/streams/streaming-{aws_user}-{suffix}/record"
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response)
    print(response.content)

test_stream("pin")
test_stream("geo")
test_stream("user")
