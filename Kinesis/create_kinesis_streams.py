import requests
import json

# create streams
def create_stream(suffix):
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "StreamName": f"streaming-0a1153066525-{suffix}"
        })
    invoke_url = f"https://dc6k35vnyj.execute-api.us-east-1.amazonaws.com/staging/streams/streaming-0a1153066525-{suffix}"
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    print(response)
    print(response.content)

create_stream("pin")
create_stream("geo")
create_stream("user")

print("note it will take a few seconds for the streams to be created and ready for posting data")
