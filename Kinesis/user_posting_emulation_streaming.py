import requests
from time import sleep
import random
from multiprocessing import Process
# import boto3
import json
import sqlalchemy
from sqlalchemy import text
# import pymysql

random.seed(100)


class AWSDBConnector:
    def __init__(self):
        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()


def post_data_to_stream_api(data, suffix):
    aws_user = '0a1153066525'
    headers = {'Content-Type': 'application/json'}
    payload = json.dumps({
        "StreamName": f"streaming-{aws_user}-{suffix}",
        "Data": data,
                "PartitionKey": "partition-1"
                })
    # invoke url for one record, if you want to put more records replace record with records
    invoke_url = f"https://dc6k35vnyj.execute-api.us-east-1.amazonaws.com/staging/streams/streaming-{aws_user}-{suffix}/record"
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    print(response)
    print(response.content)

def serialise_datetime(data, datetime_field):
    if datetime_field in data:
        data[datetime_field] = data[datetime_field].isoformat()
    else:
        print(f"warning: {datetime_field} (datetime) not found in data")

def run_infinite_post_data_loop(max_count = -1):
    count = 0
    while max_count<0 or count < max_count:
        sleep(random.randrange(0, 1))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(pin_result)
            post_data_to_stream_api(pin_result,'pin')
            print('')

            print(geo_result)
            serialise_datetime(geo_result, 'timestamp')
            print(geo_result)
            post_data_to_stream_api(geo_result,'geo')
            print('')

            print(user_result)
            serialise_datetime(user_result, 'date_joined')
            post_data_to_stream_api(user_result,'user')
            print('-----------------------')

            count = count + 1

def dump_data_pin(max_count = -1):
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        pin_string = text("SELECT * FROM pinterest_data")
        pin_rows = connection.execute(pin_string)
        
        for row in pin_rows:
            pin_result = dict(row._mapping)
            post_data_to_stream_api(pin_result,'pin')
            print(row)


def dump_data_geo(max_count = -1):
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        pin_string = text("SELECT * FROM geolocation_data")
        pin_rows = connection.execute(pin_string)
        
        for row in pin_rows:
            geo_result = dict(row._mapping)
            serialise_datetime(geo_result, 'timestamp')
            post_data_to_stream_api(geo_result,'geo')
            print(row)

def dump_data_user(max_count = -1):
    engine = new_connector.create_db_connector()

    with engine.connect() as connection:
        pin_string = text("SELECT * FROM user_data")
        pin_rows = connection.execute(pin_string)
        
        for row in pin_rows:
            user_result = dict(row._mapping)
            serialise_datetime(user_result, 'date_joined')
            post_data_to_stream_api(user_result,'user')
            print(row)


if __name__ == "__main__":
    print('Working')
    # dump_data_pin()
    # dump_data_geo()
    # dump_data_user()
    run_infinite_post_data_loop()
