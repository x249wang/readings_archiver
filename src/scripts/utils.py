import pymongo
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import boto3
import base64
import json


class Config:

    mongo_uri = "mongodb://10.0.1.224:27017"
    mongo_db_name = "archiver"
    mongo_collection_name = "articles"

    spreadsheet_id = "1ZSXV0L15PnyTvqvwvgKYVDq2C3WTZ-dL5uldpAZYMAs"
    range_name = "articles!A1:H1"


def get_secret(secret_name, region_name):

    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    if "SecretString" in get_secret_value_response:
        secret = get_secret_value_response["SecretString"]
    else:
        secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)


def get_utc_dt(year, month, date):
    return datetime(year, month, date, tzinfo=timezone.utc)


def get_unix_ts(date):
    return date.timestamp()


def connect_to_mongo_collection(conn_str, db_name, collection_name):

    client = pymongo.MongoClient(conn_str)
    db = client[db_name]
    collection = db[collection_name]

    return collection


def authorize_google_api(service_account_info, scopes):

    credentials = Credentials.from_service_account_info(
        service_account_info, scopes=scopes
    )

    return credentials


def construct_gsheet_service(credentials):

    service = build("sheets", "v4", credentials=credentials)
    return service
