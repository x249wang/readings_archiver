from datetime import datetime
from .utils import (
    Config,
    get_secret,
    authorize_google_api,
    construct_gsheet_service,
    get_unix_ts,
    connect_to_mongo_collection,
)
import logging

logger = logging.getLogger("airflow.task")

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

ATTRIBUTES = [
    "item_id",
    "resolved_url",
    "resolved_title",
    "time_added",
    "word_count",
    "excerpt",
    "full_text",
    "summary_text",
]


def get_gsheet_api_access():
    return get_secret("gsheet_api_access", "us-east-1")


def write_to_spreadsheet():

    collection = connect_to_mongo_collection(
        Config.mongo_uri, Config.mongo_db_name, Config.mongo_collection_name
    )

    gsheet_api_access = get_gsheet_api_access()

    credentials = authorize_google_api(gsheet_api_access, SCOPES)
    service = construct_gsheet_service(credentials)

    logger.info("Connected to gsheet service")

    records = collection.find({"recorded_ts": {"$exists": False}})

    logger.info(
        f"{records.count()} articles need to be recorded to Google Doc"
    )

    if records.count() > 0:

        body = {
            "values": [
                [record.get(k, "") for k in ATTRIBUTES] for record in records
            ]
        }

        append_result = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=Config.spreadsheet_id,
                range=Config.range_name,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )

        logger.info(
            "{0} cells added".format(
                append_result.get("updates").get("updatedCells")
            )
        )

        recorded_ts = get_unix_ts(datetime.utcnow())

        records = collection.find({"recorded_ts": {"$exists": False}})

        for record in records:

            collection.update_one(
                {"item_id": record["item_id"]},
                {"$set": {"recorded_ts": recorded_ts}},
            )

            logger.info(
                f"Added timestamp of recording for article {record['item_id']} to db"
            )
