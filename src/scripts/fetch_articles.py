import requests
from .utils import Config, get_secret, get_unix_ts, connect_to_mongo_collection
from datetime import datetime, timedelta
import logging

logger = logging.getLogger("airflow.task")

RETRIEVE_URL = "https://getpocket.com/v3/get"


def get_pocket_api_access():
    return get_secret("pocket_api_access", "us-east-1")


def generate_query_params(all_historical):

    pocket_api_access = get_pocket_api_access()

    if all_historical:

        return {
            "consumer_key": pocket_api_access["consumer_key"],
            "access_token": pocket_api_access["access_token"],
        }

    yesterday = datetime.now() - timedelta(days=1)
    since_ts = get_unix_ts(yesterday)

    return {
        "consumer_key": pocket_api_access["consumer_key"],
        "access_token": pocket_api_access["access_token"],
        "since": since_ts,
    }


def fetch_articles_info_from_pocket(query_params):

    response = requests.get(RETRIEVE_URL, params=query_params)

    try:
        response.raise_for_status()

    except requests.exceptions.HTTPError as http_err:
        logger.error(http_err)
        return None

    except Exception as err:
        logger.error(err)
        return None

    articles = response.json()["list"]
    logger.info(f"Found {len(articles)} new articles")

    return articles


def fetch_articles(all_historical=False):

    query_params = generate_query_params(all_historical)

    logger.info(
        f"Retrieving newly saved articles since {query_params.get('since', 'the beginning')}"
    )
    articles = fetch_articles_info_from_pocket(query_params)

    if not articles:
        return

    collection = connect_to_mongo_collection(
        Config.mongo_uri, Config.mongo_db_name, Config.mongo_collection_name
    )

    for a_id in articles:

        result = collection.update_one(
            {"item_id": articles[a_id]["item_id"]},
            {"$setOnInsert": articles[a_id]},
            upsert=True,
        )

        if result.modified_count:
            logger.info(f"Added article {articles[a_id]['item_id']} to db")
