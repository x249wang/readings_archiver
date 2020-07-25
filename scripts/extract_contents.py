import re
from boilerpy3 import extractors
from unidecode import unidecode
from .utils import Config, connect_to_mongo_collection
from urllib.error import HTTPError
import logging

logger = logging.getLogger("airflow.task")


def extract_raw_content(url):

    extractor = extractors.ArticleExtractor()
    content = extractor.get_content_from_url(url)

    return content


def process_text(raw_text):

    new_text = unidecode(re.sub("\n+", "  ", raw_text))
    new_text = new_text.strip()

    return new_text


def extract_contents():

    collection = connect_to_mongo_collection(
        Config.mongo_uri, Config.mongo_db_name, Config.mongo_collection_name
    )

    records = collection.find({"full_text": {"$exists": False}})

    logger.info(f"{records.count()} articles need full text extraction")

    for record in records:

        try:

            raw_text = extract_raw_content(record["resolved_url"])
            full_text = process_text(raw_text)

            if full_text != "":

                collection.update_one(
                    {"item_id": record["item_id"]},
                    {"$set": {"full_text": full_text}},
                )

                logger.info(
                    f"Added full text of article {record['item_id']} to db"
                )

            else:
                logger.info(f"Empty text for article {record['item_id']}")

        except HTTPError:

            logger.error(
                f"Invalid url {record['resolved_url']}; cannot retrieve webpage content"
            )
            continue

        except Exception as e:

            logger.error(e)
            continue
