import torch
from transformers import AutoModelWithLMHead, AutoTokenizer
from .utils import Config, connect_to_mongo_collection
import logging

logger = logging.getLogger("airflow.task")

# Reference: https://github.com/huggingface/transformers/blob/master/examples/summarization/evaluate_cnn.py


class Summarizer(object):
    def __init__(
        self, pretrained_model_path="facebook/bart-large-cnn", max_length=1024
    ):

        self.pretrained_model_path = pretrained_model_path
        self.max_length = max_length

        self.tokenizer = AutoTokenizer.from_pretrained(pretrained_model_path)
        self.model = AutoModelWithLMHead.from_pretrained(pretrained_model_path)

        task_specific_params = self.model.config.task_specific_params
        if task_specific_params is not None:
            self.model.config.update(
                task_specific_params.get("summarization", {})
            )

    def encode_from_string(self, text):

        token_ids = self.tokenizer.batch_encode_plus(
            [text], return_tensors="pt"
        )["input_ids"]

        token_ids = token_ids[token_ids != 50264].reshape(1, -1)
        return token_ids

    def decode_from_token_ids(self, token_ids):

        text_decoded = self.tokenizer.decode(
            token_ids, skip_special_tokens=True
        )

        return text_decoded

    def split_batches(self, token_ids):

        truncated_length = max(
            min(self.max_length, token_ids.shape[0]),
            (token_ids.shape[1] // self.max_length) * self.max_length,
        )

        token_ids_truncated = token_ids[:, :truncated_length].reshape(
            -1, min(truncated_length, self.max_length)
        )

        remainder_length = token_ids.shape[1] - truncated_length

        if remainder_length:
            token_ids_remainder = token_ids[:, -remainder_length:]
            return (token_ids_truncated, token_ids_remainder)

        return (token_ids_truncated, None)

    def generate_summary_from_token_ids(self, token_ids):

        summary_ids = self.model.generate(
            token_ids,
            num_beams=4,
            length_penalty=2.0,
            min_len=25,
            no_repeat_ngram_size=3,
        )

        return summary_ids

    def generate_article_summary(self, text):

        article_input_ids = self.encode_from_string(text)

        truncated_input, remainder_input = self.split_batches(
            article_input_ids
        )

        summary = ""

        summary_ids = self.generate_summary_from_token_ids(truncated_input)

        for ii in range(summary_ids.size()[0]):
            summary += self.decode_from_token_ids(summary_ids[ii, :]) + " "

        if remainder_input is not None:

            summary_ids = self.generate_summary_from_token_ids(remainder_input)
            summary += self.decode_from_token_ids(summary_ids.squeeze())

        return summary.strip()


def get_summaries():

    collection = connect_to_mongo_collection(
        Config.mongo_uri, Config.mongo_db_name, Config.mongo_collection_name
    )

    summarizer = Summarizer()
    logger.info(f"Loaded model {summarizer.pretrained_model_path}")

    records = collection.find(
        {"summary_text": {"$exists": False}, "full_text": {"$exists": True}}
    )

    logger.info(f"{records.count()} articles need summarizing")

    for record in records:

        summary = summarizer.generate_article_summary(record["full_text"])

        collection.update_one(
            {"item_id": record["item_id"]}, {"$set": {"summary_text": summary}}
        )

        logger.info(f"Added summary of article {record['item_id']} to db")
