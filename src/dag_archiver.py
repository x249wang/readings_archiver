from datetime import datetime, timedelta, timezone
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from scripts.fetch_articles import fetch_articles
from scripts.extract_contents import extract_contents
from scripts.get_summaries import get_summaries
from scripts.write_to_spreadsheet import write_to_spreadsheet

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import (
    SlackWebhookOperator,
)


SLACK_CONN_ID = "slack"


def get_slack_webhook_token():

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    return slack_webhook_token


def task_fail_slack_alert(context):

    slack_webhook_token = get_slack_webhook_token()

    slack_msg = """
    :red_circle: Task Failed.
    *Task*: {task}
    *Task Instance*: {ti}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_notifier",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return failed_alert.execute(context=context)


def task_success_slack_alert(context):

    slack_webhook_token = get_slack_webhook_token()

    slack_msg = """
    :heavy_check_mark: Task Succeeded.
    *Task*: {task}
    *Task Instance*: {ti}
    *Dag*: {dag}
    *Execution Time*: {exec_date}
    *Log Url*: {log_url}
    """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    success_alert = SlackWebhookOperator(
        task_id="slack_success_notifier",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )

    return success_alert.execute(context=context)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 19),
    "email": ["abc@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert,
}


dag = DAG(
    dag_id="articles_archive",
    default_args=default_args,
    description="DAG for archiving saved articles and summaries",
    schedule_interval="@daily",
)


task_fetch_articles = PythonOperator(
    task_id="fetch_articles",
    dag=dag,
    python_callable=fetch_articles,
    op_kwargs={"all_historical": False},
)

task_extract_content = PythonOperator(
    task_id="extract_contents", dag=dag, python_callable=extract_contents
)

task_get_summaries = PythonOperator(
    task_id="get_summaries", dag=dag, python_callable=get_summaries
)

task_write_to_spreadsheet = PythonOperator(
    task_id="write_to_spreadsheet",
    dag=dag,
    python_callable=write_to_spreadsheet,
)


task_fetch_articles >> task_extract_content >> task_get_summaries >> task_write_to_spreadsheet
