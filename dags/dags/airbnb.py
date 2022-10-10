import pendulum
from contextvars import Context

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from constants.webhook import SLACK_CONNECTION_ID, SLACK_WEBHOOK_DAILY_BATCH_BOT
from constants.dag_id import AIRBNB as DAG_ID

from operators.airbnb_sourcing import AirbnbSourcingOperator

SLACK_SUCCESS_NOTIFICATION_TASK_ID = "slack_success_notification_task_id"


def notify_success(context: Context):
    message = f""":large_green_circle: dag <{DAG_ID}> ran successfully!"""

    slack_success_notification_task = SlackWebhookOperator(
        task_id=SLACK_SUCCESS_NOTIFICATION_TASK_ID,
        http_conn_id=SLACK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_DAILY_BATCH_BOT,
        message=message,
    )
    return slack_success_notification_task.execute(context)


def notify_failure(context: Context):
    message = f":exclamation: dag <{DAG_ID}> failed"

    slack_failure_notification_task = SlackWebhookOperator(
        task_id=SLACK_SUCCESS_NOTIFICATION_TASK_ID,
        http_conn_id=SLACK_CONNECTION_ID,
        webhook_token=SLACK_WEBHOOK_DAILY_BATCH_BOT,
        message=message,
    )
    return slack_failure_notification_task.execute(context)

with DAG(
        dag_id=DAG_ID,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval="@daily",
        tags=["main"],
        on_failure_callback=notify_failure
) as dag:
    airbnb_task = AirbnbSourcingOperator(
        task_id="airbnb_task",
        on_success_callback=notify_success
    )
