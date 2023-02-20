# Jira case: SOD-22 - Automate Brokerage Evaluations
import json
import sys
import warnings
from datetime import date
import pandas
import prefect
import pyodbc
import requests
import time
import boto3
from prefect import Flow, Parameter, task
from contextlib import closing
from prefect.tasks.secrets import PrefectSecret
from prefect.storage import GitHub
from prefect.tasks.aws.secrets_manager import AWSSecretsManager


prefect_secrets = prefect.context.secrets
logger = prefect.context.get("logger")
slack_web_hook = PrefectSecret("SLACK_WEBHOOK_URL").run()
username_prefix = "prefect___"
msg_prefix = "`Run Evaluations by Broker` -  "

retry_frequency = 30
max_retries = 50
inter_broker_delay = 7 #7 second delay






def Sleep(waitSeconds):
    time.sleep(waitSeconds)
    return True


def publish_to_slack(webhook, msg):
    response = ""
    try:
        json_msg = {"text": msg}
        response = requests.post(webhook, json.dumps(json_msg))
        logger.info(msg)
    except BaseException as e:
        logger.warning("Unable to post message to Slack: " + e.__str__())
    return response


@task
def iterate_brokers(broker_list):
    for broker in broker_list:
        (brokerage_id, brokerage_name) = broker
        logger.info(f"Begin Processing of {brokerage_name} with ID: {brokerage_id}")
        Sleep(inter_broker_delay)
    return result


@task
def log_broker_list(broker_list):
    publish_to_slack(webhook=slack_web_hook, msg=broker_list)


with Flow("Brokerage Evaluation") as flow:
    flow.storage = GitHub(repo='hedgeco/smartx-prefect-flows', path="run_evaluations_by_broker.py",
                            access_token_secret="GITHUB_ACCESS_TOKEN")
    broker_list = Parameter("broker_list", default=None)
    evaluation_date = date.today().strftime('%m/%d/%Y')
    greeting = publish_to_slack(webhook=slack_web_hook, msg="Beginning Run - Evaluate Brokerages.")
    export_list = log_broker_list(broker_list)

    result = iterate_brokers(broker_list)

    #Dependencies
    export_list.set_upstream(greeting)
    result.set_upstream(export_list)



if __name__ == "__main__":
    flow.register(project_name="SOD-Testing", labels=["Run Evaluations by Broker"])
    broker_list = [
                    ('f1df79e0-b156-11ed-afa1-0242ac120002', 'Apple'),
                    ('f1df7ecc-b156-11ed-afa1-0242ac120002', 'Donkey'),
                    ('f1df8098-b156-11ed-afa1-0242ac120002', 'Penguin'),
                  ]

    flow.run(parameters={"broker_list": broker_list})