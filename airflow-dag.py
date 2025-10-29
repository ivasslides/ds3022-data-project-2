# airflow DAG goes here
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable 
from datetime import datetime, timedelta
import boto3
import os 

# define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

# create the dag 
dag = DAG(
    # 'nameoffunction', 
    # default_args = default_args, 
    # description = 'Sending sqs messages', 
    # schedule = timedelta(days=1)
    # cathcup = False
)

# define python function to execute
def fetch_sqs_message():
    queue_url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/fbv2sc"
    sqs = boto3.client(
        'sqs', 
        region_name='us-east-1', 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"), 
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    ) 

    # try to get any messages w message attributes from sqs queue
    try: 
        response = sqs.receive_message(
            QueueUrl = queue_url, 
            MessageSystemAttributeNames=['All'], 
            MaxNumberOfMessages = 1, 
            MessageAttributeNames = ['All']
        )

        handle = response['Messages'][0]['ReceiptHandle']
        delete_message(queue_url, handle)


    except Exception as e: 
        print(f"Error getting message: {e}")
        raise e 


# create the task 
fetch_sqs_task = PythonOperator(
    task_id = 'fetch_sqs_message',
    python_callable = fetch_sqs_message, 


)