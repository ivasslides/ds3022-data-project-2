from airflow import DAG
from airflow.operators.python import Python Operator
from airflow.models import Variable 
from datetime import datetime, timedelta
import requests
import boto3
import os
import time 

# define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=2),
}

# function 1 
def get_messages():
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/fbv2sc"
    sqs = boto3.client(
        'sqs', 
        region_name='us-east-1', 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"), 
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )
    try:
        response = requests.post(url) 
        payload = response.json() 
        print(f"raw PAYLOAD content: {payload}")
        return payload['sqs_url']
    
    except Exception as e:
        print(f"Error reaching API: {e}")
        raise e 

# function 2 
def monitoring(**kwargs):
    sqs = boto3.client(
        'sqs', 
        region_name='us-east-1', 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"), 
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )

    # pull output from get_messages using kwargs
    ti = kwargs['ti']
    queue_url = ti.xcom_pull(task_ids='get_messages', key='return_value')
    
    received_messages = {}
    count = 0

    try: 
        while count < 21:
            print(f"Count: {count}")
            response = sqs.get_queue_attributes(
                QueueUrl = queue_url, 
                AttributeNames = ['ApproximateNumberOfMessages', 
                                'ApproximateNumberOfMessagesNotVisible', 
                                'ApproximateNumberOfMessagesDelayed'],
            )

            avail_msgs = int(response['Attributes']['ApproximateNumberOfMessages'])
            delayed_msgs = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
            not_vis_msgs = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

            print(f"Available messages: {avail_msgs}, | Not visible: {not_vis_msgs} | Delayed: {delayed_msgs}")

             # if ApprxNumMes > 0, receive  
            if avail_msgs > 0:
                resp = sqs.receive_message(
                    QueueUrl=queue_url,
                    MessageSystemAttributeNames=['All'],
                    MaxNumberOfMessages=1, 
                    MessageAttributeNames=['All'],
                    WaitTimeSeconds=10
                )

                if 'Messages' in resp:
                    for msg in resp['Messages']:
                        # get number, word, and receipt handle
                        number = int(msg['MessageAttributes']['order_no']['StringValue'])
                        word = msg['MessageAttributes']['word']['StringValue']
                        handle = msg['ReceiptHandle']

                        # add message to dictionary
                        received_messages[number] = word

                        # delete message after storing
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=handle)

                        # increment count to keep track of messages stored and deleted
                        count += 1 
            # wait minimum amount of time before checking for new messages 
            else:
                print("No messages yet - now waiting.")
                time.sleep(30)

        print(f"All messages received. Count is {count}")   
        return received_messages 

    except Exception as e:
        print(f"Error collecting messages: {e}")
        raise e 

# function 3 
def arrange_messages(**kwargs):
    sqs = boto3.client(
        'sqs', 
        region_name='us-east-1', 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"), 
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )

    # pull output from get_messages using kwargs
    ti = kwargs['ti']
    received_messages = ti.xcom_pull(task_ids='monitoring', key='return_value')

    sorted_msg = sorted(received_messages.items())
    ordered_words = [word for _, word in sorted_msg]
    phrase = " ".join(ordered_words)
    print(f"PHRASE: {phrase}")
    return phrase 

# function 4 
def send_solution(**kwargs, uvaid='fbv2sc', platform='airflow'):
    sqs = boto3.client(
        'sqs', 
        region_name='us-east-1', 
        aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID"), 
        aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    )
    # pull output for phrase from arrange_messages 
    ti = kwargs['ti']
    phraes = ti.xcom_pull(task_ids='arrange_messages', key='return_value')
    
    # url to send messages to
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try:
        response = sqs.send_message(
            QueueUrl = submit_url, 
            MessageBody='solution', 
            MessageAttributes = {
                'uvaid': {
                    'DataType': 'String',  
                    'StringValue': uvaid 
                }, 
                'phrase': {
                    'DataType': 'String', 
                    'StringValue': phrase
                }, 
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        print("Messages SENT")

    except Exception as e:
        print(f"Error sending message: {e}")
        raise e 

# define the DAG
with DAG(
    # 'nameofdag
    default_args = default_args, 
    description = 'Retrieve, parse, arrange, and send SQS messages', 
    schedule = timedelta(days=1), 
    catchup = False,
) as dag: 
    # task 1, which is function 1
    task_1 = PythonOperator(
        task_id = 'get_messages', 
        python_callable = get_messages, 
    )

    # task 2, which is function 2
    task_2 = PythonOperator(
        task_id = 'monitoring', 
        python_callable = monitoring, 
    )

    # task 3, which is function 3
    task_3 = PythonOperator(
        task_id = 'arrange_messages',
        python_callable = arrange_messages,
    )

    # task 4, which is function 4
    task_4 = PythonOperator(
        task_id = 'send_solution',
        python_callable = send_solution, 
    )

    # define task dependencies 
    task_1 >> task_2 >> task_3 >> task_4
    print("Dag complete")

