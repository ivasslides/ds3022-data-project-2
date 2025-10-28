# prefect flow goes here
import os 
import boto3
import time 
from prefect import flow, task, get_run_logger
import requests 

# url
url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/fbv2sc"
sqs = boto3.client('sqs')

# receiving message
@task(log_prints=True)
def get_messages(url):
    logger = get_run_logger()
    # request from API 
    try: 
        response = requests.post(url)
        payload = response.json()  
        logger.info("API reached, and SQS queue populated.")

        # printing payload to check
        logger.info(f"Raw PAYLOAD content: {payload}")

        # returning my queue url 
        return payload

    except Exception as e:
        print(f"Error reaching API: {e}")
        raise e 
     

@task(log_prints=True)
def monitoring(queue_url): 
    logger = get_run_logger()
    # create empty dictionary to store numbers and words
    received_messages = {}

    # initiate count 
    count = 0

    try:
        # while count is less than 21, aka more messages to be receieved 
        while count < 21:
            logger.info(f"COUNT: {count}")
            # check queue attributes
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages', 
                                'ApproximateNumberOfMessagesNotVisible', 
                                'ApproximateNumberOfMessagesDelayed'],
            )

            # number of messages
            avail_msgs = int(response['Attributes']['ApproximateNumberOfMessages'])
            delayed_msgs = int(response['Attributes']['ApproximateNumberOfMessagesDelayed'])
            not_vis_msgs = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])

            logger.info(f"Available messages: {avail_msgs}, | Not visible: {not_vis_msgs} | Delayed: {delayed_msgs}")

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
                logger.info("No messages yet - now waiting.")
                time.sleep(30)

        print(f"All messages received. Count is {count}")   
        return received_messages     

    except Exception as e:
        print(f"Error collecting messages: {e}")
        raise e 

     

@task(log_prints=True)
def arrange_messages(received_messages):
    logger = get_run_logger()
    received_messages = received_messages

    # sort dictionary by key (the numbers)
    sorted_msg = sorted(received_messages.items())

    # prove that sorting worked
    logger.info(f"Sorted phrase: {sorted_msg}")

    # extract words 
    ordered_words = [word for _, word in sorted_msg]

    # join as a string 
    phrase = " ".join(ordered_words)

    print(f"PHRASE is: {phrase}")
    return phrase


@task(log_prints=True)
def send_solution(uvaid, phrase, platform):
    logger = get_run_logger()
    submit_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    try: 
        response = sqs.send_message(
            QueueUrl=submit_url, 
            MessageBody='solution', 
            MessageAttributes={
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
        logger.info("Message has been sent.")

    except Exception as e:
        print(f"Error sending message: {e}")
        raise e

# sending message flow
@flow
def sending_sqs_flow():
    logger = get_run_logger()
    logger.info("Flow started.")
    # get messages 
    payload = get_messages(url)

    # extract url
    queue_url = payload.get("sqs_url")

    # monitor queue
    msgs_future = monitoring(queue_url)

    # reassemble 
    phrase_future = arrange_messages(msgs_future)

    # send final message
    send_solution(uvaid='fbv2sc', phrase=phrase_future, platform='prefect')

    logger.info("Flow complete!")


if __name__ == "__main__":
    sending_sqs_flow() 