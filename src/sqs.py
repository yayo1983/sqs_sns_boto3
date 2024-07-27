import boto3
import json
import os
from dotenv import load_dotenv


QUEUE_NAME = 'Mytest-SQS-queue'
QUEUE_NAME_URL = 'https://sqs.eu-west-1.amazonaws.com/068137244923/Mytest-SQS-queue'
QUEUE_FIFO_NAME = 'MytestQueue.fifo'
QUEUE_FIFO_NAME_URL = 'https://sqs.eu-west-1.amazonaws.com/068137244923/MytestQueue.fifo'
QUEUE_DLQ_NAME = 'Dead-Letter-Queue-for-Main'
Main_Queue = 'Main-Queue'
Main_Queue_URL ='https://sqs.eu-west-1.amazonaws.com/068137244923/Main-Queue'
def sqs_client():
    sqs = boto3.client('sqs', region_name='eu-west-1')
    return sqs


# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Acceder a las variables de entorno
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

def create_sqs_queue():
    return sqs_client().create_queue(
        QueueName=QUEUE_NAME
    )

def create_fifo_queue():
    return sqs_client().create_queue(
        QueueName=QUEUE_FIFO_NAME,
        Attributes={
            'FifoQueue': 'true'
        }
    )

def create_queue_dead_letter():
    return sqs_client().create_queue(
        QueueName=QUEUE_DLQ_NAME
    )

def create_main_queue_for_dead_letter():
    redrive_policy = {
        'deadLetterTargetArn': 'arn:aws:sqs:eu-west-1:068137244923:Dead-Letter-Queue-for-Main',
        'maxReceiveCount': 3
    }
    return sqs_client().create_queue(
        QueueName=Main_Queue,
        Attributes={
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144',
            'VisibilityTimeout': '30',
            'MessageRetentionPeriod': '345688',
            'ReceiveMessageWaitTimeSeconds': '0',
            'RedrivePolicy': json.dumps(redrive_policy)
        }
    )

def find_queue():
    return sqs_client().list_queues(
        QueueNamePrefix='Mytest'
    )

def list_all_queues():
    return sqs_client().list_queues()

def get_queue_attributes():
    return sqs_client().get_queue_attributes(
        QueueUrl = Main_Queue_URL,
        AttributeNames= ['All']
    )

def update_queue_attributes():
    return sqs_client().set_queue_attributes(
        QueueUrl = Main_Queue_URL,
        Attributes={
            'MaximumMessageSize': '131072',
            'VisibilityTimeout': '15'
        }
    )
def delete_queue_attributes():
    return sqs_client().delete_queue(
        QueueUrl = QUEUE_NAME_URL,
    )

def send_message_toqueue():
    return sqs_client().send_message(
        QueueUrl=Main_Queue_URL,
        MessageAttributes={
            'Title': {
                'DataType': 'String',
                'StringValue': 'My message  title',
                },
            'Author':
                {
                'DataType': 'String',
                'StringValue': 'Niyazi',
            },
            'Time':
                {
                    'DataType': 'Number',
                    'StringValue': '6',
                }
        },
        MessageBody='This is first sqs message'
    )

# enviar mensajes por lotes
def send_batch_message_toqueue():
    return sqs_client().send_message_batch(
        QueueUrl=Main_Queue_URL,
        Entries=[
            {
                'Id': 'FirstMessageBatch',
                'MessageBody': 'first message of batch'
            },
            {
                'Id': 'SecondMessageBatch',
                'MessageBody': 'Second message of batch'
            },
            {
                'Id': 'ThirdMessageBatch',
                'MessageBody': 'Thrid message of batch'
            },
            {
                'Id': 'FourMessageBatch',
                'MessageBody': 'fourth message of batch'
            },
        ]

    )

if __name__ == '__main__':
    # print(create_sqs_queue())
    # print(create_fifo_queue())
    # print(create_queue_dead_letter())
    # print(create_main_queue_for_dead_letter())
    # print(find_queue())
    # print(list_all_queues())
    # print(get_queue_attributes())
    # print(update_queue_attributes())
    # print(delete_queue_attributes())
    # print(send_message_toqueue())
    print(send_batch_message_toqueue())
