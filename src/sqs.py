import boto3
import json


QUEUE_NAME = 'Mytest-SQS-queue'
QUEUE_NAME_URL = 'https://sqs.us-east-1.amazonaws.com/068137244923/Mytest-SQS-queue'
QUEUE_FIFO_NAME = 'MytestQueue.fifo'
QUEUE_FIFO_NAME_URL = 'https://sqs.us-east-1.amazonaws.com/068137244923/MytestQueue.fifo'
QUEUE_DLQ_NAME = 'Dead-Letter-Queue-for-Main'
QUEUE_DLQ_URL = 'https://sqs.us-east-1.amazonaws.com/068137244923/Dead-Letter-Queue-for-Main'
Main_Queue = 'Main-Queue'
Main_Queue_URL ='https://sqs.us-east-1.amazonaws.com/068137244923/Main-Queue'
def sqs_client():
    sqs = boto3.client('sqs', region_name='us-east-1')
    return sqs

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
        'deadLetterTargetArn': 'arn:aws:sqs:us-east-1:068137244923:Dead-Letter-Queue-for-Main',
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

def send_message_to_queue():
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
def send_batch_message_to_queue():
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

def poll_queue_for_message():
    return sqs_client().receive_message(
        QueueUrl=Main_Queue_URL,
        MaxNumberOfMessages=10
    )

def process_message_from_queue():
    queue_messages = poll_queue_for_message()
    if 'Messages' in queue_messages and len(queue_messages['Messages']) >= 1:
        for message in queue_messages['Messages']:
            print("processing message" + message['MessageId'] + " with text"+ message['Body'])
            #delete_message_from_queue(message['ReceiptHandle'])
            change_message_visibility_timeout(message['ReceiptHandle'])

def delete_message_from_queue(receip_handle):
    sqs_client().delete_message(
        QueueUrl=Main_Queue_URL,
        ReceiptHandle=receip_handle
    )

def change_message_visibility_timeout(receipt_handle):
    sqs_client().change_message_visibility(
        QueueUrl=Main_Queue_URL,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=5
    )

def purge_queue():
    return sqs_client().purge_queue(
        QueueUrl=Main_Queue_URL
    )

if __name__ == '__main__':
    # print(create_sqs_queue())
    # print(create_fifo_queue())
    # print(create_queue_dead_letter())
    # print(create_main_queue_for_dead_letter())
    # print(find_queue())
    print(list_all_queues())
    # print(get_queue_attributes())
    # print(update_queue_attributes())
    # print(delete_queue_attributes())
    # print(send_message_to_queue())
    # print(send_batch_message_to_queue())
    # print(poll_queue_for_message())
    # print(process_message_from_queue())
    # print(purge_queue())
