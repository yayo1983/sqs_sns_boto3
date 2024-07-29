import boto3
from botocore.exceptions import ClientError


TOPIC_NAME = 'SubscriptionTopic'
TOPIC_ARN = 'arn:aws:sns:us-east-1:068137244923:SubscriptionTopic'
QUEUE_ARN = 'arn:aws:sqs:us-east-1:068137244923:Main-Queue'


def sns_client():
    sns = boto3.client('sns', region_name='us-east-1')
    return sns


def create_topic():
    sns_client().create_topic(
        Name=TOPIC_NAME
    )


def get_topics():
    return sns_client().list_topics()


def get_topic_attributes():
    return sns_client().get_topic_attributes(
        TopicArn=TOPIC_ARN
    )


def update_topic_attributes():
    return sns_client().set_topic_attributes(
        TopicArn=TOPIC_ARN,
        AttributeName='DisplayName',
        AttributeValue=TOPIC_NAME + '-Updated'
    )


def delete_topic():
    return sns_client().delete_topic(
        TopicArn=TOPIC_ARN
    )


def create_email_subscription(topic_arn, email_address):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='email',
        Endpoint=email_address
    )


def create_sms_subscription(topic_arn, phone_number):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sms',
        Endpoint=phone_number
    )


def create_sqs_subscription(topic_arn, queue_arn):
    return sns_client().subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )


def get_topic_subscriptions(topic_arn):
    return sns_client().list_subscriptions_by_topic(
        TopicArn=topic_arn
    )


def check_if_phone_number_opted_out(phone_number):
    return sns_client().check_if_phone_number_is_opted_out(
        phoneNumber=phone_number
    )


def list_phone_numbers_opted_out():
    return sns_client().list_phone_numbers_opted_out()


def opt_out_of_email_subscription(email_add):
    try:
        subscriptions = get_topic_subscriptions(TOPIC_ARN)
        for subscription in subscriptions['Subscriptions']:
            if subscription['Protocol'] == 'email' and subscription['Endpoint'] == email_add:
                print('Unsubscribing ' + subscription['Endpoint'] + ' email ' + email_add)
                subscription_arn = subscription.get('SubscriptionArn')
                # Verificar si el ARN es válido
                if subscription_arn and subscription_arn.startswith('arn:aws:sns:'):
                    print(f'Unsubscribing {email_add} with ARN {subscription_arn}')
                    sns_client().unsubscribe(SubscriptionArn=subscription_arn)
                    return f'Successfully unsubscribed {email_add}'
                else:
                    return f'Invalid SubscriptionArn for {email_add}'
        return f'No subscription found for {email_add}'
    except ClientError as e:
        return f'Error: {e}'

def opt_out_of_sms_subscription(phone):
    try:
        subscriptions = get_topic_subscriptions(TOPIC_ARN)
        for subscription in subscriptions['Subscriptions']:
            if subscription['Protocol'] == 'sms' and subscription['Endpoint'] == phone:
                print('Unsubscribing ' + subscription['Endpoint'] + ' phone ' + phone)
                subscription_arn = subscription.get('SubscriptionArn')
                # Verificar si el ARN es válido
                if subscription_arn and subscription_arn.startswith('arn:aws:sns:'):
                    print(f'Unsubscribing {phone} with ARN {subscription_arn}')
                    sns_client().unsubscribe(SubscriptionArn=subscription_arn)
                    return f'Successfully unsubscribed {phone}'
                else:
                    return f'Invalid SubscriptionArn for {phone}'
        return f'No subscription found for {phone}'
    except ClientError as e:
        return f'Error: {e}'

def opt_in_phone_number(phone_number):
    try:
        response = sns_client().opt_in_phone_number(
            phoneNumber=phone_number
        )
        print(f'Successfully opted in phone number {phone_number}')
        return response

    except ClientError as e:
        print(f"ClientError: {e}")
        return f'Error: {e}'
    except Exception as e:
        print(f"Unexpected error: {e}")
        return f'Error: {e}'


def publishing_message_to_subscribers(topic_arn):
    return sns_client().publish(
        TopicArn=topic_arn,
        Message='Hello, you are receiving this, because you are subscribed'
    )


if __name__ == '__main__':
    # print(create_topic())
    # print(get_topics())
    # print(get_topic_attributes())
    # print(update_topic_attributes())
    # print(delete_topic())
    # print(create_email_subscription(TOPIC_ARN, 'yazanenator@gmail.com'))
    # print(create_sms_subscription(TOPIC_ARN, '524731472061'))
    # print(create_sqs_subscription(TOPIC_ARN, QUEUE_ARN))
    # print(get_topic_subscriptions(TOPIC_ARN))
    # print(check_if_phone_number_opted_out('524731472061'))
    # print(list_phone_numbers_opted_out())
    # print(opt_out_of_email_subscription('yazanenator@gmail.com'))
    # print(opt_out_of_sms_subscription('+524731472061'))
    # print(opt_in_phone_number('524731472061'))
    print(publishing_message_to_subscribers(TOPIC_ARN))