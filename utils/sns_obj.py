import boto3


sns = boto3.client('sns')


# Function to send out notification
def sns_notification(sns_topic, messages, subject):
    sns_response = sns.publish(
        TopicArn=sns_topic,
        Message=messages,
        Subject=subject
    )
    sns_table = dynamo_db.Table(f'vf-{environment}-sns-details')
    sns_table.put_item(
        Item={
            'message_id': sns_response['MessageId'],
            'message': messages,
            'triggered': strftime("%Y-%m-%d %H:%M:%S", gmtime()),
            'sns_topic': sns_topic
        }
    )
