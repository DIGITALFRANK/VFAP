import boto3
from botocore.exceptions import ClientError
from datetime import datetime


def send_err_notif_email(environment, file_name, data_source, job_run_id, exception):
    # create a new SES resource and specify a region
    client = boto3.client('ses', region_name="us-east-1")
    # sender address must be verified with Amazon SES
    sender = "DL Notifications <retail_analytics@mail.vfc.com>"
    # if your account is still in the sandbox, recipient address must be verified as well
    recipient = "frank@digitalfrank.com"
    # subject line for the email
    subject = f"{environment} - {file_name} failed: {datetime.utcnow()}"
    # character encoding for the email
    charset = "UTF-8"

    # email body for recipients with non-HTML clients.
    body_text = ("f{environment} - {file_name} failed:\r\n"
                 f"Data Source: {data_source}"
                 f"Time: {datetime.utcnow()}"
                 f"Exception: {exception}"
                 f"Job Run ID: {job_run_id}"
                 )

    # HTML body of the email.
    body_html = f"""<html>
    <head></head>
    <body>
      <h1>{environment} - {file_name} failed:</h1>
      <p>Data Source: {data_source}
        </br>
        Time: {datetime.utcnow()}
        </br>
        Job Run ID: {job_run_id}
        </br>
        Exception: {exception}
        </br>
        log into the AWS console <a href='https://vfcloud.awsapps.com/start#/'>here</a>
      </p>
    </body>
    </html>
                """

    try:
        response = client.send_email(
            Source=sender,
            Destination={
                'ToAddresses': [recipient]
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': charset,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': charset,
                        'Data': body_text,
                    },
                },
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


## add the following import in DL "main.py"
# from common-routine.dl_email_err_notifs import send_err_notif_email

## in transform-routine >> src >> digital_main.py,
## add the following line at the end of the Error Handling code block
## BEFORE the last line [ raise Exception("{}".format(error)) ]
# send_err_notif_email(environment, file_name, data_source, job_run_id, error)
## can you get env & data source from Dynamo Config table?? -- read the main.py code
