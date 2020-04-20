import boto3
from botocore.exceptions import ClientError
from datetime import datetime

"""
This module handles email notification in case of any error while processing any file within the DL pipeline
"""

## in transform-routine >> src >> digital_main.py,
## add the following import
# from common-routine.dl_email_err_notifs import send_err_notif_email

## add the following line at the end of the Error Handling code block
## BEFORE the last line [ raise Exception("{}".format(error)) ]
# send_err_notif_email(environment, file_name, data_source, job_run_id, error)

## can you get env & data source from Dynamo Config table?? -- read the main.py code
## make sure all these params are available within digital_main.py, and are passed to the function


# create a new SES resource and specify a region
client = boto3.client('ses', region_name="us-east-1")
# sender address must be verified with Amazon SES
sender = "DL Notifications <francis_agbodji@vfc.com>"  # change to retail_analytics@mail.vfc.com?
# if your account is still in the sandbox, recipient address must be verified as well
recipient = "frank@digitalfrank.com"  # change to vfap team? VF_UAPTEAM@vfc.com
# character encoding for the email
charset = "UTF-8"


def send_err_notif_email(environment, file_name, data_source, job_run_id, exception):
    """
    Works with transform-routine.src.digital_main.driver
    Sends notification email for any exception raised within the pipeline
    *** Excludes ecomm_merge job ***

    :param environment:
    :param file_name:
    :param data_source:
    :param job_run_id:
    :param exception:
    :return:
    """

    # subject line for the email
    subject = f"{upper(environment)} - {file_name} failed: {str(datetime.utcnow())}"

    # email body for recipients with non-HTML clients.
    body_text = ("f{environment} - {file_name} failed:\r\n"
                 f"Data Source: {data_source}"
                 f"Time: {str(datetime.utcnow())}"
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
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
                'Body': {
                    'Html': {
                        'Charset': charset,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': charset,
                        'Data': body_text,
                    }
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])






"""
*** in modules.dataprocessor.dataprocessor_merge.process ***

ecomm_merge_record_count = transformed_df.count()
env = env_params["transformed_bucket"].split("-")[1]  # double check this

if d1.head(1).isEmpty:
    send_ecomm_merge_missing_file_notif(env, "adobe source", ecomm_merge_job_record_count, date)
elif df2.count() == 0:
    ...

"""


def send_ecomm_merge_missing_file_notif(environment, file_source, merge_job_record_count, date):
    """
    Works with modules.dataprocessor.dataprocessor_merge.process
    Sends email in case of missing source file

    :param environment:
    :param file_source:
    :param merge_job_record_count:
    :param date:
    :return:
    """

    # subject line for the email
    subject = f"{upper(environment)} - {file_source} missing for E-COMM MERGE: {str(datetime.utcnow())}"

    # email body for recipients with non-HTML clients.
    body_text = (f"{upper(environment)} - {file_source} failed:\r\n"
                 f"Data Source: {file_source}"
                 f"Ecomm Merge Record Count: {merge_job_record_count}"
                 f"Date: {str(date)}"
                 f"Time: {str(datetime.utcnow())}"
                 f"Job Run ID: {job_run_id}"  # can we find this for each merge job???  where are the logs in s3?
                 )

    # HTML body of the email.
    body_html = f"""<html>
        <head></head>
        <body>
          <h1>{environment.upper()} - {file_source} failed:</h1>
          <p>Data Source: {file_source}
            </br>
            Ecomm Merge Record Count: {merge_job_record_count}
            </br>
            Date: {str(date)}
            </br>
            Time: {str(datetime.utcnow())}
            </br>
            Job Run ID: {job_run_id}
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
                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
                'Body': {
                    'Html': {
                        'Charset': charset,
                        'Data': body_html,
                    },
                    'Text': {
                        'Charset': charset,
                        'Data': body_text,
                    }
                }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
