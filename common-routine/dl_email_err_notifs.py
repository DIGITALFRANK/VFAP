import boto3
from botocore.exceptions import ClientError
from datetime import datetime

"""
This module handles email notification in case of any error while processing any file within the DL pipeline / 
We may want to wrap this module in a class and make calls from its @staticmethods - not imperative
"""

## in transform-routine >> src >> digital_main.py,
## add the following import
# from common-routine.dl_email_err_notifs import send_err_notif_email

## add the following line at the end of the Error Handling code block
## BEFORE the last line [ raise Exception("{}".format(error)) ]
# send_err_notif_email(environment, file_name, data_source, job_run_id, error) // how are you getting environement & dat_source

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


def send_email(subject, body_html, body_text):
    """
    functionality to send a AWS SES email
    :param subject: str
    :param body_html: str
    :param body_text: str
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


def send_err_notif_email(environment, file_name, data_source, job_run_id, exception):
    """
    Works with transform-routine.src.digital_main.driver
    Sends notification email for any exception raised within the pipeline
    *** Excludes ecomm_merge job ***

    :param environment: Str
    :param file_name: Str
    :param data_source: Str
    :param job_run_id: Str
    :param exception: Exception
    :return:
    """

    # subject line for the email
    subject = f"{environment.upper()} - {file_name} failed: {str(datetime.utcnow())}"

    # email body for recipients with non-HTML clients.
    body_text = ("f{environment} - {file_name} failed:\r\n"
                 f"Data Source: {data_source}"
                 f"Time: {str(datetime.utcnow())}"
                 f"Job Run ID: {job_run_id}"
                 f"Exception: {exception}"
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
      </p>
    </body>
    </html>
                """
    send_email(subject, body_html, body_text)


def send_ecomm_merge_missing_file_notif(environment, missing_sources, merge_job_record_count, date, job_run_id):
    """
    Works with modules.dataprocessor.dataprocessor_merge.process
    Sends email in case of missing source file

    :param environment: Str
    :param missing_sources: List
    :param merge_job_record_count: Int
    :param date: Str
    :param job_run_id: Str
    :return:
    """

    # subject line for the email
    subject = "ECOMM MERGE NOTIFICATION"

    # email body for recipients with non-HTML clients.
    body_text = (
        f"{environment.upper()} - Ecomm Merge job missing files for {date}:\r\n" 
        f"Missing files: {str(missing_sources)}"
        f"Final merge job count: {merge_job_record_count}"
        f"Job run ID: {job_run_id}"  # can we find this for each merge job???  where are the logs in s3?
        )

    # HTML body of the email.
    body_html = f"""<html>
        <head></head>
        <body>
          <h1>{environment.upper()} - Ecomm Merge job missing files for {date}</h1>
          <p>Missing files: {str(missing_sources)}
            </br>
            Final merge job count: {merge_job_record_count}
            </br>
            Job Run ID: {job_run_id}
          </p>
        </body>
        </html>
                    """
    send_email(subject, body_html, body_text)
