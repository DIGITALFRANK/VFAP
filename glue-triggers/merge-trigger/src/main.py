from datetime import date, timedelta, datetime
import boto3
import os

glue = boto3.client('glue')
week_number = date.today().isocalendar()[1]


def lambda_handler(event, context):
    print("Week number: " + str(week_number))
    environment = os.environ['environment']
    gluejobname = f'vf-{environment}-merge-job'
    date = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    print(date)
    filename = event["feed_name"]+date
    print(filename)
    if week_number % 2 != 0 and event["feed_name"] == 'VF_FILE_ADOBEBIWEEKLY_':
        try:
            print("In Biweekly")
            runId = glue.start_job_run(JobName=gluejobname,
                                      Arguments={
                                          '--FILE_NAME': filename,
                                          '--DATE': date
                                      })
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
            print("Job Status : ", status['JobRun']['JobRunState'])
        except Exception as e:
            print(e)
            print('Error triggering the Glue Job: '.format(gluejobname))
            raise e
    elif event["feed_name"] == 'VF_FILE_ADOBEWEEKLY_' or event["feed_name"] == 'VF_FILE_ECOMMERGE_':
        try:
            print("Not in Biweekly")
            runId = glue.start_job_run(JobName=gluejobname,
                                      Arguments={
                                          '--FILE_NAME': filename,
                                          '--DATE': date
                                      })
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
            print("Job Status : ", status['JobRun']['JobRunState'])
        except Exception as e:
            print(e)
            print('Error triggering the Glue Job: '.format(gluejobname))
            raise e
    else:
        return False