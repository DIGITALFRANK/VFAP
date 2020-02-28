import json
import boto3
import os
from datetime import datetime, timedelta

#environment = os.environ.get('environment', 'dev')
environment = os.environ['environment']
gluejobname = f'vf-{environment}-merge-job'
# gluejobname = f'vf_{environment}_merge_job'

def lambda_handler(event, context):
    # TODO implement
    
    glue = boto3.client('glue')
    
    date = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    print(date)
    filename = event["feed_name"]+date
    print(filename)
    
    try:
        runId = glue.start_job_run(JobName=gluejobname,
        Arguments = {
                '--FILE_NAME': filename,
                '--DATE': date
        })
        status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
        print("Job Status : ", status['JobRun']['JobRunState'])
    except Exception as e:
        print(e)
        print('Error triggering the Glue Job: '.format(gluejobname))
        raise e