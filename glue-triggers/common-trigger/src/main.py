import json
import boto3
import os
import time

#environment = os.environ.get('environment', 'dev')
environment = os.environ['environment']
gluejobname = f'vf-{environment}-transformed-job'


def lambda_handler(event, context):
    # TODO implement
    print(event)
    time.sleep(15)

    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    foldername = event['Records'][0]['s3']['object']['key']
    obj = event.get('Records')[0].get('s3').get('object')
    if foldername.find('common_files') != -1:
        return False
    elif not int(obj.get('size', 0)) != -1:
        return False
    else:
        filename = foldername.split('/')[2]
        print(filename)
        try:
            runId = glue.start_job_run(JobName=gluejobname,
                                       Arguments={
                                           '--FILE_NAME': filename})
            status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
            print("Job Status : ", status['JobRun']['JobRunState'])
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist '
                  'and your bucket is in the same region as this '
                  'function.'.format(source_bucket, source_bucket))
            raise e