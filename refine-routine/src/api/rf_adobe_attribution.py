import json
import boto3
from datetime import datetime
import csv
from utils.s3_util import S3Client
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')

class rf_adobe_attribution:

    @staticmethod
    def parser(source_bucket, file_name):
        y = file_name.rsplit('/')
        local_file_name = '/tmp/' + y[2]
        sr3.Bucket(source_bucket).download_file(file_name, local_file_name)
        date_part = file_name.rsplit("_", 1)
        date = date_part[1].strip('.csv')

        if file_name.lower().find('weekly') != -1:
            final_date = date[15:]
        else:
            final_date = date[-8:]
        final_date = datetime.strptime(final_date, '%Y%m%d').strftime('%Y-%m-%d')
        return local_file_name, final_date