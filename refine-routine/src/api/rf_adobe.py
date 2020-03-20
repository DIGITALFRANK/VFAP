import json
import boto3
from datetime import datetime
from utils.s3_util import S3Client
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class rf_adobe:
    """class with default constructor to perform parsing of adobe
        
    """

    @staticmethod
    def parser(source_bucket, file_name):
        try:
            y = file_name.rsplit('/')
            local_file_name = '/tmp/' + y[2]
            #downloading the csv file from raw bucket into /tmp
            sr3.Bucket(source_bucket).download_file(file_name, local_file_name)
            date_part = file_name.rsplit("_", 1)
            date = date_part[1].strip('.csv')
            final_date = datetime.strptime(date, '%Y%m%d').strftime('%Y-%m-%d')
            
        except Exception as error:
            print(error)
        #returning the file stored in /tmp and date
        return local_file_name, final_date