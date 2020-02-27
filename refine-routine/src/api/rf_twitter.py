import json
import boto3
import csv
from datetime import datetime
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class rf_twitter:
    """class with default constructor to perform parsing of Twitter
        
    """

    @staticmethod
    def parser(source_bucket, file_name):
        try:
            y = file_name.rsplit('/')
            local_file_name = '/tmp/'+y[2]
            #Loading the csv file from s3 into /tmp folder
            sr3.Bucket(source_bucket).download_file(file_name, local_file_name)
            date_part = file_name.rsplit("_", 1)
            date = date_part[1].strip('.csv')
            # date_obj = datetime.strptime(date, '%Y-%m-%d')
            # final_date = date_obj.strftime('%Y-%m-%d')
        except Exception as error:
            print(error)
        #Returning the file and date 
        return local_file_name, date