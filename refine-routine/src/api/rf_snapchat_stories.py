import csv
from utils.s3_util import S3Client
from datetime import datetime
import json
import boto3
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')

class rf_snapchat_stories:
    """class with default constructor to perform parsing of snapchat stories
        
    """

    @staticmethod
    def parser(source_bucket, file_name):
        lst = []
        dict1 = {}
        try:
            x = file_name.rsplit('/', 1)
            file_part = x[1].strip('.json')
            date_part = file_part.rsplit('_', 1)
            date_obj = datetime.strptime(date_part[1], '%Y%m%d')
            date = date_obj.strftime('%Y-%m-%d')
            #Reading the json file from s3 bucket
            obj = sr3.Object(source_bucket, file_name)
            body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            #Converting the String into Dictionary
            data = json.loads(body)
            #appending the Data with headers into a dictionary
            for j in range(0, len(data)):
                for i in range(0, len(data[j]['stories'])):
                    dict1 = {"Brand": data[j]['brand'], "Story_Id": data[j]['stories'][i]['id'], "Opens": data[j]['stories'][i]['opens'],
                         "CompletionRate": data[j]['stories'][i]['completionRate'], "TotalTime": data[j]['stories'][0]['totalTime'],
                         "TotalScreenshots": data[j]['stories'][0]['totalScreenshots'],
                         "TotalViews": data[j]['stories'][0]['totalViews'], "Timestamp": data[j]['stories'][0]['timestamp']}
                    lst.append(dict1)
            #Opening the csv file in /tmp folder
            f = csv.writer(open('/tmp/Stories_Output_SNAPCHAT_.csv', 'a', newline=''))
            #Writing the headers to the csv file
            f.writerow(["Brand", "Story_Id", "Opens", "CompletionRate", "TotalTime", "TotalScreenshots",
                        "TotalViews", "Timestamp"])
            #writing the data to the csv file
            for j in range(0, len(lst)):
                f.writerow([lst[j]['Brand'], lst[j]['Story_Id'], lst[j]['Opens'], lst[j]['CompletionRate'], lst[j]['TotalTime'],
                           lst[j]['TotalScreenshots'], lst[j]['TotalViews'], lst[j]['Timestamp']])
        except Exception as error:
            print(error)
        return '/tmp/Stories_Output_SNAPCHAT_.csv', date