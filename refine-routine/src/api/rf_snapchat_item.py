import json
import boto3
from datetime import datetime
import csv
from utils.s3_util import S3Client
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')

class rf_snapchat_item:
    """class with default constructor to perform parsing of Snapchat Items
        
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
            #Reading the json file stored on s3
            obj = sr3.Object(source_bucket, file_name)
            body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            #getting the string data into a dictionary object
            data = json.loads(body)
            #appending the data with headers into a list
            for j in range(0, len(data)):
                for i in range(0, len(data[j]['items'])):
                    dict1 = {"Brand": data[j]['brand'], "timestamp": data[j]['items'][i]['timestamp'],
                             "screenshotCount": data[j]['items'][i]['screenshotCount'],
                             "type": data[j]['items'][i]['type'], "viewCount": data[j]['items'][i]['viewCount']}
                    lst.append(dict1)
            #Opening the csv file in /tmp folder
            f = csv.writer(open('/tmp/Single_Brand_SNAPCHAT_.csv', 'a', newline=''))
            f.writerow(["Brand", "timestamp", "screenshotCount", "type", "viewCount"])
            #writing the data to the csv 
            for j in range(0, len(lst)):
                f.writerow([lst[j]['Brand'], lst[j]['timestamp'], lst[j]['screenshotCount'], lst[j]['type'],
                            lst[j]['viewCount']])
        except Exception as error:
            print(error)
        return '/tmp/Single_Brand_SNAPCHAT_.csv', date