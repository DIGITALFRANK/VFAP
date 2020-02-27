import json
import boto3
import csv
from datetime import datetime
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')


class rf_youtube:
    """class with default constructor to perform parsing of Youtube
        
    """
    
    @staticmethod
    def parser(source_bucket, file_name):
        lst = []
        lst1 = []
        file_part = file_name.rsplit('/', 1)
        file_inter = file_part[1].strip('.json')
        date = file_inter.rsplit('_', 1)
        date_obj = datetime.strptime(date[1], '%Y%m%d')
        final_date = date_obj.strftime('%Y-%m-%d')
        #Reading the file from s3
        obj = sr3.Object(source_bucket, file_name)
        body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
        #Converting the string to Dictionary
        data = json.loads(body)
        #appending the data with Headers to a list
        for i in range(0, len(data)):
            lst.append(data[i]['items'][0]['statistics'])
            lst1.append(data[i]['items'][0]['id'])
        for j in range(0, len(lst)):
            lst[j].update({"Date": final_date, "Channel_id": lst1[j]})
        #Opening a csv file in /tmp folder
        f = csv.writer(open('/tmp/SOCIAL_MEDIA_YOUTUBE_'+final_date+'.csv', 'a', newline=''))
        #writing the Headers to the csv file
        f.writerow(["Channel_id", "Date", "ViewCount", "SubscriberCount", "VideoCount", "CommentCount"])
        #Writing the Data to the csv file
        for k in range(0, len(lst)):
           f.writerow([lst[k]['Channel_id'], lst[k]['Date'], lst[k]['viewCount'], lst[k]['subscriberCount'],
                       lst[k]['videoCount'], lst[k]['commentCount']])
        return '/tmp/SOCIAL_MEDIA_YOUTUBE_'+final_date+'.csv', final_date
