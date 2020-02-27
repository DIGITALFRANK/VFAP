import json
import boto3
from datetime import datetime
import csv
from utils.s3_util import S3Client
sr3 = boto3.resource('s3')
s3 = boto3.client('s3')


class rf_google:
    """class with default constructor to perform parsing of catchpoint
        
    """

    @staticmethod
    def parser(source_bucket, file_name):
        final_lst = []
        lst_dimension = []
        lst_check = []
        lst_final = []
        lst_metrics = []
        lst_values = []
        lst_intermediate = []
        view_id_list = []
        profile_list = []
        n = 3
        try:
            #opening the csv file in /tmp/ folder
            f = csv.writer(open('/tmp/GOOGLE_ANALYTICS_GOOGLEANALYTICS.csv', 'a', newline=''))
            #writing the headers of the csv file
            f.writerow(["Date", "DeviceCategory", "Total_Sessions", "Total_units", "Total_sales", "Total_orders",
                        "average_time_per_page", "Total_page_views", "average_session_length", "Bounce_Rate",
                        "Total_visitors", "profile_id", "profile"])
            #Reading the json file
            obj = sr3.Object(source_bucket, file_name)
            body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            json_view = json.loads(body)
            #Adding the Metrics with headers into a list
            for i in range(0, len(json_view)):
                view_id_list.append(json_view[i]['view_id'])
                profile_list.append(json_view[i]['profile_id'])
                lst = json_view[i]['reports'][0]['columnHeader']['dimensions']
                lst2 = json_view[i]['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
                lst3 = json_view[i]['reports'][0]['data']['rows']
                for j in lst:
                    j = j.strip('ga:')
                    lst_dimension.append(j)
                for k in lst3:
                    str_date = datetime.strptime(k['dimensions'][0], '%Y%m%d')
                    k['dimensions'][0] = str_date.strftime('%Y-%m-%d')
                    lst_check.append(k['dimensions'])
                for l in lst2:
                    lst_intermediate.append(l['name'].strip('ga:'))
                    if l['name'] == 'ga:uniquepageviews':
                        lst_metrics.append(lst_intermediate)
                        lst_intermediate = []
                for m in lst3:
                    lst_values.append(m['metrics'][0]['values'])
            for z in range(0, len(lst_check)):
                dict1 = {lst_dimension[0]: lst_check[z][0], lst_dimension[1]: lst_check[z][1]}
                lst_final.append(dict1)
            for s in range(0, len(lst_final)):
                for t in range(0, len(lst_metrics[0])):
                    lst_final[s].update({lst_metrics[1][t]: lst_values[s][t]})
            #Adding View id and profile id to each dictionary inside the list
            view_id_final = [item for item in view_id_list for i in range(n)]
            profile_id_final = [item for item in profile_list for i in range(n)]
            for s in range(0, len(lst_final)):
                lst_final[s].update({"Profile_id": view_id_final[s]})
                lst_final[s].update({"Profile": profile_id_final[s]})
            #writing the Data to the csv file stored in temp
            for z in range(0, len(lst_final)):
                f.writerow([lst_final[z]['date'], lst_final[z]['deviceCategory'], lst_final[z]['sessions'],
                            lst_final[z]['itemquantity'], lst_final[z]['totalvalue'], lst_final[z]['transactions'],
                            lst_final[z]['vgtimeonpage'], lst_final[z]['pageviews'], lst_final[z]['vgSessionDuration'],
                            lst_final[z]['visitbouncerate'], lst_final[z]['users'], lst_final[z]['Profile_id'],
                            lst_final[z]['Profile']])
        except Exception as error:
            print(error)
        return '/tmp/GOOGLE_ANALYTICS_GOOGLEANALYTICS.csv', lst_final[0]['date']


