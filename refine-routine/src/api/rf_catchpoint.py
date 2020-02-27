import json
import boto3
from datetime import datetime
import csv
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')
header_list = []
final_lst = []
metrics_dict = {}

class rf_catchpoint:
    """class with default constructor to perform parsing of catchpoint
        
    """

    @staticmethod
    def parser(source_bucket, file_name):
        try:
            date_part = file_name.rsplit('_', 1)
            date_part = date_part[1].strip(".json")
            date_obj = datetime.strptime(date_part, '%Y%m%d')
            final_date = date_obj.strftime('%Y-%m-%d')
            #Reading the json file from s3 bucket
            obj = sr3.Object(source_bucket, file_name)
            body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
            #Converting the string into json
            data = json.loads(body)
            #Storing the Columns for the csv file into a list 
            for k in data["summary"]["fields"]["synthetic_metrics"]:
                header_list.append(k["name"])
            #Storing Date, Test_id and Test Column into a list
            for items in data["summary"]["items"]:
                metrics_dict = {"Date": data["start"], "Test": items["breakdown_1"]["name"],
                                "Test_Id": items["breakdown_1"]["id"]}
                final_lst.append(metrics_dict)
            #Adding the Data into the final list in form of key value pair
            for k in range(0, len(final_lst)):
                for j in range(0, len(header_list)):
                    final_lst[k].update({header_list[j]: data["summary"]["items"][k]["synthetic_metrics"][j]})
            #Splitting the Test Column into different columns and appending them in final list
            for l in range(0, len(final_lst)):
                x = final_lst[l]['Test'].split(" - ")
                y = x[1].split(" ")
                final_lst[l].update({"Device_Category": y[0].upper(), "Brand_ADJ": x[0].upper(),
                                    "page_type": y[0].upper()+" "+y[1].upper()})
            #Handling the None Values in file by replacing them with 0
            for k in range(0, len(final_lst)):
                date_obj = datetime.strptime(final_lst[k]['Date'], '%Y-%m-%dT%H:%M:%S')
                date = date_obj.strftime("%Y-%m-%d")
                if final_lst[k]['Time to Title (ms)'] == None:
                    final_lst[k].update({'Time to Title (ms)': 0})
                if final_lst[k]['Brand_ADJ'] == 'THE NORTH FACE':
                    final_lst[k].update({'Brand_ADJ': 'TNF US'})
                if final_lst[k]['Brand_ADJ'] == 'KIPLING USA':
                    final_lst[k].update({'Brand_ADJ': 'KIPLING US'})
                final_lst[k].update({'Date': date})
            #opening a csv file in temp
            f = csv.writer(open('/tmp/WEB_PERFORMANCE_CATCHPOINT_.csv', 'a', newline=''))
            #Writing the headers of the csv file
            f.writerow(["Date", "Device_Category", "Brand_ADJ", "page_type", "Test_Id", "Avg_Response_ms",
                        "Avg_Total_Downloaded_Bytes", "Avg_DOM_Load_ms", "Avg_Document_Complete_ms", "Avg_Webpage_Response_ms",
                        "Avg_Render_Start_ms", "Avg_Time_to_Title_ms", "Avg_No_Connections", "Avg_No_Hosts", "Availability",
                        "No_Runs"])
            #Writing the Data of the csv file
            for z in range(0, len(final_lst)):
                f.writerow([final_lst[z]['Date'], final_lst[z]['Device_Category'], final_lst[z]['Brand_ADJ'],
                            final_lst[z]['page_type'], final_lst[z]['Test_Id'], final_lst[z]['Response (ms)'],
                            final_lst[z]['Total Downloaded Bytes'], final_lst[z]['DOM Load (ms)'], final_lst[z]['Document Complete (ms)'],
                            final_lst[z]['Webpage Response (ms)'], final_lst[z]['Render Start (ms)'], final_lst[z]['Time to Title (ms)'],
                            final_lst[z]['# Connections'], final_lst[z]['# Hosts'], final_lst[z]['% Availability'],
                            final_lst[z]['# Runs']])
        except Exception as error:
            print(error)
        return '/tmp/WEB_PERFORMANCE_CATCHPOINT_.csv', final_date
