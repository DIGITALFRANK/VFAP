from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from modules.utils.utils_dataprocessor import utils
import boto3
import json
import csv
s3 = boto3.client('s3')
sr3 = boto3.resource('s3')
key = utils.get_parameter_store_key()
env_params = utils.get_param_store_configs(key)
obj = sr3.Object('aws-samcli-lambda', 'prev_fiscal_transformation/viewid.txt')
body = obj.get()['Body'].read().decode(encoding="utf-8", errors="ignore")
json_view = eval(body)
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
f = csv.writer(open("C:\\Users\\Abhinav.Srivastav\\Downloads\\GoogleAnalytics.csv", "w", newline=''))
f.writerow(["Date", "DeviceCategory", "Total_Sessions", "Total_units", "Total_sales", "Total_orders",
            "average_time_per_page", "Total_page_views", "average_session_length", "Bounce_Rate", "Total_visitors",
            "profile_id", "profile"])
for i in range(0, len(json_view['Google_Analytics']['ViewID'])):
    VIEW_ID = json_view['Google_Analytics']['ViewID'][i]
    Profile = json_view['Google_Analytics']['Profile'][i]

    def initialize_analyticsreporting():
        secret_key = utils.get_secret(env_params['googleapi_secret_name'])
        credentials = ServiceAccountCredentials._from_parsed_json_keyfile(secret_key, SCOPES)
        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)

        return analytics


    def get_report(analytics):
        return analytics.reports().batchGet(
            body={
              'reportRequests': [
              {
                'viewId': VIEW_ID,
                'dateRanges': [{'startDate': '2019-12-22', 'endDate': '2019-12-22'}],
                'metrics': [{'expression': 'ga:sessions'}, {'expression': 'ga:itemquantity'},
                            {'expression': 'ga:totalvalue'}, {'expression': 'ga:transactions'},
                            {'expression': 'ga:avgtimeonpage'}, {'expression': 'ga:pageviews'},
                            {'expression': 'ga:avgSessionDuration'}, {'expression': 'ga:visitbouncerate'},
                            {'expression': 'ga:users'}, {'expression': 'ga:uniquepageviews'}],
                'dimensions': [{'name': 'ga:date'}, {'name': 'ga:deviceCategory'}]
              }]
            }
        ).execute()


    def print_response(response):
        print(response)
        response = json.dumps(response)
        json_data = json.loads(response)
        dict1 = {}
        lst_dimension = []
        lst_check = []
        lst_final = []
        lst_metrics = []
        lst_values = []
        lst = json_data['reports'][0]['columnHeader']['dimensions']
        lst2 = json_data['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
        lst3 = json_data['reports'][0]['data']['rows']
        for i in lst:
            i = i.strip('ga:')
            lst_dimension.append(i)
        for j in lst3:
            str_date = datetime.strptime(j['dimensions'][0], '%Y%m%d')
            j['dimensions'][0] = str_date.strftime('%Y-%m-%d')
            lst_check.append(j['dimensions'])
        for z in range(0, len(lst_check)):
            dict1 = {lst_dimension[0]: lst_check[z][0], lst_dimension[1]: lst_check[z][1]}
            lst_final.append(dict1)
        for i in lst2:
            lst_metrics.append(i['name'].strip('ga:'))
        for i in lst3:
            lst_values.append(i['metrics'][0]['values'])
        for z in range(0, len(lst_final)):
            print(lst_final[z])
            for i in range(0, len(lst_metrics)):
                lst_final[z].update({lst_metrics[i]: lst_values[z][i]})
                lst_final[z].update({'Profile_id': VIEW_ID, 'Profile': Profile})

        print(lst_final)
        for z in range(0, len(lst_final)):
            f.writerow([lst_final[z]['date'], lst_final[z]['deviceCategory'], lst_final[z]['sessions'],
                        lst_final[z]['itemquantity'], lst_final[z]['totalvalue'], lst_final[z]['transactions'],
                        lst_final[z]['vgtimeonpage'], lst_final[z]['pageviews'], lst_final[z]['vgSessionDuration'],
                        lst_final[z]['visitbouncerate'], lst_final[z]['users'], lst_final[z]['Profile_id'],
                        lst_final[z]['Profile']])

    def main():
        initialize_analyticsreporting()
        analytics = initialize_analyticsreporting()
        response = get_report(analytics)
        print_response(response)


    if __name__ == '__main__':
        main()
