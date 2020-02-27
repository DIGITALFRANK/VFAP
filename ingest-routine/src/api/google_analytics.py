import json
import os
from external_lib.googleapiclient.discovery import build
from utils.secrets_util import SecretsUtil
from utils.s3_util import S3Client
from external_lib.oauth2client.service_account import ServiceAccountCredentials
from utils.sns_obj import sns_notification
from datetime import datetime, timedelta

secrets_util = SecretsUtil()
s3_client = S3Client()
report_list = []


class google_analytics:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

                  Arguments:
                      api_params-- Required parameters fetched from DynamoDB
                     source_id -- Uniqud id assigned to particular data source and file pattern
                  """
        # getting secret keys dictionary from secret manager
        secret_manager_key = api_params['secret_manager_key']
        keys = secrets_util.get_all_secrets(secret_manager_key)
        # getting api params from dynamodb
        date_format = api_params['dateformat']
        today_date = datetime.strftime(datetime.now(), date_format)
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1), date_format)
        startDate = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        scopes = [api_params['api_url']]
        target_bucket = api_params['target_bucket']
        bucket_key_folder = api_params['bucket_key_folder']
        api_params_source_filename = api_params["api_params_filename"]
        body = s3_client.read_file_from_s3(target_bucket, api_params_source_filename)
        json_view = eval(body)
        view_id = json_view['Google_Analytics']['ViewID']
        profile_id = json_view["Google_Analytics"]["Profile"]
        brand_id = json_view["Google_Analytics"]["Brand"]
        #make a call to google analytics API end point to get response
        file = '/tmp/google_analytics.json'
        if os.path.exists(file):
            os.remove(file)
        print("Removed the file %s" % file)
        for i in range(0, len(view_id)):
            option = google_analytics().main(view_id[i], profile_id[i], brand_id, scopes, target_bucket, i, keys,
                                             startDate)

        with open('/tmp/google_analytics.json', 'w') as json_file:
            json.dump(report_list, json_file)
        # Writing the file to tmp folder
        s3_client.move_from_tmp_to_bucket('/tmp/google_analytics.json',
                                          bucket_key_folder + "/"  "GOOGLE_ANALYTICS_GOOGLEANALYTICS_{}.json".format(
                                              yesterday_date), target_bucket)
        
        if os.path.exists(file):
            os.remove(file)
        print("Removed the file %s" % file)

    def initialize_analyticsreporting(self, scopes, keys):
        # authenticating google analytics API
        credentials = ServiceAccountCredentials._from_parsed_json_keyfile(keys, scopes)
        analytics = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)
        return analytics

    def get_report(self, analytics, VIEW_ID, startDate):
        # creating google analytics report
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': [{'startDate': startDate, 'endDate': startDate}],
                        'metrics': [{'expression': 'ga:sessions'}, {'expression': 'ga:itemquantity'},
                                    {'expression': 'ga:totalvalue'}, {'expression': 'ga:transactions'},
                                    {'expression': 'ga:avgtimeonpage'}, {'expression': 'ga:pageviews'},
                                    {'expression': 'ga:avgSessionDuration'}, {'expression': 'ga:visitbouncerate'},
                                    {'expression': 'ga:users'}, {'expression': 'ga:uniquepageviews'}],
                        'dimensions': [{'name': 'ga:date'}, {'name': 'ga:deviceCategory'}]
                    }]
            }
        ).execute()

    def main(self, VIEW_ID, profile_id, brand_id, scopes, target_bucket, file_count, keys, startDate):

        analytics = self.initialize_analyticsreporting(scopes, keys)
        response = self.get_report(analytics, VIEW_ID, startDate)
        response["view_id"] = VIEW_ID
        response["profile_id"] = profile_id
        response["brand"] = brand_id
        report_list.append(response)
