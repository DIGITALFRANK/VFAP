import csv
from io import StringIO
from datetime import datetime, timedelta
import json
import boto3
from external_lib import requests
import os
from utils.dynamo_util import DynamoUtils
from utils.secrets_util import SecretsUtil
import sys
from utils.s3_util import S3Client
from utils.sns_obj import sns_notification

SNS_FAILURE_TOPIC = 'TODO'
os.chdir("/tmp/")
secrets_util = SecretsUtil()
dynamo_db = DynamoUtils()
s3_client = S3Client()


class youtube:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

                  Arguments:
                      api_params-- Required parameters fetched from DynamoDB
                     source_id -- Uniqud id assigned to particular data source and file pattern
                  """
        # get secret keys from secret manager

        try:
            secret_manager_key = api_params['secret_manager_key']
            api_key = secrets_util.get_secret(secret_manager_key, 'api_secret_key')

        except Exception as e:
            message = (
                f"Exception: {e} \nMessage: secret value cannot be fetched"

            )
            sns_notification(
                SNS_FAILURE_TOPIC,
                message,
                "General exception occurred."
            )
        date_format = api_params['dateformat']
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1), date_format)
        api_params_source_filename = api_params["api_params_filename"]
        target_bucket = api_params['target_bucket']
        body = s3_client.read_file_from_s3(target_bucket, api_params_source_filename)
        json_view = eval(body)
        channel_id_list = json_view['youtube']['channel_id']
        part = api_params['part']
        url = api_params['api_url']
        bucket_key_folder = api_params['bucket_key_folder']
        response_list = []

        # api calls to get the json data from different apis
        for i in range(0, len(channel_id_list)):
            params = {'id': channel_id_list[i],
                      'key': api_key,
                      'part': part
                      }
            # try:
            response = requests.get(url=url, params=params)
            data = json.loads(response.content)
            # Writing the file to tmp folder
            response_list.append(data)

        with open("/tmp/youtube.json", 'w') as json_file:
            json.dump(response_list, json_file)
        try:
            # writing the file to s3 bucket
            s3_client.move_from_tmp_to_bucket(
                "/tmp/youtube.json",
               bucket_key_folder + "/SOCIAL_MEDIA_YOUTUBE_{}.json".format(yesterday_date),
                target_bucket
            )
        except Exception as e:
            # message = (
            #     f"Exception: {e} \nMessage: File youtube could not be"
            #     f"uploaded to S3 bucket {target_bucket}"
            #     "\nFunction name: yotube"
            # )
            # sns_notification(
            #     SNS_FAILURE_TOPIC,
            #     message,
            #     "General exception occurred."
            # )
            print(f"Exception: {e} \nMessage: Youtube data could not be"
                  f"uploaded to S3 bucket {target_bucket}"
                  "\nFunction name: youtube")