from external_lib import requests
from datetime import datetime, timedelta
import boto3
import json
import csv
from utils.sns_obj import sns_notification
from utils.secrets_util import SecretsUtil
from utils.s3_util import S3Client

secrets_util = SecretsUtil()
SNS_FAILURE_TOPIC = 'TODO'
s3_client = S3Client()


class catchpoint:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

               Arguments:
                   api_params-- Required parameters fetched from DynamoDB
                  source_id -- Uniqud id assigned to particular data source and file pattern
               """

        try:
            # get client_id, client_secret from secret manager
            secret_manager_key = api_params['secret_manager_key']
            client_id = secrets_util.get_secret(secret_manager_key, 'client_id')
            client_secret = secrets_util.get_secret(secret_manager_key, 'client_secret')
            grant_type = secrets_util.get_secret(secret_manager_key, 'grant_type')
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
        bucket_key_folder = api_params['bucket_key_folder']
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1), date_format)
        token_url = api_params['token_url']
        catchpoint_url = api_params['api_url']
        target_bucket = api_params['target_bucket']
        parameters = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": grant_type
        }
        try:
            # make a call to token endpoint to get access token required to get response from API
            token_response = requests.post(url=token_url,
                                           data=parameters)
        except Exception as e:
            message = (
                f"Exception: {e} \nMessage: request for token response failed"

            )
            sns_notification(
                SNS_FAILURE_TOPIC,
                message,
                "General exception occurred."
            )
        # Retrieve access token from API response
        token = token_response.json()
        access_token = token['access_token']

        # make a call to API end point to get response
        auth = {"Authorization": "Bearer " + access_token}
        try:
            data_response = requests.get(
                url=catchpoint_url, headers=auth)
        except Exception as e:
            message = (
                f"Exception: {e} \nMessage: request for catchpoint response failed"
            )
            sns_notification(
                SNS_FAILURE_TOPIC,
                message,
                "General exception occurred."
            )

        data = json.loads(data_response.content)
        # Writing the file to tmp folder

        with open('/tmp/catchpoint.json', 'w') as json_file:
            json.dump(data, json_file)
        try:
            # writing raw data to raw S3 bucket
            s3_client.move_from_tmp_to_bucket("/tmp/catchpoint.json",
                                              bucket_key_folder + "/" + "WEB_PERFORMANCE_CATCHPOINT_{}.json".format(
                                                  yesterday_date), target_bucket)
        except Exception as e:
            message = (
                f"Exception: {e} \nMessage:  catchpoint data could not be"
                f"uploaded to S3 bucket {target_bucket}"
                "\nFunction name: catchpoint"
            )
            sns_notification(
                SNS_FAILURE_TOPIC,
                message,
                "General exception occurred."
            )