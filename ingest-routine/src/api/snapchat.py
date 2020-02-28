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


class snapchat:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

                  Arguments:
                      api_params-- Required parameters fetched from DynamoDB
                     source_id -- Uniqud id assigned to particular data source and file pattern
                  """
        # getting secret keys from secret manager
        try:
            secret_manager_key = api_params['secret_manager_key']
            api_key = secrets_util.get_secret(secret_manager_key, 'api_key')
        except Exception as e:
            message = (
                f"Exception: {e} \nMessage: secret value cannot be fetched"

            )
            sns_notification(
                SNS_FAILURE_TOPIC,
                message,
                "General exception occurred."
            )
            
        file = '/tmp/snapchat_stories.json'
        if os.path.exists(file):
            os.remove(file)
        file1 = '/tmp/snapchat_snaps.json'
        if os.path.exists(file1):
            os.remove(file1)
        date_format = api_params['dateformat']
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1), date_format)
        #get required parameters from DynamoDb
        bucket_key_folder = api_params['bucket_key_folder']
        url = api_params['api_url']
        target_bucket = api_params['target_bucket']
        auth = {"api-token": api_key}
        response = requests.get(url=url + "/accounts?", headers=auth)
        response_data = json.loads(response.content)
        #print(response_data)
        snapchat_stories_id_list = [i['id'] for i in response_data['accounts']]
        snapchat_brand = [i['snapchatUsername'] for i in response_data['accounts']]
        # getting the snapchat stories looping through the ids
        final_lst = []
        final_item_lst = []
        for j in range(0, len(snapchat_stories_id_list)):
            snapchat_stories_response = requests.get(
                url=url + "/accounts/{}/stories?".format(snapchat_stories_id_list[j]), headers=auth)
            snapchat_stories_response_data = json.loads(snapchat_stories_response.content)
            snapchat_stories_response_data['brand'] = snapchat_brand[j]
            final_lst.append(snapchat_stories_response_data)
        with open('/tmp/snapchat_stories.json', 'a') as json_file:
            json.dump(final_lst, json_file)
        s3_client.move_from_tmp_to_bucket(
            '/tmp/snapchat_stories.json',
            bucket_key_folder +  "/STORIES_OUTPUT_SNAPCHAT_{}.json".format(yesterday_date),
            target_bucket
        )
        # getting the snaps looping through the group ids
        for j in range(0, len(snapchat_stories_id_list)):
            snapchat_stories_response = requests.get(
                url=url + "/accounts/{}/stories?".format(snapchat_stories_id_list[j]), headers=auth)
            snapchat_stories_response_data = json.loads(snapchat_stories_response.content)
            snapchat_id_list = [i['id'] for i in snapchat_stories_response_data['stories']]
            for i in range(0, len(snapchat_id_list)):
                snapchat_snaps_response = requests.get(url=url + "/stories/{}/items?".format(snapchat_id_list[i]),
                                                       headers=auth)
                snapchat_snaps_response_data = json.loads(snapchat_snaps_response.content)
                snapchat_snaps_response_data['brand'] = snapchat_brand[j]
                final_item_lst.append(snapchat_snaps_response_data)
        # Writing the file to tmp folder
        try:
            with open('/tmp/snapchat_snaps.json', 'a') as json_file:
                json.dump(final_item_lst, json_file)
            #writing the file to s3 bucket
            s3_client.move_from_tmp_to_bucket(
                '/tmp/snapchat_snaps.json',
                bucket_key_folder + "/SINGLE-BRAND_SNAPCHAT_{}.json".format( yesterday_date),
                target_bucket
            )
        except Exception as e:
            # message = (
            #     f"Exception: {e} \nMessage: File coremetrics could not be"
            #     f"uploaded to S3 bucket {target_bucket}"
            #     "\nFunction name: coremetrics"
            # )
            # sns_notification(
            #     SNS_FAILURE_TOPIC,
            #     message,
            #     "General exception occurred."
            # )
            print(f"Exception: {e} \nMessage: Snapchat data could not be"
                  f"uploaded to S3 bucket {target_bucket}"
                  "\nFunction name: snapchat")
