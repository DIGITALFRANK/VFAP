import xml.etree.ElementTree as ET
from io import StringIO
from datetime import datetime, timedelta

from external_lib import requests
from utils.dynamo_util import DynamoUtils
from utils.secrets_util import SecretsUtil
import sys
from utils.s3_util import S3Client
from utils.sns_obj import sns_notification
import os
SNS_FAILURE_TOPIC = 'TODO'

secrets_util = SecretsUtil()
dynamo_db = DynamoUtils()
s3_client = S3Client()



class coremetrics:

    @staticmethod
    def call_api(api_params, source_id):
        """This function makes call to API end point, gets the raw data and push to raw s3 bucket

                  Arguments:
                      api_params-- Required parameters fetched from DynamoDB
                     source_id -- Uniqud id assigned to particular data source and file pattern
                  """

        try:
            # get client_id, user_auth_key from secret manager
            secret_manager_key = api_params['secret_manager_key']
            client_id = secrets_util.get_secret(secret_manager_key, 'client_id')
            user_name = secrets_util.get_secret(secret_manager_key, 'user_name')
            user_auth_key = secrets_util.get_secret(secret_manager_key, 'user_auth_key')
        except Exception as e:
            print(f"Exception: {e} \nMessage: secret value cannot be fetched")
        #get required parameters from DynamoDB
        api_params_source_filename = api_params["api_params_filename"]
        bucket_key_folder = api_params['bucket_key_folder']
        target_bucket = api_params['target_bucket']
        #Get viewid required from file located in S3
        body = s3_client.read_file_from_s3(target_bucket, api_params_source_filename)
        json_view = eval(body)
        view_id_list = json_view['coremetrics']['ViewID']
        filename_list = json_view['coremetrics']['FileName']
        device_category = json_view['coremetrics']['DeviceCetegory']
        format = api_params['format']
        language = api_params['language']
        url = api_params['api_url']
        date_format = api_params['dateformat']
        yesterday_date = datetime.strftime(datetime.now() - timedelta(1),
                                           date_format)  # yesterday's date for appending in filename  
        file = '/tmp/coremetrics_root.xml'
        file1 = '/tmp/coremetrics.xml'
        if os.path.exists(file):
            os.remove(file)
            print("Removed the file %s" % file)
        if os.path.exists(file1):
            os.remove(file1)
            print("Removed the file %s" % file1)
        # make a call to API end point to get response
        for i in range(0, len(view_id_list)):

            params = {'clientId': client_id,
                      'username': user_name,
                      'format': format,
                      'userAuthKey': user_auth_key,
                      'language': language,
                      'viewID': view_id_list[i],
                      'period_a': 'D{}'.format(yesterday_date),
                      'fileName': filename_list[i] + str(yesterday_date) + '.json'
                      }
            try:
                response = requests.get(url=url, params=params)
                tree = ET.parse(StringIO(response.content.decode('utf-8')))
                root = tree.getroot()
                doc = ET.SubElement(root, "View")
                ET.SubElement(doc, "viewid", name="view_id").text = view_id_list[i] #device_category[i] #view_id_list[i]
                new_xml_tree_string = ET.tostring(tree.getroot()).decode('utf-8')
                print(new_xml_tree_string)
                with open('/tmp/coremetrics.xml', 'a') as f:
                    f.write(new_xml_tree_string)
                with open('/tmp/coremetrics.xml', 'r') as f, open('/tmp/coremetrics_root.xml', 'w') as g:
                    g.write("<root>{}</root>".format(f.read()))

                #writing raw data to raw S3 bucket
                s3_client.move_from_tmp_to_bucket("/tmp/coremetrics_root.xml",
                                                  bucket_key_folder + "/" + "COREMETRICS_VANS_CM_{}.xml".format(
                                                      yesterday_date), target_bucket)
                #tree.write("/tmp/coremetrics" + str(i) + '.xml')

				                #s3_client.move_from_tmp_to_bucket("/tmp/coremetrics{}.xml".format(i),
                                                  #bucket_key_folder + "/"  + yesterday_date + "/COREMETRICS_VANS_CM_{}_{}_{}.xml".format(
                                                      #yesterday_date, device_category[i], view_id_list[i]),
                                                 # target_bucket)
            except Exception as e:
                message = (
                    f"Exception: {e} \nMessage: File coremetrics could not be"
                    f"uploaded to S3 bucket {target_bucket}"
                    "\nFunction name: coremetrics"
                )
                sns_notification(
                    SNS_FAILURE_TOPIC,
                    message,
                    "General exception occurred."
                )