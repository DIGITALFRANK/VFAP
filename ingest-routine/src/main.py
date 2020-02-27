"""
Module description: This module is a generic ingestion handler for Lambda.
                    This module gets parameters from DynamoDB config table,
                    connects to the correct server, download the files or API
                    information from the client and pushes the information to
                    an S3 bucket defined in config table.
                    Notifications are managed through SNS topics.
                    Process and Error details being logged in DynamoDB table.
"""

# import all required libraries for the module


import logging
import os
import sys
import boto3
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             'external_lib')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             'api')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             'sftp')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             'utils')))


from utils.dynamo_util import DynamoUtils
from utils.secrets_util import SecretsUtil
from utils.api_utils import utils
from sftp import get_filepattern_list, transfer_file_from_sftp_to_s3
secrets_util = SecretsUtil()
dynamo_db = DynamoUtils()

environment = os.environ.get('environment', 'dev')
file_table_name = f'vf-{environment}-filelog'

session = boto3.session.Session()
client = session.client('secretsmanager')
sns = boto3.client('sns')

"""
Function description :
This is main Lambda function and source id is being passed as event to function.
This function calls all other functions which would connect to SFTP/FTP server and push files to S3 bucket,
or alternatively if the source is an external API, this code calls out to the handler for that.
This function handles duplicate files and same file with modified data. Handling duplicate files is being
managed by overwriteflag driven from DynamoDB
"""

def lambda_handler(event, context):
    requestid = context.aws_request_id
    #requestid = 12345
    source_id = event['source_id']
    print(source_id)
    config_table_name = f'vf-{environment}-configtbl'

    # Connect to DynamoDB table and assign parameters to variables
    table = dynamo_db.connect_to_table(config_table_name)
    response = table.get_item(
        Key={
            'source_id': source_id
        }
    )

    #get the response from DynamoDB table
    details = response.get('Item').get('conn_details')
    # details = json.loads(details)
    input_type = details.get('input_type')
    source_file_pattern = details.get('file_pattern')
    overwriteflag = details.get('overwriteflag')
    delta_date_no = details.get('delta_date_no')
    tempfolder = '/tmp/'
    if input_type == 'sftp':
        file_pattern_list = get_filepattern_list(source_file_pattern, int(delta_date_no))
        source_name = details.get('source_name')
        hostname = details.get('host_name')
        username = details.get('host_username')
        secret_manager_key = details.get('secret_manager_key')
        password = secrets_util.get_secret(secret_manager_key, 'secret_key')
        port = int(details.get('port_no'))
        source_directory = details.get('host_dir')
        ftp_mode = details.get('ftpmode', '')
        ftp_type = input_type #details.get('ftptype')
        destination_s3_bucket = details.get('target_bucket')
        s3_key_folder = details.get('bucket_key_folder')
        for file_pattern in file_pattern_list:
            transfer_file_from_sftp_to_s3(source_name, destination_s3_bucket, source_directory, file_pattern, hostname,
                                          port, username, password, ftp_type, ftp_mode, tempfolder,
                                          s3_key_folder, source_id, overwriteflag,requestid)
    elif input_type == 'api':
        print(details)
        classname = details.get('classname')
        #get the claseenmae for API dynamically based on sourceid
        api_class = utils.get_dynamic_class(classname,)
        api_class.call_api(details, source_id)
    elif input_type == 'ftp':
        pass
    elif input_type == 's3':
        pass
    elif input_type == 'email':
        pass

    return "success"

#lambda_handler({"source_id":1009}, "some context")