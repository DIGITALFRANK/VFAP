"""
Module description: This module is a generic sftp file downloader deployed in AWS Lambda.
                    This module gets parameters from DynamoDB config table, connects to SFTP server,
                    download the files based on file pattern and push it to
                    S3 bucket defined in config table.
                    Notifications are managed through SNS topics. Process and Error details being logged
                    in DynamoDB table
"""

import hashlib
import logging
import os
import sys
from utils.sns_obj import sns_notification

from boto3.dynamodb.conditions import Attr
from datetime import date, datetime, timedelta
import time
from time import gmtime, strftime

from utils.dynamo_util import DynamoUtils
from utils.s3_util import S3Client
from utils.sftp_util import SFTPUtil
dynamo_db = DynamoUtils()
s3 = S3Client().conn
# set logging level to INFO
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# get environment state from runtime environment variables, fall back to dev
# if no value is provided.
environment = os.environ.get('environment', 'dev')

file_table_name = f'vf-{environment}-filelog'
process_log = dynamo_db.connect_to_table(f'vf-{environment}-processlog')
print(process_log)
file_table = dynamo_db.connect_to_table(file_table_name)
now = datetime.now()

SNS_SUCCESS_TOPIC = 'TODO'
SNS_FAILURE_TOPIC = 'TODO'


def transfer_file_from_sftp_to_s3(source_name, destination_s3_bucket,
                                  source_directory, file_pattern, hostname,
                                  port, username, password, ftp_type, ftp_mode,
                                  tempfolder, s3_key_folder, source_id,
                                  overwriteflag, requestid):
    try:

        ftp_connection = SFTPUtil(hostname, port, username, password)
        ftp_conn = ftp_connection.connect(ftp_type)
        logging.info(ftp_connection)
        files = ftp_connection.get_file_list(source_directory, file_pattern, ftp_mode)
    except:
        print("request failed")

    try:
        # update_process_details(
        #     requestid,
        #     "upload",
        #     f"upload files to {destination_s3_bucket}",
        #     "started",
        #     now
        # )
        for file in files:
            server_file_path = os.path.join(source_directory, file)
            ftp_conn.get(server_file_path, tempfolder + file)
            checksum = md5Checksum(tempfolder + file)
            check = check_fileexists_checksum(checksum, file)
            if not check_fileexists_checksum(checksum, file):#Note:- this function checks the dynamodb table(vf-dev-filelog) to find weather the file is already created if it finds the checksum colums it will not write thr file to s3
                if check_fileexists(checksum, file):# Note:- this function checks weather the required file has already been processed by Module or not
                    if int(overwriteflag) == 1:
                        time.sleep(10)
                        #Note:- if all the conditions are satified it write to s3
                        s3.upload_file(tempfolder + file,
                                destination_s3_bucket,
                                s3_key_folder + "/" + file
                        )
                        # message = (
                        #     f"Message: File {file} from {source_name} is already exist on {destination_s3_bucket}"
                        #     f" S3 bucket but data within the file has been modified. "
                        #     f"Downloading the file {file} again as overwrite flag is set."
                        #     f"\nTriggered: {strftime('%Y-%m-%d %H:%M:%S', gmtime())}"
                        # )
                        # sns_notification(
                        #     SNS_SUCCESS_TOPIC,
                        #     message,
                        #     f"File from {source_name} already exists in {destination_s3_bucket}"
                        # )

                        #Note:- after writing it to the s3 an updation happens in the dynamodb table (vf-dev-filelog)
                        file_table.update_item(
                            Key={
                                'file_name': file
                            },
                            UpdateExpression='SET checksum = :val1',
                            ExpressionAttributeValues={
                                ':val1': checksum
                            }
                        )
                    else:
                        message = (
                            f"Message: File {file} from {source_name} is already exist on "
                            f"{destination_s3_bucket}"
                            f" S3 bucket but data within the file has been modified. "
                            f"Skipping the file {file} as overwrite flag is not set."
                            f"\nTriggered: {strftime('%Y-%m-%d %H:%M:%S', gmtime())}"
                        )
                        sns_notification(SNS_SUCCESS_TOPIC, message,
                                         f"File from {source_name} already exist on {destination_s3_bucket}")
                else:
                    print(tempfolder + file,destination_s3_bucket,s3_key_folder + "/" + file)
                    time.sleep(10)
                    s3.upload_file(tempfolder + file, destination_s3_bucket, s3_key_folder + "/" + file)
                    file_table.put_item(
                        Item={'file_name': file, 'batchid': requestid, 'source_id': source_id, 'checksum': checksum,
                              'overwritecounter': 0})

                    # message = (
                    #     f"Message: File {file} from {source_name} has been copied to S3 Bucket "
                    #     f"{destination_s3_bucket} successfully."
                    #     f"\nTriggered: {strftime('%Y-%m-%d %H:%M:%S', gmtime())}"
                    # )
                    # sns_notification(
                    #     SNS_SUCCESS_TOPIC,
                    #     message,
                    #     f"File from {source_name} successfully copied to {destination_s3_bucket}"
                    # )
            else:
                pass
                # message = (
                #     f"Message: File {file} from {source_name} already exist on S3 Bucket "
                #     f"{destination_s3_bucket}. Skipping the download."
                #     f"\nTriggered: {strftime('%Y-%m-%d %H:%M:%S', gmtime())}"
                # )
                # sns_notification(
                #     SNS_SUCCESS_TOPIC,
                #     message,
                #     f"File from {source_name} already exist on {destination_s3_bucket}"
                # )
        ftp_conn.close()
        # update_process_details(
        #     requestid,
        #     "upload",
        #     f"upload files to {destination_s3_bucket}",
        #     "completed",
        #     now
        # )  # bucket name
    except Exception as e:
        message = (
            f"Exception: {e} \nMessage: File {file} could not be"
            f"uploaded to S3 bucket {destination_s3_bucket}"
            "\nFunction name: transfer_file_from_sftp_to_s3"
        )
        sns_notification(
            SNS_FAILURE_TOPIC,
            message,
            "General exception occurred."
        )
        return " "



# Based on file_pattern, generate file name required to fetch from SFTP server. It loops for n - today and get list
# of all unprocessed files in SFTP server
def get_exact_file_name(file_pattern, delta_date_no):
    try:
        first_idx = file_pattern.find('%')
        second_idx = file_pattern.find('%', first_idx + 1)
        third_idx = file_pattern.find('%', second_idx + 1)
        first_date_value = file_pattern[first_idx:second_idx]
        second_date_value = file_pattern[second_idx:third_idx]
        third_date_value = file_pattern[third_idx + 1]
        file_extension = file_pattern[file_pattern.rfind('.'):]
        file_date_format = first_date_value + second_date_value + '%' + third_date_value
        file_string = file_pattern[0: first_idx]
        yesterday = date.today() - timedelta(days=delta_date_no - 1)
        date_format = yesterday.strftime(file_date_format)
        filename = file_string + date_format + file_extension


        return filename
    except Exception as e:
        message = (
            f"Exception: {str(e)}\nMessage: {file_pattern} does not contain proper date format."
            f"\nFunction name: get_exact_file_name"
        )
        sns_notification(SNS_FAILURE_TOPIC, message, "General exception occurred.")


# Calculates checksum for file to compare if data in file has been modified or not
def md5Checksum(server_file_path):
    try:
        with open(server_file_path, 'rb') as fh:
            m = hashlib.md5()
            while True:
                data = fh.read(8192)
                if not data:
                    break
                m.update(data)
            return m.hexdigest()
    except Exception as e:
        message = (
            f"Exception: {str(e)}\nMessage: Failed in calculating checksum value."
            "\nFunction name: md5Checksum"
        )
        sns_notification(SNS_FAILURE_TOPIC, message, "General exception occurred.")


# check if file  and data within file already exists in SFTP/FTP server
def check_fileexists_checksum(checksum, filename):
    try:
        response = file_table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Attr('checksum').eq(checksum) & Attr('file_name').eq(filename))
        print("res",response)

        if response['Count'] == 1:
            return True
        else:
            return False
    except Exception as e:
        message = (
            f"Exception: {str(e)}\nMessage: Failed in calculating "
            f"checksum value.\nFunction name: check_fileexists_checksum"
        )
        sns_notification(
            SNS_FAILURE_TOPIC,
            message,
            "General exception occurred."
        )


# check if required file has already been processed by Module or not
def check_fileexists(checksum, filename):
    try:
        response = file_table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Attr('checksum').ne(checksum) & Attr('file_name').eq(filename))
        if response['Count'] == 1:
            update = file_table.update_item(
                Key={'file_name': filename},
                UpdateExpression='SET overwriteflag = :val1',
                ExpressionAttributeValues={
                    ':val1': 1
                }
            )
            return True
        else:
            return False

    except Exception as e:
        message = (
            f"Exception: {str(e)}\nMessage: Failed in calculating "
            f"checksum value.\nFunction name: check_fileexists"
        )
        sns_notification(
            SNS_FAILURE_TOPIC,
            message,
            "General exception occurred."
        )


# This function puts a record in process log table along with BatchID of Lambda run
def put_process_tbl(requestid, source_id, process_name, process_status, prefix, now):
    date = now.strftime("%d/%m/%Y")
    process_log.put_item(
        Item={
            'request_id': requestid,
            'source_id': source_id,
            'date': date,
            'process_info': {
                f'{prefix}_process_name': process_name,
                f'{prefix}_process_status': process_status,
                f'{prefix}_startdatetime': now.strftime("%d/%m/%Y %H:%M:%S"),
                f'{prefix}_enddatetime': now.strftime("%d/%m/%Y %H:%M:%S")
            },
            'error_info': {}
        }
    )


# This function updates the progress of process in process log table  for particular BatchID of Lambda run
def update_process_details(requestid, prefix, process_name, process_status, now):
    print("requestid", requestid)
    print("prefix", prefix)
    print("process name", process_name)
    print("process status", process_status)
    print("now", now)

    process_log.update_item(
        Key={
            'request_id': requestid
        },

        UpdateExpression=(
            f"set process_info.{prefix}_process_name = :r,"
            f"process_info.{prefix}_process_status=:a,"
            f"process_info.{prefix}_startdatetime=:b,"
            f"process_info.{prefix}_enddatetime =:c"
        ),
        ExpressionAttributeValues={
            ':r': process_name,
            ':a': process_status,
            ':b': now.strftime("%d/%m/%Y %H:%M:%S"),
            ':c': now.strftime("%d/%m/%Y %H:%M:%S")
        },
        ReturnValues="UPDATED_NEW"
    )


# This function updates the error details in process log table for particular BatchID of Lambda run
def update_error_details(requestid, error_message, error_type, now):
    process_log.update_item(
        Key={
            'request_id': requestid
        },
        UpdateExpression=(
            f"set error_info.ErrorMessage = :x,error_info.Error_Type=:y,"
            f"error_info.startdate=:z,error_info.enddate=:m"
        ),
        ExpressionAttributeValues={
            ':x': error_message,
            ':y': error_type,
            ':z': now.strftime("%d/%m/%Y %H:%M:%S"),
            ':m': now.strftime("%d/%m/%Y %H:%M:%S")
        },
        ReturnValues="UPDATED_NEW"
    )


def get_filepattern_list(file_pattern, delta_date_no):
    file_pattern_list = []
    for i in range(delta_date_no):
        file_pattern_new = get_exact_file_name(file_pattern, i + 1)
        file_pattern_list.append(file_pattern_new)
    return file_pattern_list
