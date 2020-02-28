import json
import boto3
import os
import time
from utils.api_utils import utils
from utils.get_refined import filename
from datetime import datetime
s3 = boto3.client('s3')


def lambda_handler(event, context):
    #getting the file name from s3 bucket
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    obj = event.get('Records')[0].get('s3').get('object')
    #Checking the file size of the folder
    if not int(obj.get('size', 0)):
        return False
    foldername = event['Records'][0]['s3']['object']['key']
    if foldername.find('WeatherTrends') != -1:
        file_name = foldername.split('/')[4]
        date = foldername.split('/')[3]
        # print(date[7:15])
    else:
        file_name = foldername.split('/')[2]
    #getting the feed name of the data source to get dynamoDB records
    refined_file = filename.get_refined(file_name)
    #Fetching the parameters in DynamoDB record
    params = utils.get_file_config_params(refined_file)
    source_key_name = params['raw_source_file_name'] + file_name
    parsing_class = utils.get_dynamic_class(params['parsing_classname'])
    if file_name.find('Extract') != -1:
        parsing_class.parser(params['raw_source_bucket'], source_key_name, params['rf_dstn_bucket'])
    elif file_name.find('reports') != -1:
        parsing_class.parser(params['raw_source_bucket'], source_key_name, params['rf_dstn_bucket'])
    elif file_name.find('WeatherTrends') != -1:
        source_key_name = params['raw_source_file_name'] + date + "/" + file_name
        print("Key name: " + source_key_name)
        file, date = parsing_class.parser(params['raw_source_bucket'], source_key_name)
        dstn_file_name = params['rf_dstn_filename'] + date +'.'+params['raw_source_filetype']
        #Uploading the csv in temp folder to destination s3 bucket
        s3.upload_file(file, params['rf_dstn_bucket'], dstn_file_name)
        #Removing the file stored in /tmp folder
        if os.path.exists(file):
            os.remove(file)
        print("Removed the file %s" % file)
    else:
        print("Key name: " + source_key_name)
        file, date = parsing_class.parser(params['raw_source_bucket'], source_key_name)
        dstn_file_name = params['rf_dstn_filename'] + date +'.'+params['raw_source_filetype']
        #Uploading the csv in temp folder to destination s3 bucket
        s3.upload_file(file, params['rf_dstn_bucket'], dstn_file_name)
        #Removing the file stored in /tmp folder
        if os.path.exists(file):
            os.remove(file)
        print("Removed the file %s" % file)
    return True
