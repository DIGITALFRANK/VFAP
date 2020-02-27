import json
import os
import boto3
import config.config as config
from datetime import datetime
from utils.dynamo_util import DynamoUtils
from botocore.exceptions import ClientError
import configparser
import base64
#environment = os.environ.get('environment', 'dev')
environment = os.environ['environment']
config_table_name = f'vf-{environment}-etl-file-broker'

class utils:
    def __init__(self):
        pass


    @staticmethod
    def get_dynamic_class(classname):
        """This function is created to dynamically import the class
        for a given string

        Arguments:
            classname {[String]} -- [description]

        Returns:
            [class] -- Returns a class which should be defined in modules.transforms or
            modules.dq
        """
        mod = __import__("api." +
                         classname, fromlist=[classname])
        klass = getattr(mod, classname)
        return klass

    @staticmethod
    def get_file_config_params(filename):
        # import ipdb;ipdb.set_trace()
        """This function is created to get the params replated to file from dynamodb

        Arguments:
            filename {String} -- filename for which params to be fetched
            logger {logger} -- logger object

        Returns:
            [dict] -- Returns a dictionary of parameters from dynamo DB
        """
        file_params = None
        if filename.find('ATTRIBUTION') or filename.find('WeatherTrends') != -1:
            file_parts = filename.split("_")
            sort_key = "_".join(file_parts[0:-1]) + "_"
            partition_key = file_parts[2]
            print(partition_key)
            print(sort_key)
        else:
            print(filename)
            file_parts = filename.split("_")
            sort_key = "_".join(file_parts[0:3]) + "_"
            partition_key = file_parts[2]
            print(partition_key)
            print(sort_key)

        file_params = DynamoUtils.get_dndb_item(
            partition_key_atrr=config.FILE_CONFIG_SORT_KEY_ATTRIBUTE,
            partition_key_value=sort_key,
            table=config_table_name
        )
        print(file_params)
        return file_params
