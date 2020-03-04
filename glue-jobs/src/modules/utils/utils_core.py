import json
import sys
import base64
import boto3
import configparser
import modules.config.config as config
from datetime import datetime
from modules.utils.utils_dynamo import DynamoUtils
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType
from modules.config import config
import pg8000
from modules.exceptions.AppUtilsException import AppUtilsError
from modules.constants import constant
import traceback
import logging


class utils:
    # logger = logging.getLogger(__name__)
    def __init__(self):
        pass

    @staticmethod
    def get_current_datetime_folder():
        now = datetime.now()
        s3_folder = (
            str(now.year)
            + "%02d" % now.month
            + "%02d" % now.day
            # + "%02d" % now.hour
            # + "%02d" % now.minute
            # + "%02d" % now.second
        )
        print("Folder created in S3 as {}".format(s3_folder))
        return s3_folder

    @staticmethod
    def s3_folder_delete(bucket_name, obj_path):
        try:
            date_ly = (datetime.date(datetime.now()) - timedelta(days=1)).strftime(
                "%d-%m-%Y"
            )
            obj_path = obj_path + "/" + date_ly + "/"
            s3 = boto3.resource("s3")
            bucket = s3.Bucket(bucket_name)
            bucket.objects.filter(Prefix=obj_path).delete()
            print("succesfully deleted the {} older".format(obj_path))
            write_status = True
        except Exception as error:
            write_status = False
            print(
                "Error Ocuured While processiong s3_folder_delete due to : {}".format(
                    error
                )
            )
        return write_status

    @staticmethod
    def is_file_exists():
        client = boto3.client("s3")
        bucket = "cc-poc-bucket-vf"
        s3_key = "input-source/raw_data/24-10-2019/"
        content = client.head_object(Bucket=bucket, Key=s3_key)
        if content.get("ResponseMetadata", None) is not None:
            file_exists = True
        else:
            file_exists = False
        return file_exists

    @staticmethod
    def get_dynamic_class(classname, logger):
        """This function is created to dynamically import the class
        for a given string

        Arguments:
            classname {[String]} -- [description]

        Returns:
            [class] -- Returns a class which should be defined in modules.transforms or
            modules.dq
        """
        if classname.startswith("tr"):
            mod = __import__("modules.transforms." + classname, fromlist=[classname])
            klass = getattr(mod, classname)
        elif classname.startswith("dq"):
            mod = __import__("modules.dq." + classname, fromlist=[classname])
            klass = getattr(mod, classname)
        elif classname.startswith("map"):
            mod = __import__("modules.map." + classname, fromlist=[classname])
            klass = getattr(mod, classname)
        else:
            klass = None
            raise NotImplementedError

        return klass

    @staticmethod
    def read_parameter_from_ddb(key, logger):
        """This function is created to read parameters from dynamo DB using boto3 client

        Arguments:
            key {String} -- A Key that must be configured beforehand in control table
            logger {logger} -- logger object

        Returns:
            [dict] -- Returns a dictionary of parameters from dynamo DB
        """
        # *TODO : Parametrize this piece later with config.py params
        client = boto3.client("dynamodb", region_name=config.REGION)
        try:
            obj = client.get_item(TableName=config.FILE_BROKER_TABLE, Key=key)
            params = json.loads(json.dumps(obj["Item"]))
        except Exception as error:
            logger.error("Error Occurred {}".format(error))
        return params

    @staticmethod
    def get_file_config_params(filename, logger):
        """This function is created to get the params replated to file from dynamodb

        Arguments:
            filename {String} -- filename for which params to be fetched
            logger {logger} -- logger object

        Returns:
            [dict] -- Returns a dictionary of parameters from dynamo DB
        """
        file_params = None
        broker_table = utils.get_param_store_configs(utils.get_parameter_store_key())['config_table']
        if filename.__contains__("xref") or filename.__contains__("map"):
            logger.debug("Get Weekly job configuration")
            file_parts = filename.split("_")

            # for map i/p map_name will be weekly_map_coremetrics_20192012
            if file_parts[0] == "weekly" and file_parts[1] == "map":
                partition_key = "_".join(filename.split("_")[1:-1])
            else:
                partition_key = "_".join(filename.split("_")[0:-1])
            file_params = DynamoUtils.get_dndb_item(
                partition_key_atrr=config.FILE_BROKER_PARTITION_KEY,
                partition_key_value=partition_key,
                sort_key_attr=None,  # config.FILE_BROKER_SORT_KEY_ATTRIBUTE,
                sort_key_value=None,  # sort_key,
                table=broker_table,
                logger=logger,
            )
            logger.info("File parameters for weekly job is {}".format(file_params))
        elif filename.startswith("reporting"):
            file_parts = filename.split("_")
            partition_key = "_".join(file_parts[1:])  # file_parts[2]
            logger.info("File Broker Params for File : {}".format(filename))
            logger.info(
                "fetching broker table with primary key : {} and ".format(partition_key)
            )
            file_params = DynamoUtils.get_dndb_item(
                partition_key_atrr=config.FILE_BROKER_PARTITION_KEY,
                partition_key_value=partition_key,
                sort_key_attr=None,  # config.FILE_BROKER_SORT_KEY_ATTRIBUTE,
                sort_key_value=None,  # sort_key,
                table=broker_table,
                logger=logger,
            )
            logger.info("File parameters for file are as : {}".format(file_params))
        else:
            try:
                file_parts = filename.split("_")
                # file_parts = filename.split("_")
                # if file_parts[0] == "weekly":
                #     partition_key = "_".join(filename.split("_")[1:])
                #
                # else:

                partition_key = "_".join(file_parts[0:-1])  # file_parts[2]
                # checking condition if filename date part with hhmmss or not,if not taking yymmdd
                if len(file_parts[-1].split(".")[0]) < 14:
                    date_format = "%Y%m%d"
                else:
                    date_format = config.FILE_DATE_FORMAT
                file_date = datetime.strptime(file_parts[-1].split(".")[0], date_format)

                # sort_key = "_".join(file_parts[0:-1]) + "_"
                # partition_key = file_parts[2]

                # get date from filename

                logger.info("File Broker Params for File : {}".format(filename))
                logger.info(
                    "fetching broker table with primary key : {} and ".format(
                        partition_key
                    )
                )
                file_params = DynamoUtils.get_dndb_item(
                    partition_key_atrr=config.FILE_BROKER_PARTITION_KEY,
                    partition_key_value=partition_key,
                    sort_key_attr=None,  # config.FILE_BROKER_SORT_KEY_ATTRIBUTE,
                    sort_key_value=None,  # sort_key,
                    table=config.FILE_BROKER_TABLE,
                    logger=logger,
                )
                # Adding file_date as attribute to ddb params using in tr_weather_historical

                if file_parts[0] != "weekly":
                    file_params["file_date"] = file_date
                logger.info("File parameters for file are as : {}".format(file_params))
            except Exception as error:
                logger.error(
                    "Error Ocurred while get_file_config_params  due to : {}".format(
                        error
                    ),
                    exc_info=True,
                )
                file_params = None
                raise AppUtilsError(
                    moduleName=constant.CORE_UTILS,
                    exeptionType=constant.CORE_UTILS_EXCEPTION,
                    message="Error Ocurred while get_file_config_params  due to : {}".format(
                        traceback.format_exc()
                    ),
                )
        return file_params

    @staticmethod
    def upload_file(file_name, bucket, args=None):
        """Upload a file to an S3 bucket

        Arguments:
            file_name {String} -- File to upload
            bucket {String} -- Bucket to upload to

        Keyword Arguments:
            args {String} -- S3 object name. (default: {None})

        Returns:
            Boolean -- True if file was uploaded, else False
        """

        # Upload the file
        s3_client = boto3.client("s3")
        s3_obj = ""
        log_file_path = ""
        try:
            datetime_folder = utils.get_current_datetime_folder()
            s3_folder = datetime_folder + "/"
            if "FILE_NAME" in args and args["FILE_NAME"].__contains__("xref"):
                print("In if condition with arguments as {}".format(args))
                # ! Below line is used only to test on glue dev endpoint
                # args["JOB_RUN_ID"]=args["FILE_NAME"]
                s3_obj = (
                    config.LOG_DIR
                    + s3_folder
                    + args["FILE_NAME"]
                    + "/"
                    + args["JOB_RUN_ID"]
                    + ".log"
                )
            elif "JOB_PARAM" in args and args["JOB_PARAM"].__contains__("map"):
                s3_obj = (
                    config.LOG_DIR
                    + s3_folder
                    + args["JOB_PARAM"]
                    + "/"
                    + args["JOB_RUN_ID"]
                    + ".log"
                )

            else:
                s3_obj = (
                    config.LOG_DIR
                    + s3_folder
                    + args["FILE_NAME"].split(".")[0]
                    + "/"
                    + args["JOB_RUN_ID"]
                    + ".log"
                )
            print("s3 obj : {} ,file path s3://{}/{}".format(s3_obj, bucket, s3_obj))
            s3_client.upload_file(file_name, bucket, s3_obj)
            print("File uploaded successfully to S3")
            log_file_path = "s3://{}/{}".format(bucket, s3_obj)
            status = True

        except Exception as e:
            print("Error occured while uploading file to s3 {}".format(e))
            log_file_path = ""
            status = False
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error occured while uploading  file to s3 : {}".format(
                    traceback.format_exc()
                ),
            )
        return status, log_file_path

    @staticmethod
    def get_param_store_configs(param_name):
        """Get Parameters from AWS Parameter store

        Arguments:
            param_name {String} --Name of the Parameter defined in AWS Parameter store

        Returns:
            dict -- A dictionary of environment parameters
        """
        try:
            client = boto3.client("ssm", region_name="us-east-1")
            response = client.get_parameter(Name=param_name)
            params = json.loads(response["Parameter"]["Value"])
            print("Environment parameters are : {}".format(params))
        except Exception as error:
            print("Error occured .Check parameter key name {}".format(error))
            params = None
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Environment parameters are : {}".format(
                    traceback.format_exc()
                ),
            )
        return params

    @staticmethod
    def get_parameter_store_key():
        """This method parses the config_store.ini file to retrieve param store key

        Returns:
            String -- Return key name defined in config_store.ini as string
        """
        config = configparser.RawConfigParser()
        config.read("config_store.ini")
        try:
            print(
                "Found param store key as {}".format(config["PARAMSTORE"]["StoreKey"])
            )
            key = str(config["PARAMSTORE"]["StoreKey"])
        except Exception as e:
            key = None
            print("Failed to parse config_store.ini {}".format(e))
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Failed to parse config_store.ini {}".format(
                    traceback.format_exc()
                ),
            )
        return key

    @staticmethod
    def get_secret(secret_name, service_name="secretsmanager", region_name="us-east-1"):
        """Method to retrieve all the secrets from secrets manager

        Arguments:
            secret_name {str} -- Secret name defined in AWS secret manager

        Keyword Arguments:
            service_name {str} -- service name where secret is defined
                                (default: {"secretsmanager"})
            region_name {str} -- Region name where the secret is defined in AWS
                                 (default: {"us-east-1"})

        Raises:
            e: [description]
            e: [description]
            e: [description]
            e: [description]
            e: [description]

        Returns:
            dict -- Returns a  string if single secret is defined in SSM. Else returns a dict
        """
        client = boto3.client(service_name=service_name, region_name=region_name)
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the
                # provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current
                # state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these
            # fields will be populated.
            if "SecretString" in get_secret_value_response:
                secret = json.loads(get_secret_value_response["SecretString"])
            else:
                secret = json.loads(
                    base64.b64decode(get_secret_value_response["SecretBinary"])
                )

        return secret

    @staticmethod
    def get_glue_job_run_id():
        job_run_id = None
        try:
            args = getResolvedOptions(sys.argv, ["JOB_NAME"])
            job_run_id = args["JOB_RUN_ID"]
            print("Glue_job_run_id : {}".format(job_run_id))
        except Exception as error:
            print("Error Ocurred in get_glue_job_run_id due to : {}".format(error))
            job_run_id = None
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error Ocurred in get_glue_job_run_id due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return job_run_id

    @staticmethod
    def check_existence_of_redshift_table(
        spark, redshift_table, whouse_details, logger
    ):
        """
            Parameters:

            redshift_table: str,
            redshift_schema: str,
            redshift_url: str,
            redshift_user: str,
            redshift_password: str,
            spark_session: pyspark.sql.session,
            log: logging.Logger

            Returns:

            Bool

            This function checks the information_schema metadata store in Redshift for the existence of
            a user specified table in the public Redshift schema. Table must be qualified with schema.
        """
        redshift_user = whouse_details["username"]
        redshift_password = whouse_details["password"]
        redshift_schema = whouse_details["dbSchema"]
        logger.info("Attempting to create jdbc url for redshift")
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            whouse_details["engine"],
            whouse_details["host"],
            whouse_details["port"],
            whouse_details["dbCatalog"],
        )

        logger.info(
            "Checking to see if table {0}.{1} exists in Redshift".format(
                redshift_schema, redshift_table
            )
        )
        query = "SELECT table_name FROM information_schema.tables where  upper(table_schema)='{}' and upper(table_name)='{}'".format(
            redshift_schema.upper(), redshift_table.upper()
        )
        logger.info(query)
        tables_df = (
            spark.read.format("jdbc")
            .option("driver", "com.amazon.redshift.jdbc.Driver")
            .option("url", redshift_url)
            .option("query", query)
            .option("password", redshift_password)
            .option("user", redshift_user)
            .load()
        )
        if tables_df.count() == 1:
            logger.info(
                "Table {0}.{1} exists in Redshift".format(
                    redshift_schema, redshift_table
                )
            )
            return True

        else:
            logger.info(
                "Table {0}.{1} does not exist in Redshift".format(
                    redshift_schema, redshift_table
                )
            )
            return False

    @staticmethod
    def move_s3_file_from_current(file_name, src_bucket, tgt_bucket, params, logger):
        file_moved_status = False
        try:
            s3_client = boto3.client("s3")
            file_parts = file_name.split("_")
            feed_name = "_".join(file_name.split("_")[:-1])

            if len(file_parts[-1].split(".")[0]) < 14:
                date_format = "%Y%m%d"
            else:
                date_format = config.FILE_DATE_FORMAT

            file_date = datetime.strptime(file_parts[-1].split(".")[0], date_format)
            date_partition = file_parts[-1].split(".")[0][0:8]
            src_path = "{}/{}".format(params["rf_source_dir"], file_name)
            tgt_path = "{}{}/date={}/{}".format(
                params["rf_dstn_folder_name"], feed_name, date_partition, file_name
            )
            copy_source = {"Bucket": src_bucket, "Key": src_path}
            logger.info("Copying file from  {} to {}".format(src_path, tgt_path))
            file_moved_response = s3_client.copy(
                Bucket=tgt_bucket, CopySource=copy_source, Key=tgt_path
            )
            print("file_moved_response {}".format(file_moved_response))
            s3_client.delete_object(Bucket=src_bucket, Key=src_path)
            file_moved_status = True
        except Exception as error:
            file_moved_status = False
            logger.error(
                "Error Ocurred in move_s3_file_from_current due to : {}".format(error),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error Ocurred in move_s3_file_from_current due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return file_moved_status

    @staticmethod
    def check_day_of_week(job_type="xrefs"):
        """

        Keyword Arguments:
            job_type {str} -- [description] (default: {"weekly"})

        Returns:
            start -- Boolean value to show if weekly job should start.
                        Job should start on Friday once the daily jobs are completed.
        """
        weekday = datetime.today().weekday()
        if job_type.lower() == "daily":
            # ALL JOB SCHEDULES WILL BE HANDLED BY WORKFLOW
            if weekday <= 4:
                start = True
            else:
                # start = False
                start = True
        elif job_type.lower() == "xrefs":
            # **Jobs running on Fridays
            if weekday == 4:
                start = True
            else:
                start = True
        elif job_type.lower() == "maps":
            # **Jobs running on Saturdays
            if weekday == 5:
                start = True
            else:
                start = True
        else:
            start = True
        return start

    @staticmethod
    def convert_dynamodb_json_schema_to_struct_type_schema(dndb_json_schema, logger):
        """Convert the schema saved in dynamodb for every file of specific formaaat with
        column_name,column_index to struct type spark schema
        e.g schema format saved in dynamodb =>
        {'id': {'column_order': '1', 'data_type': 'int'}

        Returns:
            custom_schema(StuctTupe) : returns StructTypeSchema
        """
        custom_schema = None
        try:
            # get the sorted list of schema columns based on column_order
            sorted_schema_list = sorted(
                dndb_json_schema.items(), key=lambda kv: int(kv[1]["column_order"])
            )

            # convert sorted schema to spark json schema
            json_schema = {"type": "struct"}
            fileds = []
            for dtype in sorted_schema_list:
                json_type = {}
                json_type["name"] = dtype[0]
                json_type["type"] = config.data_type_dict[
                    dtype[1]["data_type"].lower().strip()
                ]
                json_type["nullable"] = True
                json_type["metadata"] = {}
                fileds.append(json_type)
            json_schema["fields"] = fileds
            # convert json schema to spark struct type schema
            struct_schema = StructType.fromJson(json_schema)
        except Exception as error:
            custom_schema = None
            logger.error(
                "Error Ocurred in convert_dynamodb_json_schema_to_struct_type_schema due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error Ocurred in convert_dynamodb_json_schema_to_struct_type_schema due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return struct_schema

    @staticmethod
    def execute_query_in_redshift(query, whouse_details, logger):
        """This method will go and execute the given query.
        """
        redshift_user = whouse_details["username"]
        redshift_password = whouse_details["password"]
        redshift_schema = whouse_details["dbSchema"]
        redshift_host = whouse_details["host"]
        redshift_port = whouse_details["port"]
        redshift_database = whouse_details["dbCatalog"]

        query_run_status = False
        try:
            logger.info("Connecting to Redshift..")
            conn = pg8000.connect(
                database=redshift_database,
                user=redshift_user,
                password=redshift_password,
                host=redshift_host,
                port=redshift_port,
            )

            logger.info("Connecting to redshift table and executing the given Query.")

            logger.info("Query to Execute: {}".format(query))
            cur1 = conn.cursor()
            cur1.execute(query)
            logger.info("Query executed successfully in the Redshift..")
            conn.commit()
            cur1.close()
            conn.close()
            query_run_status = True

        except Exception as Error:
            logger.error(
                "Error occured while executing the query in the Redshift. :{}".format(
                    Error
                ),
                exc_info=True,
            )
            query_run_status = False
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error occured while re_run_table {}".format(
                    traceback.format_exc()
                ),
            )

        return query_run_status

    @staticmethod
    def re_run_table(redshift_table, whouse_details, file_name, logger):
        """This will go to the given table and delete the records if they are
            processed today.
        This will help at the time of processing the file again to avoid redundancy.
        """
        redshift_user = whouse_details["username"]
        redshift_password = whouse_details["password"]
        redshift_schema = whouse_details["dbSchema"]
        redshift_host = whouse_details["host"]
        redshift_port = whouse_details["port"]
        redshift_database = whouse_details["dbCatalog"]

        re_run_delete_status = False
        try:
            logger.info("Connecting to Redshift..")
            conn = pg8000.connect(
                database=redshift_database,
                user=redshift_user,
                password=redshift_password,
                host=redshift_host,
                port=redshift_port,
            )

            logger.info(
                "Connecting to redshift table and deleting the data which has processed today in the given table."
            )

            query1 = (
                "Delete from "
                + redshift_schema
                + "."
                + redshift_table
                + " where to_date(process_dtm,'yyyy-mm-dd') = to_date(current_date,'yyyy-mm-dd') and file_name = '{}'".format(
                    file_name
                )
            )
            logger.info("re_run query to delete: {}".format(query1))
            cur1 = conn.cursor()
            cur1.execute(query1)
            logger.info("Deleted the records which are processed today..")
            conn.commit()
            cur1.close()
            conn.close()
            re_run_delete_status = True

        except Exception as Error:
            logger.error(
                "Error occured while deleting the data from the table. :{}".format(
                    Error
                ),
                exc_info=True,
            )
            re_run_delete_status = False
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error occured while re_run_table {}".format(
                    traceback.format_exc()
                ),
            )

        return re_run_delete_status

    @staticmethod
    def run_file_checklist(whouse_details, logger, spark):
        """This will query the redshift table and pull the required records for run_full_file_checklist.
        """
        redshift_user = whouse_details["username"]
        redshift_password = whouse_details["password"]
        redshift_schema = whouse_details["dbSchema"]
        redshift_host = whouse_details["host"]
        redshift_port = whouse_details["port"]
        redshift_database = whouse_details["dbCatalog"]

        query_status = False

        try:
            logger.info("Connecting to Redshift..")
            conn = pg8000.connect(
                database=redshift_database,
                user=redshift_user,
                password=redshift_password,
                host=redshift_host,
                port=redshift_port,
            )
            logger.info("Query Execution in Progress...")
            query1 = """select case when sas_brand_id = '7' then 'TNF' else 'VANS' end as Brand,'F_'||Brand||'_'||'CLASS' as file_name, trunc(process_dtm) as load_date,count(*) as brand_count from vfapdsmigration.class group by 1,2,3 union all
                         select  case when sas_brand_id = '7' then 'TNF' else 'VANS' end as Brand,  'F_'||Brand||'_'||'COLOR' as file_name, trunc(process_dtm) as load_date,count(*) as brand_count from vfapdsmigration.class group by 1,2,3"""
            cur1 = conn.cursor()
            df_redshift_daily_data = cur1.execute(query1)
            logger.info("Query executed successfully")
            results = df_redshift_daily_data.fetchall()
            logger.info("Results : {}".format(results))
            df_redshift = spark.createDataFrame(
                results, ["Brand", "file_name", "load_date", "brand_count"]
            )
            logger.info("df_redshift created.. {} ".format(type(df_redshift)))
            conn.commit()
            cur1.close()
            conn.close()

        except Exception as Error:
            logger.error(
                "Error occured while querying data from redshift. :{}".format(Error),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error occured while run_file_checklist : {}".format(
                    traceback.format_exc()
                ),
            )

        return df_redshift

    @staticmethod
    def is_feed_active(logger, active_status=None):
        """Method to check if active flag of file in dynamo db

        Keyword Arguments:
            active_status {String} -- is_active flag that is configured in dynamo db
             (default: {None})
        """
        if active_status is None:
            logger.info("No parameter active_status received ")
            status = False
        elif active_status.lower() == "true":
            logger.info(
                "Received parameter {} .File is preconfigured as active in dynamo db configuration".format(
                    active_status
                )
            )
            status = True
        elif active_status.lower() == "false":
            logger.info(
                "Received parameter {} .File is preconfigured as inactive in dynamo db configuration".format(
                    active_status
                )
            )
            logger.info("File will not be processed. Skipping file")
            status = False
        else:
            logger.info("Incorrect parameter received {}".format(active_status))
        return status

    @staticmethod
    def create_etl_status_record(
        etl_status_record_parameters,
        env_params,
        logger=logging,
        etl_status_tbl_sort_key_as_job_process_dttm=None,
    ):
        create_etl_status_record_status = constant.ETL_STATUS_CREATION_FAILED
        job_process_dttm = None
        try:
            if etl_status_tbl_sort_key_as_job_process_dttm != None:
                job_process_dttm = str(etl_status_tbl_sort_key_as_job_process_dttm)
            else:
                job_process_dttm = str(datetime.utcnow())
            etl_status_record_parameters["processing_date"] = job_process_dttm
            logger.info(
                "Initializing Record For Status ETL Table With Params : {}".format(
                    etl_status_record_parameters
                )
            )
            DynamoUtils.put_dndb_item(
                dndb_item=etl_status_record_parameters,
                table=env_params["status_table"],
                logger=logger,
            )
            create_etl_status_record_status = constant.ETL_STATUS_CREATION_SUCCESS
        except Exception as error:
            create_etl_status_record_status = constant.ETL_STATUS_CREATION_FAILED
            job_process_dttm = None
            logger.error(
                "Error ocurred while processing create_etl_status_table_row due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.CORE_UTILS,
                exeptionType=constant.CORE_UTILS_EXCEPTION,
                message="Error ocurred while processing create_etl_status_table_row due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return create_etl_status_record_status, job_process_dttm

    @staticmethod
    def get_glue_job_params(job_status, log_file_path):
        return {
            "job_status": job_status,
            "log_file_path": log_file_path,
            "job_id": utils.get_glue_job_run_id(),
        }

    @staticmethod
    def is_exception_has_required_attributes(exception_obj):
        return vars(exception_obj).keys() >= set(
            ["message", "moduleName", "exeptionType"]
        )

    @staticmethod
    def get_etl_status_tbl_initilize_params(file_name):
        if file_name.__contains__("map"):
            return {
                "file_name": file_name,
                "map_status": {
                    "error_info": "null",
                    "status": config.STAGE_NOT_STARTED_STATUS,
                    "update_dttm": str(datetime.utcnow()),
                },
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow()),
            }
        elif file_name.__contains__("xref"):
            return {
                "file_name": file_name,
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow()),
            }
        else:
            return {
                "file_name": file_name,
                "tr_status": config.STAGE_REFINED_TO_TRANSFORM_INITIAL_STATUS_PARAMS,
                "dq_status": config.STAGE_DQ_CHECK_INITIAL_STATUS_PARAMS,
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow()),
            }

