import json
import sys
import base64
import boto3
import pg8000
import modules.config.config as config
from datetime import datetime
from modules.utils.utils_dynamo import DynamoUtils
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import configparser
import logging
import traceback


class utils:
    def __init__(self):
        pass

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
            mod = __import__("modules.transforms." +
                             classname, fromlist=[classname])
            klass = getattr(mod, classname)
        elif classname.startswith("dq"):
            mod = __import__("modules.dq." + classname, fromlist=[classname])
            klass = getattr(mod, classname)
        elif classname.startswith("map"):
            mod = __import__("modules.map." + classname, fromlist=[classname])
            klass = getattr(mod, classname)
        else:
            klass = NotImplemented

        return klass

    @staticmethod
    def get_file_config_params(filename, logger):
        # import ipdb;ipdb.set_trace()
        """This function is created to get the params replated to file from dynamodb

        Arguments:
            filename {String} -- filename for which params to be fetched
            logger {logger} -- logger object

        Returns:
            [dict] -- Returns a dictionary of parameters from dynamo DB
        """
        file_params = None
        # try:
        key = utils.get_parameter_store_key()
        env_params = utils.get_param_store_configs(key)

        file_parts = filename.split("_")
        #print(file_parts)
        sort_key = "_".join(file_parts[0:-1]) + "_"
        partition_key = file_parts[2]
        print(partition_key)
        # get date from filename
        # file_date = datetime.strptime(
        #     file_parts[-1].split(".")[0], config.FILE_DATE_FORMAT
        # )

        logger.info("File Broker Params for File : {}".format(filename))
        logger.info(
            "fetching file broker table with primary key : {} and sort key : {}".format(
                partition_key, sort_key
            )
        )

        file_params = DynamoUtils.get_dndb_item(
            partition_key_atrr=config.FILE_CONFIG_SORT_KEY_ATTRIBUTE,
            partition_key_value=sort_key,
            # sort_key_attr=config.FILE_CONFIG_SORT_KEY_ATTRIBUTE,
            # sort_key_value=sort_key,
            table=env_params["config_table"],
            logger=logger
        )
        print(file_params)
        logger.info(
            "File parameters for file are as : {}".format(file_params))
        return file_params

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
        return params

    @staticmethod
    def get_parameter_store_key():
        """This method parses the config_store.ini file to retrieve param store key

        Returns:
            String -- Return key name defined in config_store.ini as string
        """
        config = configparser.RawConfigParser()
        config.read("config_store.ini")
        print(config.sections())
        try:
            print(
                "Found param store key as {}".format(config["PARAMSTORE"]["StoreKey"])
            )
            key = str(config["PARAMSTORE"]["StoreKey"])
        except Exception as e:
            key = None
            print("Failed to parse config_store.ini {}".format(e))
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
    def check_existence_of_redshift_table(
            spark, redshift_table, redshift_details, logger
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
        redshift_user = redshift_details["username"]
        redshift_password = redshift_details["password"]
        redshift_schema = redshift_details["dbSchema"]
        logger.info("Attempting to create jdbc url for redshift")
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            redshift_details["engine"],
            redshift_details["host"],
            redshift_details["port"],
            redshift_details["dbCatalog"],
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
    def execute_query_in_redshift(query, redshift_details, logger):
        """This method will go and execute the given query.
        """
        redshift_user = redshift_details["username"]
        redshift_password = redshift_details["password"]
        redshift_schema = redshift_details["dbSchema"]
        redshift_host = redshift_details["host"]
        redshift_port = redshift_details["port"]
        redshift_database = redshift_details["dbCatalog"]

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

            logger.info(
                "Connecting to redshift table and executing the given Query."
            )

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
                ), exc_info=True
            )
            query_run_status = False

        return query_run_status

    @staticmethod
    def re_run_table(redshift_table, redshift_details, file_name, logger):
        """This will go to the given table and delete the records if they are
            processed today.
        This will help at the time of processing the file again to avoid redundancy.
        """
        redshift_user = redshift_details["username"]
        redshift_password = redshift_details["password"]
        redshift_schema = redshift_details["dbSchema"]
        redshift_host = redshift_details["host"]
        redshift_port = redshift_details["port"]
        redshift_database = redshift_details["dbCatalog"]

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
                file_name)
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
                ), exc_info=True
            )
            re_run_delete_status = False


        return re_run_delete_status

    @staticmethod
    def create_etl_status_record(etl_status_record_parameters, env_params,
                                 etl_status_tbl_sort_key_as_job_process_dttm, logger=logging):
        create_etl_status_record_status = config.ETL_STATUS_CREATION_FAILED
        job_process_dttm = None
        try:
            if etl_status_tbl_sort_key_as_job_process_dttm != None:
                job_process_dttm = str(
                    etl_status_tbl_sort_key_as_job_process_dttm)
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
            create_etl_status_record_status = \
                config.ETL_STATUS_CREATION_SUCCESS
        except Exception as error:
            create_etl_status_record_status = config.ETL_STATUS_CREATION_FAILED
            job_process_dttm = None
            logger.error(
                "Error ocurred while processing create_etl_status_table_row due to : {}".format(
                    error
                ), exc_info=True
            )

        return create_etl_status_record_status, job_process_dttm

    @staticmethod
    def get_etl_status_tbl_initilize_params(file_name):
        if file_name.__contains__("map"):
            return {
                "file_name": file_name,
                "map_status": {"error_info": "null",
                               "status": config.STAGE_NOT_STARTED_STATUS,
                               "update_dttm": str(datetime.utcnow())},
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow())
            }
        elif file_name.__contains__("xref"):
            return {
                "file_name": file_name,
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow())
            }
        else:
            return {
                "file_name": file_name,
                "tr_status": config.STAGE_REFINED_TO_TRANSFORM_INITIAL_STATUS_PARAMS,
                "job_id": utils.get_glue_job_run_id(),
                "job_start_time": str(datetime.utcnow())
            }

    @staticmethod
    def get_glue_job_run_id():
        job_run_id = None
        try:
            args = getResolvedOptions(sys.argv, ["JOB_NAME"])
            job_run_id = args["JOB_RUN_ID"]
            print("Glue_job_run_id : {}".format(job_run_id))
        except Exception as error:
            print("Error Ocurred in get_glue_job_run_id due to : {}".format(
                error))
            job_run_id = None

        return job_run_id


    @staticmethod
    def get_glue_job_params(job_status, log_file_path):
        return {
            "job_status": job_status,
            "log_file_path": log_file_path,
            "job_id": utils.get_glue_job_run_id()
        }

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
            print("datetime_folder: " + datetime_folder)
            s3_folder = datetime_folder + "/"
            print("S3 Folder: " + s3_folder)
            args = getResolvedOptions(sys.argv, ["JOB_NAME", "FILE_NAME"])
            print(config.LOG_DIR)

            s3_obj = (
                    config.LOG_DIR
                    + s3_folder
                    + args["FILE_NAME"].split(".")[0]
                    + "/"
                    + args["JOB_RUN_ID"]
                    + ".log"
            )
            print("s3 obj : {} ,file path s3://{}/{}".format(s3_obj, bucket,
                                                             s3_obj))
            s3_client.upload_file(file_name, bucket, s3_obj)
            print("File uploaded successfully to S3")
            log_file_path = "s3://{}/{}".format(bucket, s3_obj)
            status = True

        except Exception as e:
            print("Error occured while uploading file to s3 {}".format(e))
            log_file_path = ""
            status = False

        return status, log_file_path

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
            date_ly = (datetime.date(datetime.now()) - timedelta(
                days=1)).strftime(
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
            logger.info(
                "Incorrect parameter received {}".format(active_status))
        return status
