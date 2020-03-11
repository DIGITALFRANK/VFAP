from pyspark.context import SparkContext
import modules.config.config as config
from awsglue.context import GlueContext
from modules.utils.utils_core import utils
from modules.app_log.create_logger import get_logger
from modules.utils.utils_dynamo import DynamoUtils
from datetime import datetime
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from modules.exceptions import (
    CustomAppException,
    StageStatusNotSatisfiedException,
    IOException,
)
from modules.constants import constant
import traceback
from pyspark.sql.types import StructType, StructField, StringType, DecimalType


class Core_Job:
    def __init__(self, file_name):
        """This is the Constructor for the base class when an object of the
           base class is instantiated.

        Arguments:
            file_name {[type]} -- file_name {[String]} --
                                Name of the file that is used
            to initialise file params ,Logger ,SparkContext ,GlueContext,
            feed_name,etc.
        """

        self.sc = SparkContext.getOrCreate()
        glueContext = GlueContext(self.sc)
        self.glueContext = glueContext
        self.file_name = file_name
        # * -Key for AWS Param store from config_store.ini for every env
        # get the glue_job_run_id
        self.glue_job_run_id = utils.get_glue_job_run_id()
        print("Glue Job Id : {}".format(self.glue_job_run_id))
        self.key = utils.get_parameter_store_key()
        self.file_name = file_name
        print(
            "Found Configuration file with parameter store key as : {}".format(self.key)
        )
        self.env_params = utils.get_param_store_configs(self.key)
        logger = get_logger(__name__)
        self.logger = logger
        self.whouse_details = utils.get_secret(self.env_params["secret_manager_key"])
        # print("Using Redshift credentials as {}".format(self.whouse_details))
        if self.whouse_details is None:
            logger.error("Cannot find redshift credentials for the environment")
        else:
            logger.info("Found Redshift credentials for the environment")
        self.feed_name = file_name.split("_")[1]
        try:
            self.source = file_name.split("_")[2]
        except Exception as error:
            logger.error("Error Occured {}".format(error))
            self.source = None
        try:
            self.params = utils.get_file_config_params(file_name, self.logger)
        except Exception as error:
            logger.error(
                "Error Occurred while getting file_config_params in Core_job initializing : {}".format(
                    error
                ),
                exc_info=True,
            )
            self.params = None
        self.spark = glueContext.spark_session

        try:
            self.structype_schema = utils.convert_dynamodb_json_schema_to_struct_type_schema(
                self.params["schema"], self.logger
            )
        except Exception as error:
            logger.error(
                "Error Occurred while getting schema  in Core_job initializing : {}".format(
                    error
                ),
                exc_info=True,
            )
            self.structype_schema = None

        logger.info(
            "Core Job initialised successfully. Environment \
                parameters configured for {}".format(
                self.env_params["env_name"]
            )
        )

    @staticmethod
    def get_control_file_name(file_name):
        """
        Parameters:

        file_name: str - file name of the feed data

        Returns:

        control_file_name - str - file name of the corresponding control file for the feed
        """
        control_file_name = "_".join(
            file_name.split("_")[:-1] + ["CNTL", file_name.split("_")[-1]]
        )
        # Patch for all SFMC control files which do not share a time component with the filename of the feed data
        if ("SFMC" in file_name) or (("ADOBE" in file_name)):
            control_file_name = control_file_name.split(".")[0] + "000000.csv"
        return control_file_name

    def get_control_row_count(self, s3_directory):
        """This function gets the control table from S3 and produces the row count associated
           with the file name of the executing job

        Arguments:
            s3_directory: str - Path to the directory containing the control file
        Returns:
            row_count: int - Count of rows according to control file
        """
        log = self.logger
        spark = self.spark
        params = self.params
        is_header_included_in_count = params["header_cnt_included_cntl"]
        log.info("Getting control file from S3 to ensure consistent row count")
        control_file_name = self.get_control_file_name(self.file_name)
        s3_path = s3_directory + "/" + control_file_name
        try:
            log.info("Reading control file from s3 - {0}".format(s3_path))
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("delimiter", params["control_file_delimiter"])
                .option("mode", "FAILFAST")
                .schema(
                    StructType(
                        [
                            StructField("Filename", StringType(), True),
                            StructField("Count", DecimalType(), True),
                        ]
                    )
                )
                .load(s3_path)
            )
            df.show()
            control_file_row_count = df.count()
            if control_file_row_count > 1:
                log.error(
                    "Control file has {0} rows - expecting only 1".format(
                        control_file_row_count
                    )
                )
                raise Exception
        except Exception as error:
            log.error("Unable to read and parse control file - {0} ".format(s3_path))
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_S3_READ_OBJECT_EXCEPTION,
                message=constant.S3_READ_FAILED_MESSAGE.format(error),
            )
        log.info("Collecting stored row count from control file")
        # Subtract 1 if header row included in control count
        if is_header_included_in_count.lower() == "true":
            if control_file_row_count == 0:
                log.info("Control file from s3 has 0 records")
                row_count = 0
            else:
                log.info("Control file from s3 records extracted from ctrl_file_df")
                row_count = df.select(df.Count).collect()[0][0] - 1
        else:
            if control_file_row_count == 0:
                log.info("Control file from s3 has 0 records")
                row_count = 0
            else:
                log.info("Control file from s3 records extracted from ctrl_file_df")
                row_count = df.select(df.Count).collect()[0][0]
        log.debug("Successfully collected row count - {0}".format(row_count))
        return row_count

    def read_from_s3_without_multilines(self, bucket, path, structype_schema):
        """This function reads data from s3 using spark.read method in Pyspark without multiline option so that if we reading a feed file with multiline feed, we can get physical count of the number of records in the feed

        Arguments:
            bucket {[String]} -- Name of the bucket in S3
            path {String} -- s3 object key
        Returns:
            [spark.dataframe] -- Returns a spark dataframe count
        """
        logger = self.logger
        params = self.params
        delimiter = params["raw_source_file_delimiter"]
        if params["custom_s3_read_params"]["null_value"] is None:
            null_value = ""
        else:
            null_value = params["custom_s3_read_params"]["null_value"]
        try:
            spark = self.spark

            s3_obj = "s3://{}/{}".format(bucket, path)
            logger.info("Reading file : {} from s3...".format(s3_obj))
            logger.debug("Reading dataframe")

            raw_df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("delimiter", delimiter)
                .option(
                    "timestampFormat",
                    params["custom_s3_read_params"]["timestamp_format"],
                )
                .option("nullValue", null_value)
                .option("mode", params["custom_s3_read_params"]["mode_value"])
                .option("escape", params["custom_s3_read_params"]["escape_value"])
                .schema(structype_schema)
                .load(s3_obj)
            )
            logger.info("Dataframe without multiline read successfully")
            logger.info("Showing sample records \n")
            raw_df.persist()
            raw_df.show()

        except Exception as error:
            raw_df = None
            logger.error("Could not read file {}".format(error), exc_info=True)
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_S3_READ_OBJECT_EXCEPTION,
                message=constant.S3_READ_FAILED_MESSAGE.format(error),
            )
        return raw_df.count()

    # **Need to update this method with params

    def read_from_s3(self, bucket, path, structype_schema):
        """This function reads data from s3 using spark.read method with various read options in Pyspark

        Arguments:
            bucket {[String]} -- Name of the bucket in S3
            path {String} -- s3 object key
        Returns:
            [spark.dataframe] -- Returns a spark dataframe
        """
        logger = self.logger
        params = self.params
        delimiter = params["raw_source_file_delimiter"]
        if params["custom_s3_read_params"]["null_value"] is None:
            null_value = ""
        else:
            null_value = params["custom_s3_read_params"]["null_value"]
        try:
            spark = self.spark

            s3_obj = "s3://{}/{}".format(bucket, path)
            logger.info("Reading file : {} from s3...".format(s3_obj))
            logger.debug("Reading dataframe")

            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("delimiter", delimiter)
                .option(
                    "timestampFormat",
                    params["custom_s3_read_params"]["timestamp_format"],
                )
                .option("nullValue", null_value)
                .option("mode", params["custom_s3_read_params"]["mode_value"])
                .option("multiline", params["custom_s3_read_params"]["multiline_value"])
                .option("escape", params["custom_s3_read_params"]["escape_value"])
                .schema(structype_schema)
                .load(s3_obj)
            )
            logger.info("Dataframe read successfully")
            logger.info("Showing sample records \n")
            df.persist()
            df.show()

        except Exception as error:
            df = None
            logger.error("Could not read file {}".format(error), exc_info=True)
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_S3_READ_OBJECT_EXCEPTION,
                message=constant.S3_READ_FAILED_MESSAGE.format(error),
            )

        # Addition of row count check
        if params["custom_s3_read_params"]["multiline_value"] == "true":
            logger.info(
                "Reading file without multiline to get physical count of the file from s3..."
            )
            try:
                self.feed_row_count = self.read_from_s3_without_multilines(
                    bucket=self.env_params["refined_bucket"],
                    path=params["rf_source_dir"] + "/" + self.file_name,
                    structype_schema=self.structype_schema,
                )
                logger.info(
                    "The physical count of feed file (without multiline) is - {}".format(
                        self.feed_row_count
                    )
                )
            except Exception as error:
                self.feed_row_count = None
                logger.error(
                    "Could not read feed-file without multiline{}".format(error),
                    exc_info=True,
                )
                raise IOException.IOError(
                    moduleName=constant.CORE_JOB,
                    exeptionType=constant.IO_S3_READ_OBJECT_EXCEPTION,
                    message=constant.S3_READ_FAILED_MESSAGE.format(error),
                )
        else:
            logger.info(
                "Reading file with multiline to get physical count of the file from s3..."
            )
            self.feed_row_count = df.count()
            logger.info(
                "The actual count of feed file is - {}".format(self.feed_row_count)
            )

        control_row_count = self.get_control_row_count(
            s3_directory="/".join(s3_obj.split("/")[:-1])
        )

        if self.feed_row_count != control_row_count:
            logger.error(
                "Inconsistent row count:  number of rows loaded from {0} - {1}, control file row count -{2}".format(
                    s3_obj, self.feed_row_count, control_row_count
                )
            )
            raise StageStatusNotSatisfiedException.StageStatusNotSatisfiedError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.DQ_ROW_COUNT_EXCEPTION,
                message=constant.DQ_ROW_COUNT_FAILURE_MESSAGE,
            )
        else:
            logger.info(
                "Number of rows found in {0} matches control file row count - {1}".format(
                    s3_obj, self.feed_row_count
                )
            )
            self.df = df
            return df

    # **Need to update this method with params
    def write_to_tgt(self, df, bucket, path, mode="append"):
        """This function writes data to s3 using
            spark.dataframe.write method in Pyspark

        Arguments:
            df {spark.dataframe} -- Spark dataframe
            bucket{String} -- bucket name to which data to be written

        Keyword Arguments:
            mode {str} -- Mode for writing on S3 (default: {"append"})

        Returns:
            [bool] -- Boolean status for writing into s3
        """
        write_status = None
        logger = self.logger
        try:

            s3_obj = "s3://{}/{}".format(bucket, path)
            logger.info("Writing File : {} ...".format(s3_obj))
            logger.info("s3_ obj : {} has records : {}".format(s3_obj, df.count()))
            df.write.mode(mode).format("csv").option("header", "true").save(s3_obj)
            logger.info("Writing File  : {}".format(s3_obj))

            write_status = True
        except Exception as error:
            write_status = False
            logger.error(
                "Error Occurred While writing file : {} to s3 due to : {}".format(
                    path, error
                ),
                exc_info=True,
            )
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_S3_WRITE_OBJECT_EXCEPTION,
                message=constant.S3_WRITE_FAILED_MESSAGE.format(error),
            )
        return write_status

    # **TODO -Implement this method little later
    def update_ddb():
        """Placeholder for any update_ddb method
        """
        pass

    def get_status(self, file_name, etl_status_sort_key_as_job_process_dttm):
        """Method to get the status of every stage for a Glue ETL job.
           The status table name must be present for previous stage in dynamo DB
           and parameter store must
           be configured with status table name

        Arguments:
            file_name {string} -- name of file for which status should be checked

        Returns:
            stage_status{dict} -- Returns dict for stage
        """
        stage_status = None
        env_params = self.env_params
        file_name = self.file_name
        logger = self.logger
        try:
            # for daily job and weekly job depending on file_name/map_name primary/part key for status table need to  be extracted
            stage_status = DynamoUtils.get_dndb_item(
                partition_key_atrr=config.FILE_STATUS_PRIMARY_KEY_ATTRIBUTE,
                partition_key_value=file_name,
                table=env_params["status_table"],
                sort_key_attr="processing_date",
                sort_key_value=etl_status_sort_key_as_job_process_dttm,
                logger=logger,
            )
            # status=config.dndb_item
            logger.info("Stage Status : {}".format(stage_status))
        except Exception as error:
            logger.error(
                "Error ocurred in get_status : {}".format(error), exc_info=True
            )
            stage_status = None
            # chnage exception type to error.exeptionType and error.message
            raise CustomAppException.CustomAppError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_DYNAMODB_READ_EXCEPTION,
                message=constant.GET_STATUS_FAILED_MESSAGE,
            )

        return stage_status

    def update_stage_status(
        self, stages_to_be_updated, etl_status_sort_key_as_job_process_dttm
    ):
        """Method to update the status of every stage for a Glue ETL job.
           The status table name must be present in dynamo DB and parameter store must
           be configured with status table name


        Arguments:
            stages_to_be_updated {dict} -- Dictionary of items to be updated in status
                                           table

        Returns:
            stage_update_status -- Returns boolean value for every update
        """
        stage_update_status = False
        env_params = self.env_params
        file_name = self.file_name
        logger = self.logger
        try:
            stage_update_status = DynamoUtils.update_dndb_items(
                table=env_params["status_table"],
                partition_key_atrr=config.FILE_STATUS_PRIMARY_KEY_ATTRIBUTE,
                partition_key_value=file_name,
                attributes_to_be_updated_dict=stages_to_be_updated,
                sort_key_attr="processing_date",
                sort_key_value=etl_status_sort_key_as_job_process_dttm,
                logger=logger,
            )
            stage_update_status = True
        except Exception as error:
            logger.error(
                "Error Occured update_stage_status {}".format(error), exc_info=True
            )
            stage_update_status = False
            raise CustomAppException.CustomAppError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_DYNAMODB_UPDATE_EXCEPTION,
                message=constant.UPDATE_STAGE_STATUS_FAILED_MESSAGE,
            )
        return stage_update_status

    # *TODO -Implement this method little later
    def validation():
        """Placeholder for any validation method
        """
        pass

    def data_quality_checks(self, df, etl_status_sort_key_as_job_process_dttm):
        """This is the default data_quality_checks used by the base class

        Returns:
            [bool] -- Boolean status to represent if the data_quality checks
            are passed
            or failed
        """
        dq_stage_params_to_be_updated = {}
        logger = self.logger
        params = self.params
        file_name = self.file_name
        try:

            # TODO get previous status of stage and depending on status take decison for DQ RUN
            # get status for previous stage
            stage_status_from_dndb_status_tbl = self.get_status(
                self.file_name, etl_status_sort_key_as_job_process_dttm
            )

            dq_status = True
            dq_status_to_be_updated_in_dndb = ""

            dq_list_to_call = params["dq_params"].keys()
            # Inorder to start DQ process, we are checking the schema length and the dataframe count.
            if not (
                (len(df.columns) == len(params["schema"].keys())) and df.count() > 0
            ):
                raise CustomAppException.CustomAppError(
                    moduleName=constant.CORE_JOB,
                    exeptionType=constant.DQ_EXCEPTION,
                    message="Schema Mismatch or DF is empty",
                )

            # if dq_list_to_call is empty then Skip dq
            if len(dq_list_to_call) != 0:
                for dq_item in dq_list_to_call:
                    # If dq_status is True, We need to update status table. Also, we will proceed with Transformations.
                    if dq_status is True:
                        logger.info("DQ Process starts here..")
                        dq_class = params["dq_params"][dq_item]["dq_class_to_call"]
                        # Start Executing respective DQ Class
                        logger.info("Applying DQ : {0}".format(dq_class))
                        klass = utils.get_dynamic_class(dq_class, logger)
                        dq_obj = klass(file_name=file_name, dq_item=dq_item)
                        dq_status = dq_obj.dq(df)
                        logger.info("DQ Status : {}".format(dq_status))

                    # If dq_status is False, We need to raise exception and updating the status table.
                    # It won't proceed to execute Transformation process.
                    else:
                        dq_status = False
                        logger.info(
                            "DQ Process failed for the file {}".format(file_name)
                        )
                        # updating status table -- To do
                        # Raise Exception -- To do
                logger.info("DQ Status After All DQ Checks : {}".format(dq_status))
                if dq_status is False:
                    raise StageStatusNotSatisfiedException.StageStatusNotSatisfiedError(
                        moduleName=constant.CORE_JOB,
                        exeptionType=constant.DQ_EXCEPTION,
                        message=constant.DQ_CHECK_STAGE_STATUS_FAILED_MESSAGE,
                    )
                dq_status_to_be_updated_in_dndb = config.STAGE_COMPLETED_STATUS

            else:
                logger.info("DQ Not_Applicable")
                dq_status_to_be_updated_in_dndb = config.STAGE_NOT_APPLICABLE_STATUS

            dq_stage_params_to_be_updated.update(
                {
                    "dq_status": {
                        "error_info": "null",
                        "status": dq_status_to_be_updated_in_dndb.lower(),
                        "update_dttm": str(datetime.utcnow()),
                    }
                }
            )
        except Exception as error:
            dq_status = False
            logger.error(
                "Error Ocurred in data_quality_checks due to :{}".format(error),
                exc_info=True,
            )
            dq_stage_params_to_be_updated.update(
                {
                    "dq_status": {
                        "error_info": "DQ Failed",
                        "status": config.STAGE_FAILED_STATUS.lower(),
                        "update_dttm": str(datetime.utcnow()),
                        "error_traceback": str(error.message),
                    }
                }
            )
            raise CustomAppException.CustomAppError(
                moduleName=error.moduleName,
                exeptionType=error.exeptionType,
                message=error.message,
            )
        finally:
            self.update_stage_status(
                stages_to_be_updated=dq_stage_params_to_be_updated,
                etl_status_sort_key_as_job_process_dttm=etl_status_sort_key_as_job_process_dttm,
            )
        return dq_status

    def transform(self, df, etl_status_sort_key_as_job_process_dttm):
        refined_to_transformed_stage_status = False
        # initializing default status parameteres to be updated
        refined_to_transformed_stage_params_to_be_updated = {}
        params = self.params
        logger = self.logger
        load_mode = self.params["write_mode"]

        try:
            # get status for previous stage
            stage_status_from_dndb_status_tbl = self.get_status(
                self.file_name, etl_status_sort_key_as_job_process_dttm
            )

            # get status for raw_to_refined_stage
            dq_check_stage_status = stage_status_from_dndb_status_tbl["dq_status"][
                "status"
            ].lower()
            refined_to_transformed_stage_status = stage_status_from_dndb_status_tbl[
                "tr_status"
            ]["status"].lower()

            # checking dq_status if it is completed and refined_to_transformed_stage is either not_started or failed then only proceed for Transform
            if not (
                (
                    dq_check_stage_status == config.STAGE_COMPLETED_STATUS
                    or dq_check_stage_status == config.STAGE_NOT_APPLICABLE_STATUS
                )
                and refined_to_transformed_stage_status != config.STAGE_COMPLETED_STATUS
            ):
                raise StageStatusNotSatisfiedException.StageStatusNotSatisfiedError(
                    moduleName=constant.CORE_JOB,
                    exeptionType=constant.TRANSFORMATION_EXCEPTION,
                    message=constant.TRANSFORMATION_PRECONDITION_STAGE_STATUS_EXCEPTION_MESSAGE,
                )

            # getting tr_class_to_call from dynamodb
            transformation_class_to_call_dict = params["tr_class_to_call"]
            klass = utils.get_dynamic_class(
                transformation_class_to_call_dict["1"], logger
            )
            tr_obj = klass(self.file_name)

            # applying tranformation on refined df
            transformed_df_data_dict = tr_obj.transform(df)

            if len(transformed_df_data_dict) == 0:
                raise CustomAppException.CustomAppError(
                    moduleName=constant.CORE_JOB,
                    exeptionType=constant.TRANSFORMATION_EXCEPTION,
                    message=constant.TRANSFORMED_DF_DATA_DICT_EMPTY_MESSAGE,
                )

            transformed_df_to_redshift_table_status = True
            for target_table, transformed_df in transformed_df_data_dict.items():
                if transformed_df_to_redshift_table_status:
                    logger.info(
                        "Inside datadict loop writing transformed_df to : {}".format(
                            target_table
                        )
                    )

                    # transformed_df_to_redshift_table_status = self.write_df_to_redshift_table(
                    #     df=transformed_df,
                    #     redshift_table=target_table,
                    #     # load_mode="append",
                    #     load_mode=load_mode,
                    # )
                    # Write to Refshift using Glue dynamic dataframe
                    transformed_df = transformed_df.withColumn(
                        "file_name", F.lit(self.file_name)
                    )
                    transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                        df=transformed_df,
                        redshift_table=target_table,
                        # load_mode="append",
                        load_mode=load_mode,
                    )
                    logger.info(
                        "Response from writing to redshift is {}".format(
                            transformed_df_to_redshift_table_status
                        )
                    )

                else:
                    transformed_df_to_redshift_table_status = False
                    logger.info("Failed to Load Transformed Data Dict To Redshift")

            logger.info(
                "Response from writing to redshift is {}".format(
                    transformed_df_to_redshift_table_status
                )
            )

            # transformed_df_to_bucket_status = tr_obj.write_to_tgt(
            #     df=transformed_df,
            #     mode="append",
            #     bucket=config.FB_TRANSFORMED_BUCKET,
            #     path=params["tgt_dstn_folder_name"],
            # )

            if transformed_df_to_redshift_table_status is False:
                raise CustomAppException.CustomAppError(
                    moduleName=constant.CORE_JOB,
                    exeptionType=constant.TRANSFORMATION_EXCEPTION,
                    message=constant.TRANSFORMED_DF_TO_REDSHIFT_TABLE_WRITE_FAILED_MESSAGE,
                )

            refined_to_transformed_stage_params_to_be_updated.update(
                {
                    "tr_status": {
                        "error_info": "null",
                        "status": config.STAGE_COMPLETED_STATUS,
                        "update_dttm": str(datetime.utcnow()),
                    }
                }
            )
            refined_to_transformed_stage_status = True

        except Exception as error:
            logger.error(
                "Error Ocurred in transform core_job due to : {}".format(error),
                exc_info=True,
            )
            refined_to_transformed_stage_status = False
            refined_to_transformed_stage_params_to_be_updated.update(
                {
                    "tr_status": {
                        "error_info": "Transformed Failed",
                        "status": config.STAGE_FAILED_STATUS,
                        "update_dttm": str(datetime.utcnow()),
                        "error_traceback": str(traceback.format_exc()),
                    }
                }
            )
            raise CustomAppException.CustomAppError(
                moduleName=error.moduleName,
                exeptionType=error.exeptionType,
                message=error.message,
            )
        finally:
            logger.info(
                "Updating refined_to_transformed_stage params with params : {}".format(
                    refined_to_transformed_stage_params_to_be_updated
                )
            )
            self.update_stage_status(
                stages_to_be_updated=refined_to_transformed_stage_params_to_be_updated,
                etl_status_sort_key_as_job_process_dttm=etl_status_sort_key_as_job_process_dttm,
            )
        return refined_to_transformed_stage_status

    @staticmethod
    def map(map_name, job_process_dttm):
        status = False
        logger = get_logger()
        etl_status_sort_key_as_job_process_dttm = str(job_process_dttm)
        core_job = Core_Job(map_name)
        map_stage_params_to_be_updated = {}
        map_status_to_be_updated = ""
        map_error_info_to_be_updated = "null"
        try:
            # getting the parameters from dynamodb for respective map
            params = utils.get_file_config_params(map_name, logger)
            map_dict = params["map_class_to_call"]

            klass = utils.get_dynamic_class(map_dict["1"], logger)
            map_obj = klass(map_name)
            status = map_obj.map()

            logger.info("status of map  : {}".format(status))

            if status is True:
                map_status_to_be_updated = config.STAGE_COMPLETED_STATUS
                map_error_info_to_be_updated = "null"
            else:
                map_status_to_be_updated = config.STAGE_FAILED_STATUS
                map_error_info_to_be_updated = "Map Stage Failed"

            # updating status parameters
            map_stage_params_to_be_updated.update(
                {
                    "map_status": {
                        "error_info": map_error_info_to_be_updated,
                        "status": map_status_to_be_updated,
                        "update_dttm": str(datetime.utcnow()),
                    }
                }
            )

        except Exception as error:
            status = False
            map_stage_params_to_be_updated.update(
                {
                    "map_status": {
                        "error_info": "Map Job Failed",
                        "error_traceback": str(traceback.format_exc()),
                        "status": config.STAGE_FAILED_STATUS,
                        "update_dttm": str(datetime.utcnow()),
                    }
                }
            )
            logger.error(
                "Error Ocurred in run_map due to : {}".format(error), exc_info=True
            )
            if utils.is_exception_has_required_attributes(error):
                logger.info("with attributes")
                raise CustomAppException(
                    moduleName=error.moduleName,
                    exeptionType=error.exeptionType,
                    message=error.message,
                )
            else:
                raise CustomAppException(
                    moduleName=constant.CORE_JOB_MODULE,
                    exeptionType=constant.MAP_EXCEPTION,
                    message="Error Ocurred in run_map due to : {}".format(
                        traceback.format_exc()
                    ),
                )
        finally:
            logger.info(
                "updating stage status : {}".format(map_stage_params_to_be_updated)
            )
            map_stage_params_to_be_updated.update(
                {"job_end_time": str(datetime.utcnow())}
            )

            # updating map status parameteres
            core_job.update_stage_status(
                stages_to_be_updated=map_stage_params_to_be_updated,
                etl_status_sort_key_as_job_process_dttm=etl_status_sort_key_as_job_process_dttm,
            )

        return status

    def redshift_table_to_dataframe(self, redshift_table):
        """
        Parameters:
            redshift_table: str,
            redshift_schema: str,
            redshift_url: str
            redshift_user: str,
            redshift_password: str,
            spark.session: pyspark.sql.session,
            log: logging.Logger

        Returns:
            pyspark.sql.DataFrame

        This function reads a table from AWS Redshift into a PySpark DataFrame
        """
        logger = self.logger
        spark_session = self.spark
        redshift_user = self.whouse_details["username"]
        redshift_password = self.whouse_details["password"]
        redshift_schema = self.whouse_details["dbSchema"]
        logger.info("Attempting to create jdbc url for redshift")
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            self.whouse_details["engine"],
            self.whouse_details["host"],
            self.whouse_details["port"],
            self.whouse_details["dbCatalog"],
        )

        logger.info(
            "Attempting to read table {0}.{1} from Redshift into DataFrame with jdbc_url:{2}".format(
                redshift_schema, redshift_table, redshift_url
            )
        )
        schema_qualified_table = redshift_schema + "." + redshift_table
        try:
            df = (
                spark_session.read.format("jdbc")
                .option("driver", "com.amazon.redshift.jdbc.Driver")
                .option("url", redshift_url)
                .option("dbtable", schema_qualified_table)
                .option("password", redshift_password)
                .option("user", redshift_user)
                .load()
            )
            logger.info(
                "Successfully read table {0}.{1} from Redshift into DataFrame".format(
                    redshift_schema, redshift_table
                )
            )

        except Exception as exception:
            df = None
            logger.error(
                "Unable to read table {0}.{1} from Redshift, either table doesn't exist\
                     or connection unavailable: {2}".format(
                    redshift_schema, redshift_table, exception
                ),
                exc_info=True,
            )
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_READ_REDSHIFT_EXCEPTION,
                message=constant.REDSHIFT_TBL_TO_DF_FAILED_MESSAGE.format(exception),
            )
        return df

    def write_glue_df_to_redshift(self, df, redshift_table, load_mode):
        logger = self.logger
        env_params = self.env_params
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            self.whouse_details["engine"],
            self.whouse_details["host"],
            self.whouse_details["port"],
            self.whouse_details["dbCatalog"],
        )

        username = self.whouse_details["username"]
        password = self.whouse_details["password"]
        temp_bucket = "s3://{}/{}/".format(self.env_params["temp_bucket"], "temp")
        redshift_schema = self.whouse_details["dbSchema"]
        Target_table = redshift_schema + "." + redshift_table
        stage_table = redshift_schema + "." + redshift_table + "_stage"

        # database = "vfuap"
        # redshift_conn = "reshift-conn"
        redshift_conn = env_params["glue_rs_conn"]
        database = self.whouse_details["dbCatalog"]

        # Convert resultant dataframe to glue dynamic dataframe.
        dynamic_df = DynamicFrame.fromDF(df, self.glueContext, "dynamic_df")

        ## Write the transformed data into Amazon Redshift based on load mode

        try:
            logger.info("Load Mode: {}".format(load_mode))
            if load_mode.upper() == "APPEND":
                logger.info("Load Mode: {}".format(load_mode))
                # Staging table format

                # Create staging table in Redshift and Appending it to Target table and deleting staging table.
                logger.info(
                    "Appending Stage table - {} to Target table - {}".format(
                        stage_table, Target_table
                    )
                )

                create_stage_tbl_qry = "CREATE TABLE IF NOT EXISTS {} (LIKE {} including DEFAULTS);".format(
                    stage_table, Target_table
                )

                utils.execute_query_in_redshift(
                    create_stage_tbl_qry, self.whouse_details, logger
                )

                # post_query="begin;ALTER TABLE {} APPEND FROM {} FILLTARGET; drop table {}; end;".format(Target_table,stage_table,stage_table)
                pre_query = "begin;TRUNCATE TABLE {}; end;".format(stage_table)

                post_query = "begin; insert into {} select * from {}; end;".format(
                    Target_table, stage_table
                )
                if self.file_name.upper().startswith("I_VANS_CM_REGISTRATION"):
                    post_query = """begin;
                                    UPDATE {0} SET gndr = ' ' WHERE gndr = '"';
                                    UPDATE {0} SET new_repeat_visitor = ' ' WHERE new_repeat_visitor = '"';
                                    UPDATE {0} SET new_repeat_buyer = ' ' WHERE new_repeat_buyer = '"';""".format(
                        stage_table
                    ) + ";".join(
                        post_query.split(";")[1:]
                    )

                logger.info("Post Query to Execute : {}".format(post_query))
                logger.info(
                    "dynamic_df to append count : {}".format(dynamic_df.count())
                )
                logger.info("DF Schema : {}".format(df.printSchema()))
                logger.info("dynamic_df Schema : {}".format(dynamic_df.schema()))
                dynamic_df.show()
                ## Write the transformed data into Amazon Redshift based on load mode
                connection_options = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": stage_table,
                    "preactions": pre_query,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    "postactions": post_query,
                    "extracopyoptions": config.copy_options,
                }

                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options,
                    transformation_ctx="datasink1",
                )

                logger.info("Appended successfully to Target table!!")
                status = True

            else:
                logger.info("Overwriting the Target table..")

                pre_query = "begin;drop table if exists {};create table {} as select * from {} where 1=2; end;".format(
                    stage_table, stage_table, Target_table
                )

                post_query = "begin; TRUNCATE TABLE {}; insert into {} select * from {}; end;".format(
                    Target_table, Target_table, stage_table
                )

                connection_options = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "preactions": pre_query,
                    "dbtable": stage_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    "postactions": post_query,
                    "extracopyoptions": config.copy_options,
                }
                # logger.info(
                #     "dynamic_df to overwrite count : {}".format(dynamic_df.count())
                # )
                logger.info("dynamic_df Schema : {}".format(dynamic_df.printSchema()))

                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options,
                    transformation_ctx="datasink1",
                )

                logger.info("Successfully Overwrite to Target table!!")

                status = True

        except Exception as exception:
            status = False
            logger.error(
                "Unable to write DataFrame {0} to Redshift table {1}: {2}".format(
                    df, Target_table, exception
                ),
                exc_info=True,
            )
            raise IOException.IOError(
                moduleName=constant.CORE_JOB,
                exeptionType=constant.IO_WRITE_REDSHIFT_EXCEPTION,
                message=constant.DF_TO_REDSHIFT_WRITE_FAILED.format(
                    traceback.format_exc()
                ),
            )
        return status

    def query_to_redshift(self):
        """This method will query a redshift table and return query results in spark dataframe
        """
        pass

    @staticmethod
    def process(file_name, job_process_dttm):
        """The method will perform the actual processing of the file including read,dq,
        transform and write

        Returns:
            [bool] -- Status to show completness of the ETL processing
        """
        # logger = logging.getLogger("process_job")
        etl_status_sort_key_as_job_process_dttm = str(job_process_dttm)
        logger = get_logger()
        file_record_count = 0
        try:

            params = utils.get_file_config_params(file_name, logger)
            core_job = Core_Job(file_name)
            refined_bucket = core_job.env_params["refined_bucket"]
        except Exception as error:
            logger.error("Error Occurred While Initializing Process : {}".format(error))
            raise CustomAppException.CustomAppError(
                moduleName=error.moduleName,
                exeptionType=error.exeptionType,
                message=error.message,
            )

        control_file_name = core_job.get_control_file_name(file_name=core_job.file_name)
        #!Check if the file is active
        logger.info("Checking if the file is active")
        active_status = params["is_active"]
        if utils.is_feed_active(logger, active_status):
            process_status = "failed"
            job_status_params_to_be_updated = {}
            try:

                # read file from refined current directory
                logger.info(
                    "Bucket currently being used is ",
                    core_job.env_params["refined_bucket"],
                )
                df = core_job.read_from_s3(
                    bucket=refined_bucket,
                    path=params["rf_source_dir"] + "/" + file_name,
                    structype_schema=core_job.structype_schema,
                )
                file_record_count = core_job.feed_row_count
                if file_record_count == 0:
                    logger.info("Feed has 0 records. Skipping to end of job")
                    # move file from current to respective directory
                    file_moved_status = utils.move_s3_file_from_current(
                        file_name=file_name,
                        src_bucket=core_job.env_params["refined_bucket"],
                        tgt_bucket=core_job.env_params["refined_bucket"],
                        params=params,
                        logger=logger,
                    )
                    utils.move_s3_file_from_current(
                        file_name=control_file_name,
                        src_bucket=core_job.env_params["refined_bucket"],
                        tgt_bucket=core_job.env_params["refined_bucket"],
                        params=params,
                        logger=logger,
                    )
                    job_status_params_to_be_updated.update(
                        {
                            "job_end_time": str(datetime.utcnow()),
                            "file_record_count": file_record_count,
                        }
                    )
                    core_job.update_stage_status(
                        job_status_params_to_be_updated,
                        etl_status_sort_key_as_job_process_dttm,
                    )
                    return constant.success

                # applying dq_checks
                dq_check_status = core_job.data_quality_checks(
                    df, etl_status_sort_key_as_job_process_dttm
                )
                logger.info(
                    "DQ Stage Staus is : {0} After All DQ Checks For File : {1}".format(
                        dq_check_status, file_name
                    )
                )

                # applying transform on refined data
                refined_to_transformed_stage_status = core_job.transform(
                    df, etl_status_sort_key_as_job_process_dttm
                )
                logger.info(
                    "refined_to_transformed Stage Staus is : {0}  For File : {1}".format(
                        refined_to_transformed_stage_status, file_name
                    )
                )

                if refined_to_transformed_stage_status is False:
                    raise StageStatusNotSatisfiedException.StageStatusNotSatisfiedError(
                        moduleName=constant.CORE_JOB,
                        exeptionType=constant.TRANSFORMATION_EXCEPTION,
                        message=constant.TR_STAGE_FAILED_MESSAGE,
                    )

                # move file from current to respective directory
                file_moved_status = utils.move_s3_file_from_current(
                    file_name=file_name,
                    src_bucket=core_job.env_params["refined_bucket"],
                    tgt_bucket=core_job.env_params["refined_bucket"],
                    params=params,
                    logger=logger,
                )

                utils.move_s3_file_from_current(
                    file_name=control_file_name,
                    src_bucket=core_job.env_params["refined_bucket"],
                    tgt_bucket=core_job.env_params["refined_bucket"],
                    params=params,
                    logger=logger,
                )
                process_status = constant.success

            except Exception as error:
                logger.error(
                    "Error Occurred in Core Class due To {}".format(error), exc_info=1
                )
                logger.error(
                    "Moving Feed_file and CTNL files in the failed folder".format(
                        error
                    ),
                    exc_info=1,
                )
                file_moved_status = utils.move_s3_file_from_current(
                    file_name=file_name,
                    src_bucket=core_job.env_params["refined_bucket"],
                    tgt_bucket=core_job.env_params["refined_bucket"],
                    params=params,
                    logger=logger,
                    tgt_path="{}/{}".format(params["failed_dest_folder_nm"], file_name),
                )
                utils.move_s3_file_from_current(
                    file_name=control_file_name,
                    src_bucket=core_job.env_params["refined_bucket"],
                    tgt_bucket=core_job.env_params["refined_bucket"],
                    params=params,
                    logger=logger,
                    tgt_path="{}/{}".format(
                        params["failed_dest_folder_nm"], control_file_name
                    ),
                )

                process_status = constant.failure
                raise CustomAppException.CustomAppError(
                    moduleName=error.moduleName,
                    exeptionType=error.exeptionType,
                    message=error.message,
                )
            finally:
                job_status_params_to_be_updated.update(
                    {
                        "job_end_time": str(datetime.utcnow()),
                        "file_record_count": file_record_count,
                    }
                )
                core_job.update_stage_status(
                    job_status_params_to_be_updated,
                    etl_status_sort_key_as_job_process_dttm,
                )
        else:
            """Initialising spark and gluecontext to prevent exception
                user did not initialise spark context
            """
            process_status = constant.skipped
            sc = SparkContext.getOrCreate()
            glueContext = GlueContext(sc)
            file_moved_status = utils.move_s3_file_from_current(
                file_name=file_name,
                src_bucket=core_job.env_params["refined_bucket"],
                tgt_bucket=core_job.env_params["refined_bucket"],
                params=params,
                logger=logger,
            )
            utils.move_s3_file_from_current(
                file_name=control_file_name,
                src_bucket=core_job.env_params["refined_bucket"],
                tgt_bucket=core_job.env_params["refined_bucket"],
                params=params,
                logger=logger,
            )

        return process_status
