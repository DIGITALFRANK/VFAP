import logging
from pyspark.context import SparkContext
import modules.config.config as config
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
# from modules.app_log.create_logger import logger
from modules.app_log.create_logger import get_logger
from modules.utils.utils_dynamo import DynamoUtils
from modules.utils.utils_dataprocessor import utils
from datetime import datetime


class Dataprocessor_Job:
    def __init__(self, file_name, job_run_id):
        """This is the Constructor for the base class when an object of the
           base class is instantiated.

        Arguments:
            file_name {[type]} -- file_name {[String]} --
                                Name of the file that is used
            to initialise Logger ,SparkContext ,GlueContext,

        """

        self.sc = SparkContext.getOrCreate()
        glueContext = GlueContext(self.sc)
        self.file_name = file_name
        self.key = utils.get_parameter_store_key()
        print("Found parameter store key as : {}".format(self.key))
        self.env_params = utils.get_param_store_configs(self.key)
        self.redshift_details = utils.get_secret(self.env_params["secret_manager_key"])
        print("Using Redshift credentials as {}".format(self.redshift_details))
        self.spark = glueContext.spark_session
        logger = get_logger(__name__)
        self.logger = logger
        self.params = utils.get_file_config_params(file_name, self.logger)

        self.job_run_id = job_run_id

    def read_from_s3(self, delimiter, bucket, path):
        """This function reads data from s3 using spark.read method in Pyspark

        Arguments:
            bucket {[String]} -- Name of the bucket in S3
            path {String} -- s3 object key
        Returns:
            [spark.dataframe] -- Returns a spark dataframe
        """
        logger = self.logger
        params = self.params


        try:
            spark = self.spark

            s3_obj = "s3://{}/{}".format(bucket, path)
            logger.info("Reading file : {} from s3...".format(s3_obj))
            print(s3_obj)
            df = (
                spark.read.format("csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", delimiter)
                    .load(s3_obj)
            )
            print("Dataframe read successfully")
            logger.info("Schema : ")
            df.printSchema()
            logger.info("s3_ obj : {} has records : {}".format(
                s3_obj, df.count()))

        except Exception as error:
            df = None
            print("Could not read dataframe ", error)
        self.df = df
        return df

    def transform(self):
        pass

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
        print('transform started')
        try:
            s3_obj = "s3://{}/{}".format(bucket, path)
            logger.info("Writing File : {} ...".format(s3_obj))
            logger.info("s3_ obj : {} has records : {}".format(
                s3_obj, df.count()))
            df.repartition(1).write.mode(mode).format("csv").option(
                "header", "true").save(s3_obj)
            logger.info("Writing File  : {}".format(s3_obj))
            write_status = True
        except Exception as error:
            write_status = False
            logger.info(
                "Error Occurred While writing file : {} to s3 due to : {}"
                    .format(
                    path, error
                )
            )
        return write_status

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
        redshift_user = self.redshift_details["username"]
        redshift_password = self.redshift_details["password"]
        redshift_schema = self.redshift_details["dbSchema"]
        logger.info("Attempting to create jdbc url for redshift")
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            self.redshift_details["engine"],
            self.redshift_details["host"],
            self.redshift_details["port"],
            self.redshift_details["dbCatalog"],
        )

        logger.info(
            "Attempting to read table {0}.{1} from Redshift into DataFrame".format(
                redshift_schema, redshift_table
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
                )
            )
        return df

    def write_df_to_redshift_table(self, df, redshift_table, load_mode):
        """
            Parameters:

            df: pyspark.sql.DataFrame
            redshift_table: str,
            load_mode: str,

            Returns:

            None

            This function loads a DataFrame from memory into a table in AWS Redshift.
             The three possible
            modes for loading are:

                append - appends data to table

                overwrite - deletes all data in table and appends

                errorIfExists - throws an error and fails if the table exists

                ignore - if the table exists, do nothing
            """
        logger = self.logger
        redshift_user = self.redshift_details["username"]
        redshift_password = self.redshift_details["password"]
        redshift_schema = self.redshift_details["dbSchema"]
        logger.info("Attempting to create jdbc url for redshift")
        env_params = self.env_params
        temp_bucket = "s3://{}/{}/".format(env_params["refined_bucket"], "temp")
        redshift_url = "jdbc:{}://{}:{}/{}".format(
            self.redshift_details["engine"],
            self.redshift_details["host"],
            self.redshift_details["port"],
            self.redshift_details["dbCatalog"],
        )
        print(
            "Attempting to write DataFrame {} to Redshift table {}.{}".format(
                df, redshift_schema, redshift_table
            )
        )
        schema_qualified_table = redshift_schema + "." + redshift_table
        try:
            df.write.format("com.databricks.spark.redshift").option(
                "url",
                redshift_url
                + "?user="
                + redshift_user
                + "&password="
                + redshift_password,
            ).option("dbtable", schema_qualified_table).option(
                "tempdir", temp_bucket
            ).option(
                "aws_iam_role", env_params["redshift_iam_role"],
            ).save(
                mode=load_mode
            )
            print(
                "Successfully wrote DataFrame {} to Redshift table {}".format(
                    df, schema_qualified_table
                )
            )
            status = True

        except Exception as exception:
            status = False
            print(
                "Unable to write DataFrame {0} to Redshift table {1}: {2}".format(
                    df, schema_qualified_table, exception
                )
            )
        return status

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
            logger.error("Error Occured update_stage_status {}".format(error),exc_info=True)
            stage_update_status = False

        return stage_update_status


    @staticmethod
    def process(file_name, job_run_id, job_process_dttm):
        """The method will perform the actual processing of the file

        Returns:
            [bool] -- Status to show completness of the ETL processing
        """
        D1 = Dataprocessor_Job(file_name, job_run_id)
        etl_status_sort_key_as_job_process_dttm = str(job_process_dttm)
        logger = logging.getLogger("dataprocessor_job")
        logger.setLevel(logging.DEBUG)
        params = utils.get_file_config_params(file_name, logger)
        key = utils.get_parameter_store_key()
        # job_exception = config.job_exception
        env_params = utils.get_param_store_configs(key)
        redshift_details = utils.get_secret(env_params["secret_manager_key"])
        transformation_dict = params["tr_class_to_call"]
        delimiter = params["raw_source_file_delimiter"]
        job_status_params_to_be_updated = {}
        try:
            #import the class dynamically for given input
            klass = utils.get_dynamic_class(transformation_dict["1"], logger)
            tr_obj = klass(file_name, job_run_id)
            df = tr_obj.read_from_s3(delimiter,
                bucket=env_params["refined_bucket"],
                path=params["rf_source_dir"]+
                     "/" + tr_obj.file_name)
            transformed_df, date = tr_obj.transform(df)
            s3_write_status = tr_obj.write_to_tgt(
                    df=transformed_df,
                    mode="append",
                    bucket=env_params["transformed_bucket"],
                    path=params["tgt_dstn_folder_name"] + '/' + date,
                )
            status = tr_obj.write_df_to_redshift_table(
                 df=transformed_df,
                 redshift_table=params["tgt_dstn_tbl_name"],
                 load_mode="append",
             )
            status = "Success"
            job_status_params_to_be_updated.update(
                {
                    "tr_status":
                    {
                        "error_info": config.STAGE_ERROR_NULL,
                        "status": config.STAGE_COMPLETED_STATUS,
                        "update_dttm": str(datetime.utcnow()),
                    }
                }

            )
            D1.update_stage_status(
                job_status_params_to_be_updated,
                etl_status_sort_key_as_job_process_dttm, )


        except Exception as error:
            logger.error(
                "Error Occurred in Core Class due To {}".format(error))
            status = "failed"

        finally:
            # D1 = Dataprocessor_Job(file_name, job_run_id)
            job_status_params_to_be_updated.update(
                {"job_end_time": str(datetime.utcnow())}
            )
            D1.update_stage_status(
            job_status_params_to_be_updated,
            etl_status_sort_key_as_job_process_dttm,)

        return status
