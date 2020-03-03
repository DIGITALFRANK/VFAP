from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from pyspark.sql.functions import lit
from modules.utils.utils_core import utils
from pyspark.sql.types import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class tr_append_brand_id(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_append_brand_id

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """

        super(tr_append_brand_id, self).__init__(file_name)
        self.logger.info("inside tr append brand id constructor")

    def transform(self, df):
        self.logger.info(" Applying tr_append_brand_id ")
        refined_df_df = None
        data_df_dict={}
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
            

            table_exists = utils.check_existence_of_redshift_table(
                spark, params["tgt_dstn_tbl_name"], self.whouse_details, logger
            )
            logger.info("table is present {}".format(table_exists))
            refined_df = df
            refined_df.show()
            logger.info("count of records in refined_df {}".format(refined_df.count()))
            logger.info("param value in tr brand is {}".format(params["sas_brand_id"]))
            redshift_table = params["tgt_dstn_tbl_name"]

            if table_exists == True:
                logger.info("Table is present in redshift")
                re_run_delete_status = utils.re_run_table(
                    redshift_table, self.whouse_details,self.file_name, logger
                )
                logger.info("re_run_process started...")
                if re_run_delete_status == True:
                    
                    logger.info("Query to execute.")
                    query = ("Delete from " + self.whouse_details["dbSchema"] + "." + redshift_table + " where sas_brand_id = {}" .format(params["sas_brand_id"]))
                    query_status = utils.execute_query_in_redshift(query,self.whouse_details,logger)
                    
                    logger.info(
                        "Query executed successfully!!"
                    )

                    if query_status:

                        refined_df_df = (
                            refined_df.withColumn("sas_brand_id", lit(int(params["sas_brand_id"])))
                            .withColumn("process_dtm", F.current_timestamp())
                        )
                        logger.info(
                            "count of records in adding sas_brand_id refined_df_df {}".format(
                                refined_df_df.count()
                            )
                        )
                    else:
                        logger.info("Query Execution Failed")                    
                    
                else:
                    logger.info("Re-run process has failed")

            else:
                logger.info("Table is not present in redshift")
                refined_df_df = (
                    refined_df.withColumn("sas_brand_id", lit(int(params["sas_brand_id"])))
                    .withColumn("process_dtm", F.current_timestamp())
                )
            data_df_dict = {
                params["tgt_dstn_tbl_name"]:refined_df_df
            }
        except Exception as error:
            refined_df_df = None
            data_df_dict = {}
            logger.error(
                "Error Occurred While processing tr_append_brand_id due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_APPEND_BRAND_ID, exeptionType=constant.TRANSFORMATION_EXCEPTION, message="Error Occurred While processing tr_append_brand_id due to : {}".format(traceback.format_exc()))
        return data_df_dict
