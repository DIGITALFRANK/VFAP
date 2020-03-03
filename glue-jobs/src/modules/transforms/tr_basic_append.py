from pyspark.sql.functions import lit
from modules.core.core_job import Core_Job
from pyspark.sql import functions as F
from modules.utils.utils_core import utils
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class tr_basic_append(Core_Job):
    def __init__(self, file_name):
        """constructor for tr_basic_append

        Arguments:
            Core {class} -- Inheriting from the base class
            file_name {String} -- Name of the file to be processed.Parameter is
            received from main driver function in main.py
        """
        super(tr_basic_append, self).__init__(file_name)
        # super(tr_basic_append, self).__init__(df, logger)

    def transform(self, df):
        """This is the default transformation method used by the base class

        Arguments:
            df {spark.dataframe} -- spark dataframe
            params {dict} -- A dictionary of parameters from dynamo DB

        Returns:
            [spark.dataframe] -- Spark dataframe without coalesce after applying all
            the transformations
        """
        logger = self.logger
        params = self.params
        redshift_table = params["tgt_dstn_tbl_name"]
        logger.info("Applying tr_basic_append")
        transformed_df_dict = {}
        try:
            transformed_df=None
            re_run_delete_status = utils.re_run_table(
                    redshift_table, self.whouse_details,self.file_name, logger
                )
            logger.info("re_run_process started...")

            if re_run_delete_status == True:
                # transformed_df = df.withColumn("customer_id", lit(None).cast("string")).withColumn("sas_product_id", lit(None).cast("string")).withColumn("process_dtm", F.current_timestamp()).withColumn("sas_brand_id", lit(params["sas_brand_id"]))
                transformed_df = df.withColumn(
                    "process_dtm", F.current_timestamp()
                ).withColumn("sas_brand_id", lit(int(params["sas_brand_id"])))
                logger.info("Transformed DF Schema")
                transformed_df.printSchema()
                transformed_df.show()

                logger.info("Transformed DF Count : {}".format(transformed_df.count()))
                transformed_df_dict = {
                    params["tgt_dstn_tbl_name"]: transformed_df
                }
            else:
                logger.info("Re-run process has failed")
                transformed_df_dict={}


        except Exception as error:
            transformed_df = None
            transformed_df_dict = {}
            logger.error(
                "Error Occurred While processing Tr Basic Append due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_BASIC_APPEND,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing Tr Basic Append due to : {}".format(
                                     traceback.format_exc()))
        return transformed_df_dict
