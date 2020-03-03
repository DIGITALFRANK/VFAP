from pyspark.sql import functions as F
from pyspark.sql.types import *
from modules.core.core_job import Core_Job
from modules.utils.utils_core import utils
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback


class tr_wcs_return(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_wcs_return

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """
        super(tr_wcs_return, self).__init__(file_name)
        # super(tr_vans_wcs_wishlist, self).__init__(df, logger)

    def transform(self, df):
        """This function is overriding the default transform method in the base class

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """
        logger = self.logger
        params = self.params
        redshift_table = params["tgt_dstn_tbl_name"]
        logger.info("Applying tr_wcs_return")
        transformed_df_dict = {}

        try:
            # Re-run Process
            re_run_delete_status = utils.re_run_table(
                redshift_table, self.whouse_details,self.file_name,logger
            )
            logger.info("re_run_process started...")
            if re_run_delete_status == True:
                # Updating Date Format And SAS Product ID information
                df.show(10)
                print("the source df")
                intermediate_transformed_df = df.select(
                    "*",
                    F.when(
                        F.length(df.date) == 8,
                        F.to_date(df.date, "yyyyMMdd"),
                    )
                        .when(
                        F.length(df.date) == 10,
                        F.to_date(df.date, "yyyy-MM-dd"),
                    )
                        .otherwise(F.to_date(df.date, "yyyy-MM-dd"))
                        .alias("dt"),
                    F.when(df.productid.startswith("NF:"), F.split(df.productid, ":")[1])
                        .when(df.productid.startswith("VN:"), F.split(df.productid, ":")[1])
                        .when(
                        df.productid.startswith("VN-"),
                        F.substring(F.split(df.productid, "-")[1], 0, 6),
                    )
                        .otherwise(F.split(df.productid, ":")[0])
                        .alias("sas_product_id"),
                )
                # Creating SAS_BRAND_ID & PROCESS_DTM variable
                transformed_df = (
                    intermediate_transformed_df.drop("date")
                        .withColumn("sas_brand_id", F.lit(int(params["sas_brand_id"])))
                        .withColumn("process_dtm", F.current_timestamp())
                )
                logger.info("Schema")
                logger.info("Transformed DF Count : {}".format(transformed_df.count()))
                transformed_df_dict = {
                    params["tgt_dstn_tbl_name"]: transformed_df
                }
            else:
                logger.info("Re-run process has failed")
                transformed_df_dict = {}

        except Exception as error:
            transformed_df = None
            transformed_df_dict = {}
            logger.error(
                "Error Occurred While processing tr_wcs_return\
                            due to: {}".format(
                    error
                ), exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_WCS_RETURN,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing tr_wcs_return : {}".format(
                                     traceback.format_exc()))
        return transformed_df_dict
