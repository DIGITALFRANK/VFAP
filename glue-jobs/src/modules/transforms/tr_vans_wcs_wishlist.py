from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from modules.utils.utils_core import utils
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class tr_vans_wcs_wishlist(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_vans_wcs_wishlist

        Arguments:
            TransformCore {class} -- Base class within transform module
            file_name {string} -- Name of the file
        """
        super(tr_vans_wcs_wishlist, self).__init__(file_name)

    def transform(self, df):
        """This function is overriding the default transform method in the base class
        Arguments:
            df {spark.dataframe} -- source dataframe for input file

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """
        logger = self.logger
        params = self.params
        redshift_table = params["tgt_dstn_tbl_name"]
        logger.info("Applying tr_vans_wcs_wishlist")
        transformed_df_dict = {}
        try:
            #Re-run Process
            re_run_delete_status = utils.re_run_table(
                    redshift_table, self.whouse_details,self.file_name,logger
                )
            logger.info("re_run_process started...")
            if re_run_delete_status == True:
                logger.info("Starting transformation")
                    #Updating Date Format And SAS Product ID information
                transformed_df = (
                    df.select(
                        "*",
                        F.when(
                            df.PRODUCT_ID.startswith("NF:"), F.split(df.PRODUCT_ID, ":")[1]
                        )
                        .when(
                            df.PRODUCT_ID.startswith("VN:"), F.split(df.PRODUCT_ID, ":")[1]
                        )
                        .when(
                            df.PRODUCT_ID.startswith("VN-"),
                            F.substring(F.split(df.PRODUCT_ID, "-")[1], 0, 6),
                        )
                        .otherwise(F.split(df.PRODUCT_ID, ":")[0])
                        .alias("SAS_PRODUCT_ID"),
                    )
                    .withColumn("sas_brand_id", F.lit(int(params["sas_brand_id"])))
                    .withColumn("process_dtm", F.current_timestamp())
                )
                #    print("Transformed DF Schema \n")
                transformed_df.printSchema()
                logger.info("Transformed Dataframe is  \n")
                transformed_df.show(10, truncate=False)
                transformed_df_dict = {params["tgt_dstn_tbl_name"]: transformed_df}
            else:
                logger.info("Re-run process has failed")
                transformed_df_dict={}
        except Exception as error:
            transformed_df = None
            transformed_df_dict = {}
            logger.error(
                "Error Occurred While processing tr_vans_wcs_wishlist\
                            due to: {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_VANS_WCS_WISHLIST,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing tr_vans_wcs_wishlist : {}".format(
                                     traceback.format_exc()))
        return transformed_df_dict
