from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.types import *
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class tr_weather_forecast(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr weather forecast

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """

        super(tr_weather_forecast, self).__init__(file_name)

    def transform(self, df):
        spark = self.spark
        params = self.params
        logger = self.logger
        logger.info(" Applying tr_weather_forecast  ")
        full_load_df = None
        transformed_df_dict = {}
        try:
            warehouse_df = self.redshift_table_to_dataframe(redshift_table=params['tgt_dstn_tbl_name'])
            logger.info("count of records in warehouse_df {}".format(warehouse_df.count()))
            df.printSchema()
            warehouse_df.printSchema()
            warehouse_max_df = (
                warehouse_df.agg({"DT": "max"})
                .withColumnRenamed("max(DT)", "max_DT")
                .collect()
            )

            logger.info("warehouse_max_df is {}".format(warehouse_max_df))
            warehouse_max_dt = warehouse_max_df[0].max_DT.date()
            logger.info("warehouse date is {}".format(warehouse_max_dt))
            refined_max_df = (
                df.agg({"DT": "max"})
                .withColumnRenamed("max(DT)", "max_DT")
                .collect()
            )

            logger.info("refined_max_df is {}".format(refined_max_df))
            refined_max_dt = refined_max_df[0].max_DT.date()

            logger.info("refined max date is {}".format(refined_max_dt))
            if warehouse_max_dt < refined_max_dt:
                logger.info(
                    "truncate the existing warehouse data and insert refined data"
                )
                full_load_df = df.withColumn("fs_sk", lit(None).cast(IntegerType())).withColumn("sas_brand_id", lit(int(params["sas_brand_id"]))).withColumn("process_dtm", F.current_timestamp())
            else:
                logger.info("No need to append")
                full_load_df = warehouse_df
            #returning transformed_data_dict df
            transformed_df_dict = {
                params["tgt_dstn_tbl_name"]: full_load_df
            }
        except Exception as error:
            full_load_df = None
            transformed_df_dict={}
            logger.error(
                "Error Occurred While processing tr_weather_forecast due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_WEATHER_FORECAST,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing tr_weather_forecast due to : {}".format(
                                     traceback.format_exc()))
        return transformed_df_dict
