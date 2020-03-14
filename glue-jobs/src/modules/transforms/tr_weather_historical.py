from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
from pyspark.sql.types import IntegerType


class tr_weather_historical(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_sr_weather

        Arguments:
            TransformCore {class} -- Base class within transform module
            file_name {String} -- input file name
        """
        super(tr_weather_historical, self).__init__(file_name)

    def transform(self, df):
        """This function is overriding the default transform method in the base class
        Arguments:
            df {spark.dataframe} -- source dataframe for input file

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """
        """
        """

        full_load_df = None
        transformed_df_dict = {}
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
            logger.info("Applying tr_weather_historical")
            warehouse_df = self.redshift_table_to_dataframe(
                redshift_table=params["tgt_dstn_tbl_name"]
            )
            # add sas_process_dt column having date value from file_name
            refined_df = (
                df.withColumn("SAS_PROCESS_DT", F.lit(params["file_date"]))
                .withColumn("sas_brand_id", lit(int(params["sas_brand_id"])))
                .withColumn("process_dtm", F.current_timestamp())
                .withColumn("file_name", F.lit(self.file_name))
                .withColumn("fs_sk", lit(None).cast(IntegerType()))
            )

            # renaming columns from I_TNF_WeatherTrends_vf_historical_data file as present at destination
            logger.info("Schema After Adding SAS_PROCESS_DT : ")
            refined_df.printSchema()

            # create temp view for refined data
            logger.info(
                "Creating Temp View weather_refined_source: {}".format(
                    config.TR_WEATHER_REFINED_TEMP_VIEW
                )
            )
            refined_df.createOrReplaceTempView(config.TR_WEATHER_REFINED_TEMP_VIEW)

            # create temp view for warehouse data
            logger.info(
                "Creating Temp View weather_warehouse_source: {}".format(
                    config.TR_WEATHER_WAREHOUSE_TEMP_VIEW
                )
            )
            warehouse_df.createOrReplaceTempView(config.TR_WEATHER_WAREHOUSE_TEMP_VIEW)

            # getting changed records refined left join warehouse
            logger.info("Getting Changed records........")
            changed_records_df = spark.sql(config.chnaged_records_query)
            changed_records_df.createOrReplaceTempView(
                config.TR_WEATHER_CHANGED_RECORDS_TEMP_VIEW
            )

            logger.info("Changed Records : ")
            # changed_records_df.select(
            #     "LOCATION", "Municipality", "STATE", "COUNTY", "DT", "SAS_PROCESS_DT"
            # ).show(10, truncate=False)

            # getting retained records
            retained_records_df = spark.sql(config.retained_records_query)
            logger.info("Retained Records : ")
            # retained_records_df.select(
            #     "LOCATION", "Municipality", "STATE", "COUNTY", "DT", "SAS_PROCESS_DT"
            # ).show(10, truncate=False)

            # Union Retained and Changed Records
            full_load_df = retained_records_df.unionByName(changed_records_df)
            logger.info("Full Load  Records For Target: ")
            # full_load_df.select(
            #     "LOCATION", "Municipality", "STATE", "COUNTY", "DT", "SAS_PROCESS_DT"
            # ).show(10, truncate=False)

            # returning transformed_data_dict df
            transformed_df_dict = {params["tgt_dstn_tbl_name"]: full_load_df}

        except Exception as error:
            full_load_df = None
            transformed_df_dict = {}
            logger.error(" : {}".format(error), exc_info=True)
            raise CustomAppError(
                moduleName=constant.TR_WEARHER_HISTORICAL,
                exeptionType=constant.TRANSFORMATION_EXCEPTION,
                message="Error Ocurred in tr_weather_historical due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return transformed_df_dict
