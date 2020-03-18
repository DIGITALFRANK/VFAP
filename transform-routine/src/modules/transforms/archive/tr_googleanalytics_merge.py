# Import python modules
from datetime import timedelta, date, datetime
# Import PySpark modules
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DateType
from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge
import modules.config.config as config


class tr_googleanalytics_merge(Dataprocessor_merge):
    def __init__(self, file_pattern, date):
        """Constructor for tr google analytics merge

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_googleanalytics_merge, self).__init__(file_pattern, date)
        pass

    # This transformation gets raw data, get brand based on profile id,
    # and merge them and create a single file
    def transform(self, df):
        print("## Applying tr_googleanalytics_merge ##")
        logger = self.logger
        params = self.params
        try:

            df_eur_data = df.filter("profile not like 'USD'")
            df_usd_data = df.filter("profile not like 'EUR'")
            df_eur_data.show()
            df_usd_data.show()
            df_eur_data_brand = df_eur_data.withColumn("Brand", lit(""))
            df_usd_data_brand = df_usd_data.withColumn("Brand", lit(""))
            df_eur_data_region = df_eur_data_brand.withColumn("Region", lit(""))
            df_usd_data_region = df_usd_data_brand.withColumn("Region", lit(""))
            # Add brand column to data frame. based on profile id in
            # dynamoDB, brand would be derived
            df_eur_data_brand = df_eur_data_brand.withColumn("BRAND", lit(params['brand']))
            df_usd_data_brand = df_usd_data_brand.withColumn("BRAND", lit(params['brand']))
            df_eur_data_region = df_eur_data_region.withColumn("REGION", lit(params[
                                                                                 'region']))
            df_usd_data_region = df_usd_data_region.withColumn("REGION", lit(params[
                                                                                 'region']))

            full_load_df = df_usd_data_region.join(df_eur_data_region, ((df_usd_data_region[
                                                                             "DeviceCategory"] ==
                                                                         df_eur_data_region[
                                                                             "DeviceCategory"]) & (
                                                                                df_usd_data_region[
                                                                                    "Date"] ==
                                                                                df_eur_data_region[
                                                                                    "Date"]) & (
                                                                                df_usd_data_region[
                                                                                    "Brand"] ==
                                                                                df_eur_data_region[
                                                                                    "Brand"]) & (
                                                                                df_usd_data_region[
                                                                                    "Region"] ==
                                                                                df_eur_data_region[
                                                                                    "Region"])),
                                                   how="left").select(df_usd_data_region.Date,
                                                                      df_usd_data_region.DeviceCategory,
                                                                      df_eur_data_region.Total_Sessions,
                                                                      df_eur_data_region.Total_units,
                                                                      df_eur_data_region.Total_sales,
                                                                      df_eur_data_region.Total_orders,
                                                                      df_eur_data_region.average_time_per_page,
                                                                      df_eur_data_region.Total_page_views,
                                                                      df_eur_data_region.average_session_length,
                                                                      df_usd_data_region.Bounce_Rate,
                                                                      df_eur_data_region.Total_visitors,
                                                                      df_usd_data_region.Brand,
                                                                      df_usd_data_region.Region,
                                                                      df_usd_data_region.Total_sales.alias(
                                                                          "Total_sales_USD"))

        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing "
                        "tr_googleanalytics_merge due to : {}".format(error))
        return full_load_df
