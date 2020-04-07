# from builtins import super
from datetime import datetime, timedelta

from pyspark.sql import functions as F
from pyspark.sql.functions import *
# from pyspark.sql.types import DateType
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import lit

import modules.config.config as config
from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_adobe_ecomm(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr adobe ecomm

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_adobe_ecomm, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        print("## Applying tr_adobe_ecomm ##")
        spark = self.spark
        logger = self.logger
        params = self.params
        job_id = self.job_run_id
        env_params = self.env_params
        file_name = self.file_name
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y-%m-%d') - timedelta(1)
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            # Mapping Brand name into data frame
            df_adobe_brand = df.withColumn("BRAND", lit(params['brand']))
            # Mapping region into data frame
            df_adobe_region = df_adobe_brand.withColumn("REGION",
                                                        lit(params['region']))
            # Specify Source into data frame
            df_adobe_source = df_adobe_region.withColumn('SYSTEM',
                                                         lit(
                                                             "Adobe Analytics"))
            # Add total product view column into data frame
            df_product_view = df_adobe_source.withColumn(
                "TOTAL_PRODUCT_VIEWS", lit(0))
            # Derive sales EUR column based on region
            df_sales_eur = df_product_view.withColumn("TOTAL_SALES_EUR",
                                                      when((col('REGION') ==
                                                            'EMEA'),
                                                           col(
                                                               'REVENUE')).otherwise(
                                                          "0"))
            # Derive sales USD column based on region
            df_sales_usd = df_sales_eur.withColumn("TOTAL_SALES_USD",
                                                   when((col('REGION') ==
                                                         'EMEA'),
                                                        col(
                                                            'REVENUE') *
                                                        1.2).otherwise(
                                                       "0"))
            # removing null values from data frame
            df_remove_null = df_sales_usd.where("Day is NOT NULL")
            # format date column
            df_date = df_remove_null.withColumn("DATE", date_format(
                to_date(col("Day").cast("string"), "yyyy/MM/dd"),
                "yyyy-MM-dd"))
            # df_date = dfdate.withColumnRenamed("Day", "DATE")
            # replace device type column other with desktop value
            df_replace_devicecategory = df_date.withColumn("Device Type",
                                                           regexp_replace(
                                                               col("Device "
                                                                   "Type"),
                                                               "Other",
                                                               "Desktop"))
            # replace device type column Mobile Phone with Mobile value
            df_replace_devicecategory = df_replace_devicecategory.withColumn(
                "Device Type",
                regexp_replace(
                    col("Device Type"),
                    "Mobile Phone", "Mobile"))
            # Upper case device category values
            df_replace_devicecategory = \
                df_replace_devicecategory.withColumn('DEVICE_CATEGORY',
                                                     F.upper(
                                                         col("Device Type")))
            # Filter out the device category
            df_filter_device = df_replace_devicecategory.filter(
                col('DEVICE_CATEGORY').isin(["MOBILE", "DESKTOP", "TABLET"]))
            # Rename column names
            dftrans_units = df_filter_device.withColumnRenamed(
                "Units", "TOTAL_UNITS")
            df_revenue = dftrans_units.withColumnRenamed("Revenue",
                                                         "TOTAL_SALES")
            df_orders = df_revenue.withColumnRenamed("Orders",
                                                     "TOTAL_ORDERS")
            df_pagev = df_orders.withColumnRenamed("Page Views",
                                                   "TOTAL_PAGE_VIEWS")
            df_visitor = df_pagev.withColumn('TOTAL_VISITORS',
                                             lit(col("Unique Visitors")))
            df_cart = df_visitor.withColumnRenamed("Cart Additions",
                                                   "TOTAL_CART_ADDITIONS")
            df_unique = df_cart.withColumnRenamed("Unique Visitors",
                                                  "TOTAL_UNIQUE_VISITORS")
            df_visits = df_unique.withColumnRenamed("Visits",
                                                    "TOTAL_VISITS")
            df_insert = df_visits.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # Create final data frame
            full_load_df = df_jobid.select("SYSTEM", "BRAND", "REGION",
                                            "DEVICE_CATEGORY", "DATE",
                                            "TOTAL_UNITS", "TOTAL_SALES",
                                            "TOTAL_ORDERS",
                                            "TOTAL_PAGE_VIEWS",
                                            "TOTAL_VISITORS",
                                            "TOTAL_PRODUCT_VIEWS",
                                            "TOTAL_CART_ADDITIONS",
                                            "TOTAL_SALES_EUR",
                                            "TOTAL_SALES_USD",
                                            "TOTAL_UNIQUE_VISITORS",
                                            "TOTAL_VISITS",
                                           "ETL_INSERT_TIME",
                                           "ETL_UPDATE_TIME", "JOB_ID")
            logger.info("Data frame Created")

        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing "
                        "tr_adobe_ecomm due to : {}".format(error))
        return full_load_df, date
