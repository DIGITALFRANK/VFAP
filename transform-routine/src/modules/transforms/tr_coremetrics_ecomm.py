# import PySpark modules
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from datetime import datetime

from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_coremetrics_ecomm(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr Coremetrics eComm

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_coremetrics_ecomm, self).__init__(file_name, job_run_id)
        pass

    # This transformation gets refined data, add brand and region, add us
    # and EMEA sales column and aggregate the data
    def transform(self, df):
        print("## Applying tr_coremetrics_ecomm ##")
        logger = self.logger
        params = self.params
        file_name = self.file_name
        print("File name-----" + file_name)
        sq = self.spark
        print("Spark initialized")
        job_id = self.job_run_id
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y%m%d')
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:

            # Add data source column to data frame
            dftrans_datasource = df.withColumn("SYSTEM", lit("Core Metrics"))
            # Add brand column to data frame. based on profile id in
            # dynamoDB, brand would be derived
            dftrans_brand = dftrans_datasource.withColumn("BRAND",
                                                          lit(params['brand']))
            # Add region column to data frame
            dftrans_region = dftrans_brand.withColumn("REGION", lit(params[
                                                                        'region']))
            # Upper case Device Category
            df_device = dftrans_region.withColumn("Device_category",
                                           when(col("Device_category").contains(
                                               20931847), "desktop")
                                           .when(col("Device_category").contains(
                                               20931867), "mobile")
                                           .when(col("Device_category").contains(
                                               20931880), "tablet"))

            df_upper = df_device.withColumn('DEVICE_CATEGORY',
                                                        F.upper(F.col("Device_category")))
            # format date column
            dfdate = df_upper.withColumn("DATE", date_format(
                to_date(col("startDate").cast("string"), "yyyyMMdd"),
                "yyyy-MM-dd"))
            # func = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
            # dfdate = df_upper.withColumnRenamed('DATE', func(col('startDate')))
            # Rename columns as per requirement
            dfunit = dfdate.withColumnRenamed("TL_TOTAL_ITEMS_ORDERED",
                                              "TOTAL_UNITS")
            dfsales = dfunit.fillna({'TL_TOTAL_SALES': 0})
            dfsales = dfsales.withColumnRenamed("TL_TOTAL_SALES",
                                                "TOTAL_SALES")
            dforders = dfsales.withColumnRenamed("TL_TOTAL_ORDERS",
                                                 "TOTAL_ORDERS")
            dfpagev = dforders.withColumnRenamed("TL_TOTAL_PAGE_VIEWS",
                                                 "TOTAL_PAGE_VIEWS")
            dfvisitors = dfpagev.withColumnRenamed("TL_TOTAL_VISITORS",
                                                   "TOTAL_VISITORS")
            dfproduct = dfvisitors.withColumnRenamed(
                "TL_TOTAL_PRODUCT_VIEWS", "TOTAL_PRODUCT_VIEWS")
            dfcart = dfproduct.withColumn("TOTAL_CART_ADDITIONS", lit(0))
            dfeur = dfcart.withColumn("TOTAL_SALES_EUR", lit(0))
            dfusd = dfeur.withColumn("TOTAL_SALES_USD", lit(0))
            dfunique = dfusd.withColumnRenamed("UNIQUE_VISITORS",
                                               "TOTAL_UNIQUE_VISITORS")
            dfsession = dfunique.withColumnRenamed("TL_TOTAL_SESSIONS",
                                                  "TOTAL_VISITS")
            df_insert = dfsession.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # create final data frame
            df_jobid.createOrReplaceTempView("COREMETRICS")
            full_load_df = sq.sql("SELECT SYSTEM, BRAND, REGION, "
                                  "DEVICE_CATEGORY, DATE, TOTAL_UNITS, "
                                  "TOTAL_SALES, TOTAL_ORDERS, "
                                  "TOTAL_PAGE_VIEWS, TOTAL_VISITORS, "
                                  "TOTAL_PRODUCT_VIEWS, "
                                  "TOTAL_CART_ADDITIONS, TOTAL_SALES_EUR, "
                                  "TOTAL_SALES_USD, TOTAL_UNIQUE_VISITORS, "
                                  "TOTAL_VISITS, ETL_INSERT_TIME, "
                                  "ETL_UPDATE_TIME, JOB_ID  FROM COREMETRICS")
            logger.info("Transformed DF Count : {}".format(full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info("Error Occurred While processiong "
                        "tr_coremetrics_ecomm due to : {}".format(error))
        return full_load_df, date
