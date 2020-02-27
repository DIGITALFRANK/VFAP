# import PySpark modules
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lit
from datetime import datetime

from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_googleanalytics_ecomm(Dataprocessor_Job):

    def __init__(self, file_name, job_run_id):
        """Constructor for tr google analytics eComm

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_googleanalytics_ecomm, self).__init__(file_name, job_run_id)
        pass

    # This transformation gets refined data, add SYSTEM column
    # calculates the sales in EUR for EMEA brands
    def transform(self, df):
        print("## Applying tr_googleanalytics_ecomm ##")
        job_id = self.job_run_id
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        spark = self.spark
        logger = self.logger
        params = self.params
        env_params = self.env_params
        file_name = self.file_name
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y-%m-%d')
        date = file_date_obj.strftime('%Y%m%d')
        try:
            # reading google analytics profile file
            profile_lookup = self.read_from_s3(
                params["raw_source_file_delimiter"],
                env_params["refined_bucket"], env_params["profilel_file"])
            df_casa = df.filter("profile_id like '82395699'")
            df_casa = df_casa.withColumn(
                "profile_id", lit("82395666"))
            df_google = df.union(df_casa)
            # create data frame based on profile
            df_eur_data = df_google.filter("profile not like 'USD'")
            df_usd_data = df_google.filter("profile not like 'EUR'")
            # insert brand and region in USD data frame
            df_usd_col = df_usd_data.join(profile_lookup,
                                          (df_usd_data["profile_id"] ==
                                           profile_lookup["ProfileId"]),
                                          how="left").select(df_usd_data.Date,
                                                             df_usd_data.DeviceCategory,
                                                             df_usd_data.Total_Sessions,
                                                             df_usd_data.Total_units,
                                                             df_usd_data.Total_sales,
                                                             df_usd_data.Total_orders,
                                                             df_usd_data.average_time_per_page,
                                                             df_usd_data.Total_page_views,
                                                             df_usd_data.average_session_length,
                                                             df_usd_data.Bounce_Rate,
                                                             df_usd_data.Total_visitors,
                                                             profile_lookup.Brand,
                                                             profile_lookup.Region)
            # insert brand and region in EUR data frame
            df_eur_col = df_eur_data.join(profile_lookup,
                                          (df_eur_data["profile_id"] ==
                                           profile_lookup["ProfileId"]),
                                          how="left").select(df_eur_data.Date,
                                                             df_eur_data.DeviceCategory,
                                                             df_eur_data.Total_Sessions,
                                                             df_eur_data.Total_units,
                                                             df_eur_data.Total_sales,
                                                             df_eur_data.Total_orders,
                                                             df_eur_data.average_time_per_page,
                                                             df_eur_data.Total_page_views,
                                                             df_eur_data.average_session_length,
                                                             df_eur_data.Bounce_Rate,
                                                             df_eur_data.Total_visitors,
                                                             profile_lookup.Brand,
                                                             profile_lookup.Region)
            # Create view for Spark SQL
            df_usd_col.createOrReplaceTempView("usd")
            df_eur_col.createOrReplaceTempView("eur")
            # create merge data frame for google
            merge_df = spark.sql(
                "select usd.Date, usd.DeviceCategory, eur.Total_Sessions, "
                "eur.Total_units, eur.Total_sales, eur.Total_orders, "
                "eur.average_time_per_page, eur.Total_page_views, "
                "eur.average_session_length, usd.Bounce_Rate, "
                "eur.Total_visitors, usd.Brand, usd.Region, "
                "case when usd.Brand = 'ICEBREAKER EU' then "
                "usd.Total_sales*1.2 "
                "when usd.Brand = 'ICEBREAKER UK' then "
                "usd.Total_sales*1.32 else usd.Total_sales end "
                "as Total_sales_USD from usd usd left join eur eur on "
                "usd.Date == eur.Date and usd.Brand == eur.Brand and "
                "usd.Region == eur.Region and "
                "usd.DeviceCategory == eur.DeviceCategory")
            # Add SYSTEM column to data frame
            dftrans_system = merge_df.withColumn("SYSTEM",
                                                 lit("Google Analytics"))
            # Upper case device category
            dftrans_device = dftrans_system.withColumn(
                'DEVICE_CATEGORY', F.upper(F.col("DeviceCategory")))
            # replace NULL with 0 in Units column
            dftrans_units = dftrans_device.fillna({'Total_units': 0})
            dftrans_units = dftrans_units.withColumnRenamed("Total_units",
                                                            "TOTAL_UNITS")
            # add Total Product Views column
            dftrans_product = dftrans_units.withColumn(
                "TOTAL_PRODUCT_VIEWS", lit(0))
            # add Total card addition column
            dftrans_cart = dftrans_product.withColumn(
                "TOTAL_CART_ADDITIONS", lit(0))
            # Derice sales EUR column based on region
            dftrans_eur = dftrans_cart.withColumn("TOTAL_SALES_EUR",
                                                  when((col('REGION') ==
                                                        'EMEA'),
                                                       col('Total_sales')).otherwise("0"))
            # Derive sales USD column based on region
            dftrans_usd = dftrans_eur.withColumn("TOTAL_SALES_USD",
                                                 when((col('REGION') ==
                                                       'EMEA'),
                                                      col(
                                                          'Total_sales_USD')).otherwise(
                                                     "0"))
            # Derive Total Unique Visitor column
            dftrans_unique = dftrans_usd.withColumn("TOTAL_UNIQUE_VISITORS",
                                                    col('Total_visitors'))
            # Derive Total Visits column
            dftrans_visits = dftrans_unique.withColumn("TOTAL_VISITS",
                                                       col('Total_Sessions'))

            # Derive total visitor column based on Region
            dftrans_visitor = dftrans_visits.withColumn("TOTAL_VISITORS",
                                                        when((col('REGION')
                                                              == 'EMEA'),
                                                             col(
                                                                 'Total_Sessions')).otherwise(
                                                            col(
                                                                "Total_visitors")))
            # format date column
            dfdate = dftrans_visitor.withColumn("DATE", date_format(
                to_date(col("Date").cast("string"), "yyyy-MM-dd"),
                "yyyy-MM-dd"))
            # upper case the column names
            # dftrans_rename = dfdate.withColumnRenamed("Date", "DATE")
            dftrans_sales = dfdate.withColumnRenamed("Total_sales",
                                                     "TOTAL_SALES")
            dftrans_orders = dftrans_sales.withColumnRenamed(
                "Total_orders", "TOTAL_ORDERS")
            dftrans_pageviews = dftrans_orders.withColumnRenamed(
                "Total_page_views", "TOTAL_PAGE_VIEWS")
            # dftrans_totalvisitors = dftrans_pageviews.withColumnRenamed(
            #     "Total_visitors", "TOTAL_VISITORS")
            df_insert = dftrans_pageviews.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # create final data frame
            df_jobid.createOrReplaceTempView("GOOGLE")
            full_load_df = spark.sql("SELECT SYSTEM, BRAND, REGION, "
                                     "DEVICE_CATEGORY, DATE, TOTAL_UNITS, "
                                     "TOTAL_SALES, TOTAL_ORDERS, "
                                     "TOTAL_PAGE_VIEWS, TOTAL_VISITORS, "
                                     "TOTAL_PRODUCT_VIEWS, "
                                     "TOTAL_CART_ADDITIONS, TOTAL_SALES_EUR, "
                                     "TOTAL_SALES_USD, "
                                     "TOTAL_UNIQUE_VISITORS, TOTAL_VISITS,"
                                     "ETL_INSERT_TIME, ETL_UPDATE_TIME, "
                                     "JOB_ID  FROM GOOGLE")
            logger.info("Transformed DF Count : {}".format(
                full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing "
                        "tr_googleanalytics_ecomm due to : {}".format(error))
        return full_load_df, date
