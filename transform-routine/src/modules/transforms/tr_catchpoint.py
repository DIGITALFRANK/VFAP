# import PySpark modules
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, col
from datetime import datetime

from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_catchpoint(Dataprocessor_Job):

    def __init__(self, file_name, job_run_id):
        """Constructor for tr_catchpoint

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_catchpoint, self).__init__(file_name, job_run_id)
        pass

    # This transformation gets refined data
    def transform(self, df):
        print("## Applying tr_catchpoint ##")
        sq = self.spark
        logger = self.logger
        params = self.params
        env_params = self.env_params
        job_id = self.job_run_id
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        fiscal_delimiter = ','
        file_name = self.file_name
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y-%m-%d')
        date = file_date_obj.strftime('%Y%m%d')
        try:
            # Load fiscal calendar
            fiscal_lookup = self.redshift_table_to_dataframe(self.env_params[
                                                             "fiscal_table"])
            # Load brand mapping
            brand_lookup = self.read_from_s3(fiscal_delimiter,
                                              self.env_params[
                                                  "refined_bucket"],
                                              self.env_params["brand_mapping"])
            # Format Date column
            df_date = df.withColumn("Date", date_format(
                to_date(col("Date").cast("string"), "yyyy-MM-dd"),
                "yyyy-MM-dd"))
            # Derive Device category ID from Device_Category column
            df_device = df_date.withColumn("Device_ID",
                                           when(F.lower(col("Device_Category")).contains(
                                               "desktop"), "1")
                                           .when(F.lower(col("Device_Category")).contains(
                                               "mobile"), "2")
                                           .when(F.lower(col("Device_Category")).contains(
                                               "tablet"), "3"))
            # Include source column name
            df_source = df_device.withColumn("Source_Type", lit("CATCHPOINT"))
            # Replace all null values with 0 in file
            df_fillna = df_source.na.fill("0")
            # Derive prev year date
            df_prevdate = fiscal_lookup.join(df_fillna,
                                             fiscal_lookup["date"] ==
                                             df_fillna["Date"]).select(
                df_fillna['*'], fiscal_lookup.prev_date.alias("Prev_Date"))
            # Format prev date column
            df_date = df_prevdate.withColumn("Prev_Date", date_format(
                to_date(col("Prev_Date").cast("string"), "yyyy-MM-dd"),
                "yyyy-MM-dd"))
            # Standardize the Brand name
            df_brand = df_date.withColumn("Brand",
                                           when(F.lower(col("Brand_ADJ")).contains(
                                               "kipling"), "KIPLING US")
                                           .when(F.lower(col("Brand_ADJ")).contains(
                                               "tnf"), "TNF US")
                                           .when(F.lower(col("Brand_ADJ")).contains(
                                               "timberland"), "TIMBERLAND US")
                                          .when(F.lower(col("Brand_ADJ")).contains(
                                               "vans"), "VANS US")
                                          .otherwise(col("Brand_ADJ")))
            # Derive brand ID using brand lookup
            df_brandid = brand_lookup.join(df_brand,
                                             brand_lookup["BRAND"] ==
                                             df_brand["Brand"]).select(
                df_brand['*'], brand_lookup.BRAND_ID.alias("Brand_ID"))
            df_insert = df_brandid.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # create final data frame
            df_jobid.createOrReplaceTempView("catchpoint")
            # Create final data frame
            full_load_df = sq.sql("SELECT Date, Avg_Response_ms, "
                                  "Avg_Total_Downloaded_Bytes, "
                                  "Avg_DOM_Load_ms, "
                                  "Avg_Document_Complete_ms, "
                                  "Avg_Webpage_Response_ms, "
                                  "Avg_Render_Start_ms, "
                                  "Avg_Time_to_Title_ms, Avg_No_Connections, "
                                  "Avg_No_Hosts, Availability, No_Runs, "
                                  "Brand_ID, Device_ID, Prev_Date, "
                                  "Source_Type, ETL_INSERT_TIME, "
                                  "ETL_UPDATE_TIME, JOB_ID  FROM catchpoint")
            full_load_df.show()
            logger.info(
                "Transformed DF Count : {}".format(full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info("Error Occurred While processing "
                        "tr_catchpoint due to : {}".format(error))
            raise Exception("{}".format(error))
        return full_load_df, date
