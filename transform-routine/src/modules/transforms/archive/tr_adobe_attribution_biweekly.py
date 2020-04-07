from datetime import timedelta, datetime

from pyspark.sql.functions import *
# from pyspark import SparkContext
# from pyspark.sql import SQLContext
from pyspark.sql.functions import lit, col

from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_adobe_attribution_biweekly(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr adobe attribution

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_adobe_attribution_biweekly, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        sq = self.spark
        print("## Applying tr_adobe_attribution_biweekly ##")
        spark = self.spark
        logger = self.logger
        params = self.params
        env_params = self.env_params
        job_id = self.job_run_id
        delimiter = self.params["raw_source_file_delimiter"]
        filename = self.file_name
        file_parts = filename.split("_")
        # deriving value for attribution type
        attribution_type = file_parts[3] + " " + file_parts[4] + " Channel"
        # deriving value for Channel
        channel = file_parts[3] + " " + file_parts[4] + " Channel"
        file_date = filename.split("_")[-1].split(".")[0]
        file_date_obj = datetime.strptime(file_date, '%Y-%m-%d')
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("inside transforms", params["tgt_dstn_folder_name"])

        try:
            # reading fiscal calender file  and master file from s3
            fiscal_calender_df = self.redshift_table_to_dataframe(
                self.env_params["fiscal_table"])
            # adding Date column
            df_date = df.withColumn("Date", lit(date))
            # Format Date column
            df_date = df_date.withColumn('Date',
                                         date_format(to_date(col("Date"),
                                                             "yyyyMMdd"),
                                                     "yyyy-MM-dd"))
            # Create Channel column
            df_channel = df_date.withColumnRenamed("{}".format(channel),
                                                   "Channels")
            # Rename Unique Visitor column
            df_visitor = df_channel.withColumnRenamed("Unique Visitors",
                                                      "BI_Weekly_Unique_Visitors")
            # Rename Visits column
            df_visit = df_visitor.withColumnRenamed("Visits",
                                                    "BI_Weekly_Visits")
            # Create Type of Attribution column
            df_attribution = df_visit.withColumn("Type_Of_Attribution",
                                                 lit(str(attribution_type)))
            # Create Brand column
            df_brand = df_attribution.withColumn("Brand",
                                                 lit(params['brand']))
            # Create country column
            df_country = df_brand.withColumn("Country",
                                             lit(params["country"]))
            # Filter out null values
            df_null = df_country.where("Date is NOT NULL")
            # Derive fiscal calendar fields
            df_null.createOrReplaceTempView("attribution")
            fiscal_calender_df.createOrReplaceTempView("fiscal_calendar")
            df_fiscal = sq.sql("SELECT attr.Date, attr.Type_Of_Attribution, "
                               "attr.Channels, fiscal.fiscalweek as "
                               "Fiscal_Week, fiscal.fiscalmonth as "
                               "Fiscal_Month, fiscal.fiscalqtr as "
                               "Fiscal_QTR, fiscal.fiscalyear as Fiscal_Year, "
                               "attr.Brand, attr.Country, "
                               "attr.BI_Weekly_Unique_Visitors, "
                               "attr.BI_Weekly_Visits, "
                               "fiscal.prev_date as Prev_Date FROM "
                               "attribution attr cross join fiscal_calendar "
                               "fiscal on cast(attr.Date as Date) = "
                               "cast(fiscal.date as Date)")
            # Format prev year date column
            df_fiscal = df_fiscal.withColumn("Prev_Date",
                                                date_format(
                                                    to_date(col("Prev_Date"),
                                                            "yyyy-MM-dd"),
                                                    "yyyy-MM-dd"))
            df_insert = df_fiscal.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            full_load_df = df_update.withColumn("JOB_ID", lit(job_id))

            logger.info(
                "Transformed DF Count : {}".format(full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info(
                "Error Occurred While processing "
                "tr_adobe_attribution_biweekly due to : {}".format(error))
            raise Exception("{}".format(error))
        return full_load_df, date
