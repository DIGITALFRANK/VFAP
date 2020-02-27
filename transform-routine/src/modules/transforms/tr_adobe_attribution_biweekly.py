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
        print("##Applying tr_adobe_attribution_biweekly ##")
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
        channel = file_parts[3] + " " + file_parts[4] + " Marketing Channel"
        file_date = filename.split("_")[-1].split(".")[0]
        file_date_obj = datetime.strptime(file_date, '%Y-%m-%d') - timedelta(1)
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print("inside transforms", params["tgt_dstn_folder_name"])

        try:
            # reading fiscal calender file  and master file from s3
            fiscal_calender_df = self.redshift_table_to_dataframe(
                self.env_params["fiscal_table"])
            # Format Date column
            df_date = df.withColumn("Date Range",
                                    date_format(to_date(col("Date Range"),
                                                        "yyyy/MM/dd"),
                                                "yyyy-MM-dd"))
            # Adding days to Date column
            df_date = df_date.withColumn('Date Range',
                                         date_add('Date Range', 13))
            # Rename Date column
            df_date = df_date.withColumnRenamed("Date Range", "Date")
            # Create Channel column
            df_channel = df_date.withColumnRenamed("{}".format(channel),
                                                   "Channels")
            # Rename Unique Visitor column
            df_visitor = df_channel.withColumnRenamed("Unique Visitors",
                                                      "BI_Weekly_Unique_Visitors")
            # Rename Visits column
            df_visit = df_visitor.withColumnRenamed("Visits", "BI_Weekly_Visits")
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
            df_null.show()
            # Derive fiscal calendar fields
            df_fiscal = df_null.join(fiscal_calender_df,
                                     fiscal_calender_df["date"] == df_null[
                                         "Date"]).select(
                df_null.Date,
                df_null.Type_Of_Attribution,
                df_null.Channels,
                fiscal_calender_df.fiscalweek.alias("Fiscal_Week"),
                fiscal_calender_df.fiscalmonth.alias("Fiscal_Month"),
                fiscal_calender_df.fiscalqtr.alias("Fiscal_QTR"),
                fiscal_calender_df.fiscalyear.alias("Fiscal_Year"),
                df_null.Brand,
                df_null.Country,
                df_null.BI_Weekly_Unique_Visitors,
                df_null.BI_Weekly_Visits,
                fiscal_calender_df.prev_date.alias("Prev_Date")
            )
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
                "Error Occured While processing tr_adobe_attribution due to "
                ": {}".format(error))
        return full_load_df, date
