from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
import modules.config.config as config
from datetime import datetime, timedelta
# import sys
# from pyspark.context import SparkContext
# from pyspark.sql.types import DateType
from pyspark.sql.functions import col, udf, regexp_replace, lit
# from datetime import timedelta, date, datetime
# from pyspark.sql import SQLContext
# from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


class tr_adobe_attribution(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr adobe attribution

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_adobe_attribution, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        sq = self.spark
        print("##Applying tr_adobe_attribution ##")
        print("filename inside transform", self.file_name)
        spark = self.spark
        job_id = self.job_run_id
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger = self.logger
        params = self.params
        env_params = self.env_params
        delimiter = self.params["raw_source_file_delimiter"]
        filename = self.file_name
        file_parts = filename.split("_")
        # deriving value for attribution type
        attribution_type = file_parts[3] + " " + file_parts[4] + " " + \
                           file_parts[5]
        channel = file_parts[3] + " " + file_parts[4] + " " + "Marketing" + \
                  " " + file_parts[5]
        file_date = filename.split("_")[-1].split(".")[0]
        file_date_obj = datetime.strptime(file_date, '%Y-%m-%d') - timedelta(1)
        date = file_date_obj.strftime('%Y%m%d')
        # spaceDeleteUDF = udf(lambda s: s.replace(" ", "_"), StringType())
        # df.withColumn("Revenue (Fixed)3", spaceDeleteUDF("Revenue (
        # Fixed)3")).show()
        # params['tgt_dstn_folder_name'] = params['tgt_dstn_folder_name'] +
        # "/" + file_date + "/" + params["brand"]
        # extra_args = "/" + file_date + "/" + params["brand"]
        print("inside transforms", params["tgt_dstn_folder_name"])

        if filename.lower().find('us') != -1:
            salse_local = "Revenue (Fixed)3"
            salse_usd = "Revenue (Fixed)4"
        elif filename.lower().find('ca') != -1 and filename.lower().find(
                'first') != -1:
            salse_local = "Revenue (Fixed) (Canada)"
            salse_usd = "Revenue (USD)"
        else:
            salse_local = "Revenue (Fixed)"
            salse_usd = "Revenue (USD)"
        print(salse_local)
        df.show()
        try:
                # reading fiscal calender file  and master file from s3
            fiscal_calender_df = self.redshift_table_to_dataframe(
                self.env_params["fiscal_table"])
            masterdf = self.redshift_table_to_dataframe(self.env_params[
                                                            "attribution_table"])

            # remove null value in file
            dfnotnull = df.where("Day is NOT NULL")

            # correcting the date formate from YYYY/MM/DD to YYYY-MM-DD
            dfdate = dfnotnull.withColumn("Day", date_format(to_date(col(
                "Day"), "yyyy/MM/dd"), "yyyy-MM-dd"))
            # deriving the fiscal calendar value by looking up the date column
            dfdate = dfdate.withColumnRenamed("Day", "Date")
             # adding Type_Of_Attribution column to df
            dfattribution = dfdate.withColumn("Type_Of_Attribution",
                                              lit(str(attribution_type)))

            join_df = fiscal_calender_df.join(
                dfattribution,
                fiscal_calender_df["date"] == dfattribution["Date"]).select(
                dfattribution['*'],
                fiscal_calender_df.fiscalmonth.alias("Fiscal_Month"),
                fiscal_calender_df.fiscalweek.alias("Fiscal_Week"),
                fiscal_calender_df.fiscalqtr.alias("Fiscal_QTR"),
                fiscal_calender_df.fiscalyear.alias("Fiscal_Year"),
                fiscal_calender_df.prev_fiscalyear.alias("Prev_Fiscal_YEAR"),
                fiscal_calender_df.prev_date,
               )
            # deriving the last year value from master file looking up
                # previous date

            # adding brand column to df
            dfbrand = join_df.withColumn("Brand", lit(params['brand']))

            # adding country colum n to df
            dfcountry = dfbrand.withColumn("Country", lit(params["country"]))
            dfprevdate = dfcountry.withColumn("prev_date", date_format(
                to_date(col("prev_date"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
             # renaming channel marketing column name to channel
            dfchannel = dfprevdate.withColumnRenamed("{}".format(channel),
                                                     "Channel")
            dfwvisit = dfchannel.withColumn("weekly_visits", lit(''))
            dfbwvisits = dfwvisit.withColumn("bi_weekly_visits", lit(''))
            dfwuvisitors = dfbwvisits.withColumn("Weekly_Unique_Visitor",
                                                 lit(''))
            dfbwuvisitor = dfwuvisitors.withColumn(
                "BI_Weekly_Unique_visitor", lit(''))
            dfsaleslocal = dfbwuvisitor.withColumnRenamed(salse_local,
                                                          "Sales_Local")
            dfsalseusd = dfsaleslocal.withColumnRenamed(salse_usd, "Sales_USD")
            print("before next join")

            # deriving previous year value from looking up master file
            df_with_master = dfsalseusd.join(
                masterdf,
                ((dfsalseusd.prev_date == masterdf.day) &
                 (dfsalseusd.Type_Of_Attribution ==
                  masterdf.type_of_attribution) &
                 (dfsalseusd.Country == masterdf.country) &
                 (dfsalseusd.Channel == masterdf.channels) &
                 (dfsalseusd.Brand == masterdf.brand)), how="left").select(
                dfsalseusd['*'],
                masterdf.orders.alias("Prev_Orders"),
                masterdf.sales_local.alias("Prev_Sales_Local"),
                masterdf.sales_usd.alias("Prev_Sales_USD"),
                masterdf.visits.alias("Prev_Visits"),
                masterdf.weekly_visits.alias("prev_weekly_visits"),
                masterdf.bi_weekly_visits.alias("prev_bi_weekly_visits"),
                masterdf.bounces.alias("Prev_Bounces"),
                masterdf.entries.alias("Prev_Entries"),
                masterdf.units.alias("Prev_Units"),
                masterdf.daily_unique_visitor.alias(
                    "Prev_Daily_Unique_Visitor"),
                masterdf.weekly_unique_visitor.alias(
                    "Prev_Weekly_Unique_Visitor"),
                masterdf.bi_weekly_unique_visitor.alias(
                    "Prev_BI_Weekly_Unique_visitor")
               )
            dfduvisitors = df_with_master.withColumnRenamed('Daily Unique '
                                                            'Visitors', 'Daily_Unique_Visitor')
            df_insert = dfduvisitors.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            df_jobid.createOrReplaceTempView("adobe_attribution_final")
            full_load_df = sq.sql("select Date, Type_Of_Attribution, "
                                  "Channel as Channels, Fiscal_Week, "
                                  "Fiscal_Month, Fiscal_QTR, Fiscal_Year, "
                                  "Brand, Country, Orders, Sales_Local, "
                                  "Sales_USD, Visits, weekly_visits, "
                                  "bi_weekly_visits, Bounces, Entries, Units, "
                                  "Daily_Unique_Visitor, "
                                  "Weekly_Unique_Visitor, "
                                  "BI_Weekly_Unique_visitor, Prev_Orders, "
                                  "Prev_Sales_Local,Prev_Sales_USD, "
                                  "Prev_Visits, prev_weekly_visits, "
                                  "prev_bi_weekly_visits, Prev_Bounces, "
                                  "Prev_Entries, Prev_Units, "
                                  "Prev_Daily_Unique_Visitor, "
                                  "Prev_Weekly_Unique_Visitor, "
                                  "Prev_BI_Weekly_Unique_visitor, prev_date, "
                                  "ETL_INSERT_TIME, ETL_UPDATE_TIME, JOB_ID "
                                  "from adobe_attribution_final")
            full_load_df.show()

            logger.info("Transformed DF Count : {}".format(
                full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing tr_adobe_attribution "
                        "due to : {}".format(error))
        return full_load_df, date
