import pyspark
from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge
from modules.utils.utils_dataprocessor import utils
from pyspark.sql.functions import lit, col
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql import functions as sf
import modules.config.config as config


class tr_adobe_attribution_biweekly_merge(Dataprocessor_merge):
    def __init__(self, filepattern, date):
        """Constructor for tr adobe attribution merge

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_adobe_attribution_biweekly_merge, self).__init__(filepattern,
                                                                date)
        pass

    def transform(self, df):
        sq = self.spark
        spark = self.spark
        logger = self.logger
        params = self.params
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        job_id = utils.get_glue_job_run_id()
        redshift_details = self.redshift_details
        qdate = datetime.now()-timedelta(1).strftime('%Y-%m-%d')
        env_params = self.env_params
        query = "DELETE FROM vfap_uat.vfap_attribution WHERE day = '{}';".format(
            qdate)
        delimiter = params["raw_source_file_delimiter"]
        # attribution_biweekly = \
        #     'digitallab/common_files/VF_ADOBE_BI_WEEKLY_ATTRIBUTION_CHANNEL.csv'
        attribution_weekly = 'digitallab/attributionweekly/merge/' + \
                            self.date + '/*.csv'
        try:
            print("##Applying tr_adobe_attribution_biweekly_merge ##")
            # reading weekly attribution master file
            master_df = self.redshift_table_to_dataframe(self.env_params[
                                                            "attribution_table"])
            master_df.show()
            # reading daily attribution file
            attribution_df = self.read_from_s3(
                delimiter,
                env_params["transformed_bucket"], attribution_weekly)
            attribution_df.show()
            # Format date columns
            dfdate = df.withColumn("Date", date_format(
                to_date(col("Date"), "yyyy-MM-dd"), "yyyy-MM-dd"))
            dfdate = dfdate.withColumn("Prev_Date", date_format(
                to_date(col("Prev_Date"), "yyyy-MM-dd"), "yyyy-MM-dd"))
            df_insert = dfdate.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))

            # Derive previous year values
            print("Deriving last year values")
            df_jobid.createOrReplaceTempView("df")
            master_df.createOrReplaceTempView("master")
            df_merge = sq.sql("SELECT df.Date, df.Type_Of_Attribution, "
                              "df.Channels, df.Fiscal_Week, df.Fiscal_Month, "
                              "df.Fiscal_QTR, "
                              "df.Fiscal_Year, df.Brand, "
                              "df.Country, "
                              "df.BI_Weekly_Unique_Visitors, df.BI_Weekly_Visits, "
                              "master.prev_bi_weekly_visits, "
                              "master.Prev_BI_Weekly_Unique_visitor, "
                              "df.Prev_Date, df.ETL_INSERT_TIME, "
                              "df.ETL_UPDATE_TIME, df.JOB_ID "
                              "from df df left join master master "
                              "on cast(df.Prev_Date as Date) = cast("
                              "master.day as Date) "
                              "and df.Type_Of_Attribution = "
                              "master.type_of_attribution and "
                              "df.Channels = "
                              "master.channels and "
                              "df.Brand = master.brand and "
                              "df.Country = master.country")

            df_merge.show()


            # Join with weekly attribution files
            print("Create bi-weekly merge file")
            df_merge.createOrReplaceTempView("df_merge")
            attribution_df.createOrReplaceTempView("attribution_df")
            full_load_df = sq.sql("SELECT df_merge.Date, "
                                  "df_merge.Type_Of_Attribution, "
                                  "df_merge.Channels, df_merge.Fiscal_Week, "
                                  "df_merge.Fiscal_Month, "
                                  "df_merge.Fiscal_QTR, "
                                  "df_merge.Fiscal_Year, df_merge.Brand, "
                                  "df_merge.Country, attribution_df.Orders, "
                                  "attribution_df.Sales_Local, "
                                  "attribution_df.Sales_USD, "
                                  "attribution_df.Visits, "
                                  "attribution_df.Weekly_Visits, "###
                                  "df_merge.BI_Weekly_Visits, "
                                  "attribution_df.Bounces, "
                                  "attribution_df.Entries, "
                                  "attribution_df.Units, "
                                  "attribution_df.Daily_Unique_Visitor, "
                                  "attribution_df.Weekly_Unique_Visitors, "
                                  "df_merge.BI_Weekly_Unique_Visitors, "
                                  "attribution_df.Prev_Orders, "
                                  "attribution_df.Prev_Sales_Local, "
                                  "attribution_df.Prev_Sales_USD, "
                                  "attribution_df.Prev_Visits, "
                                  "attribution_df.Prev_Weekly_Visits, "
                                  "df_merge.prev_bi_weekly_visits, "
                                  "attribution_df.Prev_Bounces, "
                                  "attribution_df.Prev_Entries, "
                                  "attribution_df.Prev_Units, "
                                  "attribution_df.Prev_Daily_Unique_Visitor, "
                                  "attribution_df.prev_weekly_unique_visitor, "
                                  "df_merge.Prev_BI_Weekly_Unique_visitor, "
                                  "df_merge.Prev_Date, "
                                  "df_merge.ETL_INSERT_TIME, "
                                  "df_merge.ETL_UPDATE_TIME, df_merge.JOB_ID "
                                  "from df_merge df_merge left "
                                  "join attribution_df attribution_df "
                                  "on cast(df_merge.Date as Date) = cast("
                                  "attribution_df.Date as Date) "
                                  "and df_merge.Type_Of_Attribution = "
                                  "attribution_df.Type_Of_Attribution and "
                                  "df_merge.Channels = "
                                  "attribution_df.Channels and "
                                  "df_merge.Brand = attribution_df.Brand and "
                                  "df_merge.Country = attribution_df.Country")

            full_load_df.show()
            delete_status = utils.execute_query_in_redshift(query,
                                                            redshift_details,
                                                            logger)
            print(delete_status)

        except Exception as error:
            full_load_df = None
            logger.info(
                "Error Occured While processiong tr adobe attribution merge due to : {}".format(
                    error))
        return full_load_df
