# import PySpark modules
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from datetime import datetime

from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_twitter(Dataprocessor_Job):

    def __init__(self, file_name, job_run_id):
        """Constructor for tr google analytics eComm

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_twitter, self).__init__(file_name, job_run_id)
        pass

    # This transformation gets refined data, derive brand name
    def transform(self, df):
        print("## Applying tr_twitter ##")
        spark = self.spark
        logger = self.logger
        params = self.params
        env_params = self.env_params
        job_id = self.job_run_id
        file_name = self.file_name
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y-%m-%d')
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            # Capitalize column name
            df_twitterid = df.withColumnRenamed("TWITTER_Id", "TWITTER_ID")
            # Derive brand based on Twitter Name
            df_brand = df_twitterid.withColumn("BRAND",
                                               when(F.lower(df_twitterid[
                                                                "TWITTER_NAME"]) == "eaglecreekgear",
                                                    "EAGLE CREEK")
                                               .when(F.lower(df_twitterid[
                                                                 "TWITTER_NAME"]) == "eastpak",
                                                     "EASTPAK")
                                               .when(F.lower(df_twitterid[
                                                                 "TWITTER_NAME"]) == "timberland",
                                                     "TIMBERLAND")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "vans_66",
                                                         "VANS")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "lucyactivewear",
                                                         "LUCY")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "thenorthface",
                                                         "TNF")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "kiplingglobal",
                                                         "KIPLING")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "kiplingusa",
                                                         "KIPLING US")
                                               .when(F.lower(df_twitterid[
                                                                     "TWITTER_NAME"]) == "kiplingph",
                                                         "KIPLING APAC")
                                               .when(F.lower(df_twitterid[
                                                                 "TWITTER_NAME"]) == "smartwool",
                                                     "SMARTWOOL")
                                               .when(F.lower(df_twitterid[
                                                                 "TWITTER_NAME"]) == "jansport",
                                                     "JANSPORT")
                                               .when(F.lower(df_twitterid[
                                                                 "TWITTER_NAME"]) == "red_kap",
                                                     "REDKAP")
                                               .otherwise(""))
            df_insert = df_brand.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # create final data frame
            df_jobid.createOrReplaceTempView("twitter")
            full_load_df = spark.sql(
                "select cast(DATE as Date), TWITTER_ID, TWITTER_NAME, "
                "AUTHOR, FOLLOWERS_COUNT, FOLLOWING_COUNT, LISTED_IN, "
                "TWEETS_COUNT, FAVOURITES_COUNT, BRAND, ETL_INSERT_TIME, "
                "ETL_UPDATE_TIME, JOB_ID from twitter")
            logger.info(
                "Transformed DF Count : {}".format(full_load_df.count()))
        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing "
                        "tr_googleanalytics_ecomm due to : {}".format(error))
        return full_load_df, date
