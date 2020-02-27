from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
from datetime import datetime
from pyspark.sql.functions import lit


class tr_youtube(Dataprocessor_Job):

    def __init__(self, file_name, job_run_id):
        """Constructor for tr youtube

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_youtube, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        print("## Applying tr_youtube ##")
        spark = self.spark
        logger = self.logger
        params = self.params
        job_id = self.job_run_id
        file_name = self.file_name
        file_delimiter = ','
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        file_date_obj = datetime.strptime(file_part, '%Y-%m-%d')
        date = file_date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            lookup_df = self.read_from_s3(file_delimiter, self.env_params["refined_bucket"],
                                          self.env_params["youtube_lookup"])
            df_viewcount = df.fillna({'ViewCount': 0})
            df_subscribercount = df_viewcount.fillna({'SubscriberCount': 0})
            df_videocount = df_subscribercount.fillna({'VideoCount': 0})
            df_CommentCount = df_videocount.fillna({'CommentCount': 0})
            df_insert = df_CommentCount.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            # create final data frame
            lookup_df.createOrReplaceTempView("lookup")
            df_jobid.createOrReplaceTempView("youtube")
            full_load_df = spark.sql("select l.Source as Source, l.Region as "
                                     "Region, cast(m.Date as Date), "
                                     "m.ViewCount, m.SubscriberCount, "
                                     "m.VideoCount, m.CommentCount, "
                                     "m.ETL_INSERT_TIME, m.ETL_UPDATE_TIME, "
                                     "m.JOB_ID from youtube m, lookup l "
                                     "where m.Channel_id = l.channel_id")
            full_load_df.show()

        except Exception as error:
            full_load_df = None
            logger.info("Error Occurred While processiong "
                        "tr_youtube due to : {}".format(error))
        return full_load_df, date
