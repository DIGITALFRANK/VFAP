from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
import modules.config.config as config
from datetime import datetime
from pyspark.sql.functions import col, udf, regexp_replace, lit
from pyspark.sql.functions import *


class tr_snapchat(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr snapchat

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_snapchat, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        print("##Applying tr_snapchat ##")
        print("filename inside transform", self.file_name)
        spark = self.spark
        logger = self.logger
        params = self.params
        env_params = self.env_params
        delimiter = self.params["raw_source_file_delimiter"]
        filename = self.file_name
        job_run_id = self.job_run_id
        file_parts = filename.rsplit("_", 1)
        file = file_parts[1].strip('.csv')
        date_obj = datetime.strptime(file, '%Y-%m-%d')
        final_date = date_obj.strftime('%Y%m%d')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        try:
            # validating the timestamp value :taking 10 characters from left
            dftimestamp = df.withColumn("timestamp", lit(date_obj))
            df_insert = dftimestamp.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_run_id))
            #converting null values to 0
            df_full_load = df_jobid.na.fill(0)
            df_full_load.show()
            logger.info("Transformed DF Count : {}".format(df_full_load.count()))

        except Exception as error:
            df_full_load = None
            logger.info("Error Occured While processing tr_snapchat due to : {}".format(error))
        return df_full_load, final_date
