# import PySpark modules
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit
from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
import modules.config.config as config

class tr_weather(Dataprocessor_Job):

    def __init__(self, file_name, job_run_id):
        """Constructor for tr weather

        Arguments:
             file_name -- name of file which is being passed to base class
        """
        super(tr_weather, self).__init__(file_name, job_run_id)
        pass

    def transform(self, df):
        print("## Applying tr_weather ##")
        spark = self.spark
        job_id = self.job_run_id
        logger = self.logger
        params = self.params
        fiscal_delimiter = ','
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            file_name = self.file_name
            #Reading the fiscal calender into dataframe
            df_fiscal_calender = self.redshift_table_to_dataframe(self.env_params[
                                                             "fiscal_table"])
            #Splitting the filename to get File Date
            x = file_name.split('_')
            #Renaming the columns of the weather dataframe
            df_location = df.withColumn("Location", substring('Location', 3, 5))
            df_weather = df_location.withColumnRenamed("Location", "Zip5")
            df_weather = df_weather.withColumnRenamed("MinTemp[degF]", "MinTemp_degF")
            df_weather = df_weather.withColumnRenamed("MaxTemp[degF]", "MaxTemp_degF")
            df_weather = df_weather.withColumnRenamed("Prcp[in]", "Prcp_in")
            df_weather = df_weather.withColumnRenamed("Snow[in]", "Snow_in")
            df_weather = df_weather.withColumnRenamed("Wspd[mph]", "Wspd_mph")
            df_weather = df_weather.withColumnRenamed("Wdir[deg]", "Wdir_deg")
            df_weather = df_weather.withColumnRenamed("Gust[mph]", "Gust_mph")
            df_weather = df_weather.withColumnRenamed("RH[%]", "RH_per")
            df_weather = df_weather.withColumnRenamed("Skyc[%]", "Skyc_per")
            df_weather = df_weather.withColumnRenamed("Pres[mb]", "Pres_mb")
            df_weather = df_weather.withColumnRenamed("Pop[%]", "Pop_per")
            df_weather = df_weather.withColumnRenamed("MinTempLY[degF]", "MinTempLY_degF")
            df_weather = df_weather.withColumnRenamed("MaxTempLY[degF]", "MaxTempLY_degF")
            df_weather = df_weather.withColumnRenamed("MinTempNorm[degF]", "MinTempNorm_degf")
            df_weather = df_weather.withColumnRenamed("MaxTempNorm[degF]", "MaxTempNorm_degF")
            #converting the file_date to the desired format
            filedate = datetime.strptime(x[-1][0:8], '%Y%m%d')
            df_file_date = df_weather.withColumn("file_date", lit(datetime.strftime(filedate, '%Y-%m-%d')))
            df_insert = df_file_date.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))

            #creating the temporary view of weather dataframe
            df_jobid.createOrReplaceTempView("Weather")
            #creating the temporary view of fiscal_calender dataframe
            df_fiscal_calender.createOrReplaceTempView("fiscal")
            #writing a sql query to lookup weather dataframe with fiscal calender to get prev year date
            df_merged = spark.sql(
                "select m.Zip5, m.Municipality, m.State, m.County, "
                "cast(m.Date as Date), m.MinTemp_degF, m.MaxTemp_degF,"
                " m.Prcp_in, m.Snow_in, m.Wspd_mph, m.Wdir_deg, m.Gust_mph, "
                "m.RH_per, m.Skyc_per, m.Pres_mb, m.Pop_per, m.UVIndex, "
                "m.wxIcon, m.MinTempLY_degF, m.MaxTempLY_degF, m.MinTempNorm_degF,"
                " m.MaxTempNorm_degF, m.file_date, cast(l.prev_date as "
                "Date), m.ETL_INSERT_TIME, m.ETL_UPDATE_TIME, m.JOB_ID from "
                "weather m left outer join fiscal l on m.Date = l.date")
            #doing the groupby of the dataframe according to required columns and calculating the average of the values
            merged_df = df_merged.groupby(
                "State", "Date", "file_date", "prev_date",
                "ETL_INSERT_TIME", "ETL_UPDATE_TIME", "JOB_ID").agg(
                f.avg(f.col("MinTemp_degF")).alias('MinTemp_degF'),
                f.avg(f.col("MaxTemp_degF")).alias('MaxTemp_degF'),
                f.avg(f.col("Prcp_in")).alias('Prcp_in'),
                f.avg(f.col("Snow_in")).alias('Snow_in'),
                f.avg(f.col("Wspd_mph")).alias('Wspd_mph'),
                f.avg(f.col("Wdir_deg")).alias('Wdir_deg'),
                f.avg(f.col("Gust_mph")).alias('Gust_mph'),
                f.avg(f.col("RH_per")).alias('RH_per'),
                f.avg(f.col("Skyc_per")).alias('Skyc_per'),
                f.avg(f.col("Pres_mb")).alias('Pres_mb'),
                f.avg(f.col("Pop_per")).alias('Pop_per'),
                f.avg(f.col("UVIndex")).alias('UVIndex'),
                f.avg(f.col("wxIcon")).alias('wxIcon'),
                f.avg(f.col("MinTempLY_degF")).alias('MinTempLY_degF'),
                f.avg(f.col("MaxTempLY_degF")).alias('MaxTempLY_degF'),
                f.avg(f.col("MinTempNorm_degF")).alias('MinTempNorm_degF'),
                f.avg(f.col("MaxTempNorm_degF")).alias('MaxTempNorm_degF'))
            merged_df.createOrReplaceTempView("Weather")
            full_load_df = spark.sql("select State, Date as Original_date, "
                                     "MinTemp_degF, file_date as File_Date, "
                                     "MaxTemp_degF, Prcp_in, Snow_in, Wspd_mph, "
                                     "Wdir_deg, Gust_mph, RH_per as RH, "
                                     "Skyc_per as Skyc, Pres_mb, Pop_per as Pop, "
                                     "UVIndex, wxIcon, MinTempLY_degF, "
                                     "MaxTempLY_degF, MinTempNorm_degF, "
                                     "MaxTempNorm_degF, prev_date as "
                                     "Prev_Date, ETL_INSERT_TIME, "
                                     "ETL_UPDATE_TIME, JOB_ID from Weather")
            full_load_df.show()
        except Exception as error:
            full_load_df = None
            logger.info("Error Occurred While processiong "
                        "tr_weather due to : {}".format(error))
        return full_load_df, x[-1][0:8]
