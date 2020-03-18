
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import lit, dayofmonth, year, col, \
    regexp_replace, trim, when
from pyspark.sql.types import DateType
from pyspark.sql.functions  import date_format, to_date
from datetime import datetime
from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job


class tr_hitwise(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr hitwise

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_hitwise, self).__init__(file_name, job_run_id)
        pass

    # This transformation gets refined data, removes null values,
    # add region, brand and calculate two metrics required
    def transform(self, df):
        print("## Applying tr_hitwise ##")
        sq = self.spark
        logger = self.logger
        params = self.params
        filename = self.file_name
        job_id = self.job_run_id
        file_delimiter = ','
        file_name = self.file_name
        file_date = file_name.rsplit('_', 1)
        file_part = file_date[1].strip('.csv')
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(file_part)
        try:
            state = self.read_from_s3(file_delimiter,
                                          self.env_params["refined_bucket"],
                                          self.env_params["state_mapping"])
            competitor = self.read_from_s3(file_delimiter,
                                          self.env_params["refined_bucket"],
                                          self.env_params["competitor_mapping"])
            brand_lookup = self.read_from_s3(file_delimiter,
                                             self.env_params["refined_bucket"],
                                             self.env_params[
                                                 "brand_mapping"])

            if filename.lower().find('portfolio_search-terms') != -1:
                df.show()
                # remove % from '% Clicks' column
                df_date = df.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                df_clicks = df_date.withColumn("% Clicks", trim(
                    regexp_replace(col("% Clicks"),
                                   "%", "")))

                # rename '% CLicks' with Click_Per
                df_clicks_per = df_clicks.withColumnRenamed("% Clicks",
                                                            "Click_Per")
                df_insert = df_clicks_per.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
                # Transform Click_Per values if it contains <cutoff
                full_load_df = df_jobid.withColumn("Click_Per",
                                                        when((col(
                                                            'Click_Per')
                                                              == '<cutoff'),
                                                             0.00).otherwise(
                                                            col("Click_Per")))


            elif filename.lower().find('portfolio_downstream-websites_') != -1:
                df_date = df.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # remove % from '% Clicks' column
                df_clicks = df_date.withColumn("% Clicks", trim(
                    regexp_replace(col("% Clicks"),
                                   "%", "")))
                # rename '% CLicks' with Click_Per
                df_clicks_per = df_clicks.withColumnRenamed("% Clicks",
                                                            "Click_Per")
                df_insert = df_clicks_per.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
                # Transform Click_Per values if it contains <cutoff
                full_load_df = df_jobid.withColumn(
                    "Click_Per", when((col('Click_Per') == '<cutoff'),
                                      0.00).otherwise(col("Click_Per")))


            elif filename.lower().find('industries_device-breakdown') != -1:
                # Select the require columns from data frame
                df_desktop = df.select("Date", "Industry", "Domain", "URL",
                                       "% Desktop")
                df_date = df_desktop.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # remove % from the '% Desktop' column
                df_desktop_percentage = df_date.withColumn("% Desktop", trim(
                    regexp_replace(col("% Desktop"),
                                   "%", "")))
                # Transform '% Desktop' values if it contains <cutoff
                df_desktop_cutoff = df_desktop_percentage.withColumn(
                    "% Desktop",
                    when((col("% Desktop") == '< cutoff'),
                         0.00).otherwise(col("% Desktop")))
                # rename '% Desktop' with Per_Share
                df_desktop_per = df_desktop_cutoff.withColumnRenamed("% "
                                                                     "Desktop",
                                                                     "Per_Share")
                # Include device ID as 1 for Desktop
                desktop_datafreame = df_desktop_per.withColumn("Device_Id",
                                                               lit(1))

                # Select the require columns from data frame
                df_mobile = df.select("Date", "Industry", "Domain", "URL",
                                      "% Mobile")
                df_date = df_mobile.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # remove % from the '% Mobile' column
                df_mobile_percentage = df_date.withColumn("% Mobile", trim(
                    regexp_replace(col("% Mobile"),
                                   "%", "")))
                # Transform '% Mobile' values if it contains <cutoff
                df_mobile_cutoff = df_mobile_percentage.withColumn("% Mobile",
                                                                   when((col(
                                                                       "% Mobile")
                                                                         ==
                                                                         '< cutoff'),
                                                                        0.00).otherwise(
                                                                       col(
                                                                           "% Mobile")))
                # rename '% Mobile' with Per_Share
                df_mobile_per = df_mobile_cutoff.withColumnRenamed("% Mobile",
                                                                   "Per_Share")
                # Include device ID as 1 for Mobile
                mobile_dataframe = df_mobile_per.withColumn("Device_Id",
                                                            lit(2))
                # Merge Desktop and Mobile data frame
                merge_df = desktop_datafreame.union(mobile_dataframe)
                df_insert = merge_df.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                full_load_df = df_update.withColumn("JOB_ID", lit(job_id))


            elif filename.lower().find(
                    'custom-category_clickstream-websites') != -1:
                df_date = df.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # remove % from '% Clicks' column
                df_clicks = df_date.withColumn("% Clicks", trim(
                    regexp_replace(col("% Clicks"),
                                   "%", "")))
                # rename '% CLicks' with Click_Per
                df_clicks_per = df_clicks.withColumnRenamed("% Clicks",
                                                            "Click_Per")
                # Rename Brand with Industry Brand
                df_brand = df_clicks_per.withColumnRenamed("Brand",
                                                               "Industry_Brand")
                df_insert = df_brand.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                full_load_df = df_update.withColumn("JOB_ID", lit(job_id))


            elif filename.lower().find('industries_website-ranking') != -1:
                df_date = df.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # rename TV to Total Visitor
                df_visitor = df_date.withColumnRenamed("TV", "Total_Visitors")
                # Transform Total Visitor values if it contains < 500
                df_visitor = df_visitor.withColumn(
                    "Total_Visitors", when((col('Total_Visitors') == '< 500'),
                                           500).otherwise(col("Total_Visitors")
                                                          ))
                # remove < from 'Visit %' column
                df_visit = df_visitor.withColumn("Visit %",
                                                 trim(regexp_replace(
                                                     col("Visit %"),
                                                     "<", "")))
                # remove % from 'Visit %' column
                df_visit = df_visit.withColumn("Visit %",
                                               trim(regexp_replace(
                                                   col("Visit %"),
                                                   "%", "")))
                # rename 'Visit %' with Visit_Per
                df_visit_per = df_visit.withColumnRenamed("Visit %",
                                                          "Visit_Per")
                # Convert time into seconds
                df_avt = df_visit_per.withColumn("AVT", hour(col(
                    "AVT")) * 3600 + minute(col("AVT")) * 60 +
                                                 second(col("AVT")))
                # Rename AVT to Avg Time Second
                df_time = df_avt.withColumnRenamed("AVT",
                                                        "Avg_Time_Seconds")
                df_insert = df_time.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                full_load_df = df_update.withColumn("JOB_ID", lit(job_id))


            elif filename.lower().find(
                    'custom-category_demographic-lifestyle') != -1:
                df_date = df.withColumn("Date", date_format(
                    to_date(col("Date").cast("string"), "yyyyMMdd"),
                    "yyyy-MM-dd"))
                # remove % from Visits column
                df_visits = df_date.withColumn("% Visits", trim(
                    regexp_replace(col("% Visits"),
                                   "%", "")))
                # rename 'Visit %' with Visit_Per
                df_visits = df_visits.withColumnRenamed("% Visits",
                                                        "visit_percentage")
                # Convert Visits column to integer
                df_visit_int = df_visits.withColumn("visit_percentage",
                                                    col(
                                                        "visit_percentage").cast(
                                                        "double"))
                # Pivot data frame
                df_pivot = df_visit_int.groupBy("Date", "Domain", "URL",
                                                "visit_percentage"). \
                    pivot("Type").agg(expr("coalesce(first(Attribute), '')"))
                df_dma = df_pivot.withColumnRenamed("DMA®", "DMA")
                df_house = df_dma.withColumnRenamed("Household Income",
                                                    "Household_Income")
                df_mtype = df_house.withColumnRenamed("Mosaic USA 2011 Type",
                                                      "Mosaic_USA_2011_Type")
                df_mgroup = df_mtype.withColumnRenamed("Mosaic USA 2011 Group",
                                                       "Mosaic_USA_2011_Group")

                df_mgroup.createOrReplaceTempView("v1")
                state.createOrReplaceTempView("v2")
                competitor.createOrReplaceTempView("v3")
                df_merge = sq.sql(
                    "select v1.*, v2.PostalCode, v3.Competitor, v3.Brand from v1 v1 "
                    "left join v2 v2 on lower(v1.State) = v2.State left join v3 v3 "
                    "on lower(v1.Domain) = v3.Domain")
                df_merge.createOrReplaceTempView("v1")
                df_var1 = sq.sql(
                    "SELECT *, CASE WHEN URL like 'darntough.com' THEN "
                    "'DARNTOUGH' WHEN URL like 'vfoutlet.com' THEN 'VFOUTLET' "
                    "ELSE right(URL, length(URL) - instr(URL, '.')) END as var1 "
                    "from v1")
                df_var1.createOrReplaceTempView("v2")
                df_var2 = sq.sql("SELECT *, CASE WHEN var1  like '%.%' THEN "
                                 "upper(left(var1, instr(var1, '.') - 1)) ELSE var1 END as "
                                 "var2 "
                                 "from v2")

                df_dma1 = df_var2.withColumn("DMA", when(
                    length(col("PostalCode")) > 0, "")
                                             .otherwise(col("DMA")))
                df_mosaic = df_dma1.withColumn("Mosaic_USA_2011_Type",
                                               when(length(
                                                   col("PostalCode")) > 0, "")
                                               .when(length(col("DMA")) > 0,
                                                     "")
                                               .otherwise(col(
                                                   "Mosaic_USA_2011_Type")))
                df_competitor = df_mosaic.withColumn("Competitor",
                                                     when(length(col(
                                                         "Competitor")) > 0,
                                                          col("Competitor"))
                                                     .otherwise("N/A"))
                df_brand = df_competitor.withColumn("Brand", when(
                    length(col("Brand")) > 0,
                    col("Brand"))
                                                    .otherwise(col("var2")))
                df_insert = df_brand.withColumn("ETL_INSERT_TIME", lit(now))
                df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
                df_jobid = df_update.withColumn("JOB_ID", lit(job_id))

                df_jobid.createOrReplaceTempView("demographic")
                df_demographic = sq.sql(
                    "SELECT Date, Domain, URL, visit_percentage, Age, DMA, "
                    "Gender, Household_Income, Mosaic_USA_2011_Group, "
                    "Mosaic_USA_2011_Type, PostalCode as State, "
                    "upper(Competitor) as Competitor, upper(Brand) "
                    "as Brand, ETL_INSERT_TIME, ETL_UPDATE_TIME, JOB_ID from "
                    "demographic where length(Competitor) > 0 "
                    "and var2 != 'VFOUTLET' ")
                df_demographic.show()
                df_demographic_brand = self.write_to_tgt(df_demographic,
                                                         self.env_params[
                                                             "transformed_bucket"],
                                                         'digitallab/hitwise/custom-category_demographic/' + file_part)

                df_brand.createOrReplaceTempView("demographic_brand")
                df_demographic_brand = sq.sql("SELECT Date, Domain, URL, "
                                              "visit_percentage, "
                                              "Age, DMA, Gender, Household_Income, "
                                              "Mosaic_USA_2011_Group, Mosaic_USA_2011_Type, "
                                              "PostalCode as State, upper(Brand) "
                                              "as Brand, ETL_INSERT_TIME, "
                                              "ETL_UPDATE_TIME, JOB_ID "
                                              "from demographic_brand where "
                                              "Competitor = 'N/A'")
                df_demographic_brand.show()

                df_demographic_brand.createOrReplaceTempView(
                    "demographic_brand")
                brand_lookup.createOrReplaceTempView("brand_lookup")
                full_load_df = sq.sql("SELECT b.Date, b.Domain, b.URL, "
                                      "b.visit_percentage, b.Age, b.DMA, b.Gender, "
                                      "b.Household_Income, b.Mosaic_USA_2011_Group, "
                                      "b.Mosaic_USA_2011_Type, b.State, "
                                      "CASE WHEN l.BRAND_ID > 0 THEN l.BRAND_ID ELSE 0 "
                                      "END as Band_ID from "
                                      "demographic_brand b left join brand_lookup l on "
                                      "b.Brand = l.BRAND")

        except Exception as error:
            full_load_df = None
            logger.info(
                "Error Occured While processing tr_hitwise due "
                "to : {}".format(error))
        return full_load_df, file_part
