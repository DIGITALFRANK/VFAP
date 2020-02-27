
from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
import modules.config.config as config
from datetime import datetime
import sys
from pyspark.context import SparkContext
from pyspark.sql.types import DateType
from pyspark.sql.functions import col,udf, regexp_replace,lit
import pyspark.sql.functions as F

from datetime import timedelta, date, datetime
from pyspark.sql import SQLContext
#from pyspark.sql.types import StringType
from pyspark.sql.functions import *
import re


class tr_foresee(Dataprocessor_Job):
    def __init__(self, file_name, job_run_id):
        """Constructor for tr foresee

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_foresee, self).__init__(file_name, job_run_id)
        pass


    def transform(self, df):
         print("##Applying tr_foresee ##")
         print("filename inside trancform", self.file_name)
         sq = self.spark
         logger = self.logger
         params = self.params
         job_id = self.job_run_id
         env_params = self.env_params
         delimiter = self.params["raw_source_file_delimiter"]
         lookup_delimiter = ','
         file_name = self.file_name
         file_date = file_name.rsplit('-', 1)
         file_part = file_date[1].strip('.txt')
         file_date_obj = datetime.strptime(file_part, '%m%d%y') - timedelta(1)
         date = file_date_obj.strftime('%Y%m%d')
         now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


         filename = self.file_name
         file_parts = filename.split("_")
         # deriving value for brand from filename
         brand = params["brand"]
         # deriving value for survey type from filename
         survey_type = file_parts[4]
         country = params["country"]

         # Column conditions
         store_columns_for_condition1 = config.STORE_COLUMNS_FOR_CONDITION1
         mobile_columns_for_condition1 = config.MOBILE_COLUMNS_FOR_CONDITION1
         browser_columns_for_condition1 = config.BROWSER_COLUMNS_FOR_CONDITION1

         store_columns_for_condition2 = config.STRORE_COLUMNS_FOR_CONDITION2
         mobile_columns_for_condition2 = config.MOBILE_COLUMNS_FOR_CONDITION2
         browser_columns_for_condition2 = config.BROWSER_COLUMNS_FOR_CONDITION2

         browser_columns_for_condition3 = config.BROWSER_COLUMNS_FOR_CONDITION3

         # Load fiscal calendar
         fiscal_lookup = self.redshift_table_to_dataframe(self.env_params[
                                                             "fiscal_table"])
         # Load brand mapping
         brand_lookup = self.read_from_s3(lookup_delimiter,
                                          self.env_params[
                                              "refined_bucket"],
                                          self.env_params["brand_mapping"])

         try:
             if filename.lower().find('store') > -1:
                 column_condition1 = store_columns_for_condition1
                 column_condition2 = store_columns_for_condition2
                 column_condition3 = []

                 for columns in column_condition1:
                     print(columns)
                     df = df.withColumn(columns,
                                        when(col(columns).contains("10"), 10).
                                        when(length(col(columns)) == 1,
                                             0).otherwise(
                                            col(columns).substr(1, 1)))

                 for columns in column_condition2:
                     print(columns)
                     df = df.withColumn(columns,
                                        when(length(col(columns)) > 1,
                                             regexp_extract(col(columns),
                                                            "-(.*)",
                                                            1)).otherwise(
                                            None))

                 if column_condition3:
                     for columns in column_condition3:
                         df = df.withColumn(columns,
                                            when(length(col(columns)) > 1,
                                                 col(columns).substr(1,
                                                                     1)).otherwise(
                                                0))


                 df = df.withColumn("Date", date_format(
                     to_date(col("Date").cast("string"), "MM/dd/yyyy"),
                     "yyyy-MM-dd"))
                 df = df.withColumnRenamed("Satisfaction - Expectations",
                                           "Satisfaction_Expectations")
                 df = df.withColumnRenamed("Satisfaction - Ideal",
                                           "Satisfaction_Ideal")
                 df = df.withColumnRenamed("Satisfaction - Overall",
                                           "Satisfaction_Overall")
                 df = df.withColumnRenamed("Products - Apparel",
                                           "Look_and_Feel_Appeal")
                 df = df.withColumnRenamed("STORE-NAME", "Store_Name")
                 df = df.withColumnRenamed("STORE-ID", "Store_ID")
                 df = df.withColumn("Brand", lit(brand))
                 df = df.withColumn("Survey_Type", lit(survey_type))
                 df = df.withColumn("Region", lit(country))
                 df = df.withColumn("Source_Type", lit("FORESEE"))
                 df = df.withColumn("ETL_INSERT_TIME", lit(now))
                 df = df.withColumn("ETL_UPDATE_TIME", lit(""))
                 df = df.withColumn("JOB_ID", lit(job_id))

                 # Derive prev year date
                 df = fiscal_lookup.join(df, fiscal_lookup["date"] ==
                                         df["Date"]).select(
                     df['*'], fiscal_lookup.prev_date.alias("Prev_Date"))
                 # Derive brand ID using brand lookup
                 # df = brand_lookup.join(df, brand_lookup["BRAND"]
                 #                        == df["Brand"]).select(
                 #     df['*'], brand_lookup.BRAND_ID.alias("Brand_ID"))
                 df = df.na.fill("0")
                 df.createOrReplaceTempView("store")
                 brand_lookup.createOrReplaceTempView("brand")
                 df = sq.sql("select store.*, brand.BRAND_ID from store "
            "store cross join brand brand on brand.BRAND = store.Brand")
                 df.createOrReplaceTempView("store")
                 full_load_df = sq.sql("SELECT Date, 0 as Look_and_feel, "
                                       "0 as Navigation, Price, "
                                       "0 as Product_Browsing, "
                                       "0 as Product_Descriptions, "
                                       "0 as Product_Images, "
                                       "0 as Site_Performance, "
                                       "0 as Product_Descriptions_Understandable, "
                                       "0 as Product_Browsing_Narrow, "
                                       "0 as Product_Browsing_Sort, "
                                       "0 as Product_Descriptions_Answers, "
                                       "0 as "
                                       "Product_Descriptions_Thoroughness, "
                                       "0 as Product_Browsing_Features, "
                                       "Satisfaction_Expectations, "
                                       "Satisfaction_Ideal, "
                                       "Satisfaction_Overall, "
                                       "0 as Site_Performance_Consistency, "
                                       "0 as Site_Performance_Errors, "
                                       "0 as Brand_Commitment, "
                                       "0 as Purchase_Products, "
                                       "0 as Purchase_from_Store, "
                                       "0 as Purchase_from_Third_Party, "
                                       "0 as Merchandise, "
                                       "0 as Brand_Preference, Look_and_Feel_Appeal, 0 as "
                                       "Look_and_Feel_Balance, "
                                       "0 as Product_Images_Realistic, "
                                       "0 as Merchandise_Appeal, "
                                       "0 as Merchandise_Availability, "
                                       "0 as Navigation_Layout, "
                                       "0 as Navigation_Options, "
                                       "0 as Navigation_Organized, "
                                       "0 as Product_Images_Views, "
                                       "0 as Site_Performance_Loading, "
                                       "Store_ID, Survey_Type, "
                                       "0 as NPS_Score, 0 as OVSAT_Score, "
                                       "0 as Product_Expectations, "
                                       "0 as Product_Availability, "
                                       "0 as Website_Look_Feel, "
                                       "0 as Website_Usability, "
                                       "0 as Website_Customer_Service, "
                                       "0 as Website_Customer_Service_Friendly, "
                                       "0 as "
                                       "Website_Customer_Service_Knowledgeable, "
                                       "0 as "
                                       "Website_Customer_Service_Understand_Needs, "
                                       "0 as "
                                       "Website_Customer_Service_Avail_to_Assist, "
                                       "0 as Product_Material_Descriptions, "
                                       "0 as Size_finding_tools, "
                                       "0 as Product_Ratings_Reviews, "
                                       "0 as Selection_of_Sizes, "
                                       "0 as Selection_of_Colors, "
                                       "0 as Availability_of_Seasonal_products, "
                                       "0 as Selection_of_Handbags, "
                                       "0 as Selection_of_Backpacks, "
                                       "0 as Selection_of_Luggage, "
                                       "0 as Selection_of_Accessories, "
                                       "0 as Appeal_of_Prod_Presentation, "
                                       "0 as Use_of_Prod_pictures_imagery, "
                                       "0 as "
                                       "Number_of_Clicks_to_get_where_you_want, "
                                       "0 as "
                                       "Experienced_site_performance_issues, "
                                       "0 as Store_Atmosphere, "
                                       "0 as Review_Rating, "
                                       "0 as Review_Bottomline, 0 as Helpful,"
                                       "0 as Not_Helpful, Source_Type, Brand_ID, "
                                       "cast(Prev_Date as Date), "
                                       "ETL_INSERT_TIME, ETL_UPDATE_TIME, "
                                       "JOB_ID from store")

             elif filename.lower().find('mobile') > -1:
                 column_condition1 = mobile_columns_for_condition1
                 column_condition2 = mobile_columns_for_condition2
                 column_condition3 = []

                 for columns in column_condition1:
                     print(columns)
                     df = df.withColumn(columns,
                                        when(col(columns).contains("10"), 10).
                                        when(length(col(columns)) == 1,
                                             0).otherwise(
                                            col(columns).substr(1, 1)))

                 for columns in column_condition2:
                     print(columns)
                     df = df.withColumn(columns,
                                        when(length(col(columns)) > 1,
                                             regexp_extract(col(columns),
                                                            "-(.*)",
                                                            1)).otherwise(
                                            None))

                 if column_condition3:
                     for columns in column_condition3:
                         df = df.withColumn(columns,
                                            when(length(col(columns)) > 1,
                                                 col(columns).substr(1,
                                                                     1)).otherwise(
                                                0))

                 df = df.withColumn("Date", date_format(
                     to_date(col("Date").cast("string"), "MM/dd/yyyy"),
                     "yyyy-MM-dd"))
                 df = df.withColumnRenamed("Look and Feel", "Look_and_Feel")
                 df = df.withColumnRenamed("Product Browsing",
                                           "Product_Browsing")
                 df = df.withColumnRenamed("Site Performance",
                                           "Site_Performance")
                 df = df.withColumnRenamed("Product Browsing - Narrow",
                                           "Product_Browsing_Narrow")
                 df = df.withColumnRenamed("Product Browsing - Sort",
                                           "Product_Browsing_Sort")
                 df = df.withColumnRenamed("Sat - Expectations",
                                           "Satisfaction_Expectations")
                 df = df.withColumnRenamed("Sat - Ideal", "Satisfaction_Ideal")
                 df = df.withColumnRenamed("Sat - Overall",
                                           "Satisfaction_Overall")
                 df = df.withColumnRenamed("Site Performance - Errors",
                                           "Site_Performance_Errors")
                 df = df.withColumnRenamed("Purchase from Store",
                                           "Purchase_from_Store")
                 df = df.withColumnRenamed("Look and Feel - Appeal",
                                           "Look_and_Feel_Appeal")
                 df = df.withColumnRenamed("Merchandise - Appeal",
                                           "Merchandise_Appeal")
                 df = df.withColumnRenamed("Navigation - Options",
                                           "Navigation_Options")
                 df = df.withColumnRenamed("Navigation - Organized",
                                           "Navigation_Organized")
                 df = df.withColumnRenamed("Site Performance - Loading",
                                           "Site_Performance_Loading")
                 df = df.withColumn("Brand", lit(brand))
                 df = df.withColumn("Survey_Type", lit(survey_type))
                 df = df.withColumn("Region", lit(country))
                 df = df.withColumn("Source_Type", lit("FORESEE"))
                 df = df.withColumn("ETL_INSERT_TIME", lit(now))
                 df = df.withColumn("ETL_UPDATE_TIME", lit(""))
                 df = df.withColumn("JOB_ID", lit(job_id))
                 # Derive prev year date
                 df = fiscal_lookup.join(df, fiscal_lookup["date"] ==
                                         df["Date"]).select(
                     df['*'], fiscal_lookup.prev_date.alias("Prev_Date"))
                 # Derive brand ID using brand lookup
                 # df = brand_lookup.join(df, brand_lookup["BRAND"]
                 #                        == df["Brand"]).select(
                 #     df['*'], brand_lookup.BRAND_ID.alias("Brand_ID"))
                 df = df.na.fill("0")
                 df.createOrReplaceTempView("mobile")
                 brand_lookup.createOrReplaceTempView("brand")
                 df = sq.sql("select mobile.*, brand.BRAND_ID from mobile "
                             "mobile cross join brand brand on brand.BRAND = mobile.Brand")
                 df.createOrReplaceTempView("mobile")
                 full_load_df = sq.sql("SELECT Date, Look_and_feel, "
                                       "Navigation,0 as Price, "
                                       "Product_Browsing, "
                                       "0 as Product_Descriptions, "
                                       "0 as Product_Images, "
                                       "Site_Performance, "
                                       "0 as Product_Descriptions_Understandable, "
                                       "Product_Browsing_Narrow, "
                                       "Product_Browsing_Sort, "
                                       "0 as Product_Descriptions_Answers, "
                                       "0 as "
                                       "Product_Descriptions_Thoroughness, "
                                       "0 as Product_Browsing_Features, "
                                       "Satisfaction_Expectations, "
                                       "Satisfaction_Ideal, "
                                       "Satisfaction_Overall, "
                                       "0 as Site_Performance_Consistency, "
                                       "Site_Performance_Errors, "
                                       "0 as Brand_Commitment, "
                                       "0 as Purchase_Products, "
                                       "Purchase_from_Store, "
                                       "0 as Purchase_from_Third_Party, "
                                       "Merchandise, "
                                       "0 as Brand_Preference, Look_and_Feel_Appeal, 0 as "
                                       "Look_and_Feel_Balance, "
                                       "0 as Product_Images_Realistic, "
                                       "Merchandise_Appeal, "
                                       "0 as Merchandise_Availability, "
                                       "0 as Navigation_Layout, "
                                       "Navigation_Options, "
                                       "Navigation_Organized, "
                                       "0 as Product_Images_Views, "
                                       "Site_Performance_Loading, "
                                       "0 as Store_ID, Survey_Type, "
                                       "0 as NPS_Score, 0 as OVSAT_Score, "
                                       "0 as Product_Expectations, "
                                       "0 as Product_Availability, "
                                       "0 as Website_Look_Feel, "
                                       "0 as Website_Usability, "
                                       "0 as Website_Customer_Service, "
                                       "0 as Website_Customer_Service_Friendly, "
                                       "0 as "
                                       "Website_Customer_Service_Knowledgeable, "
                                       "0 as "
                                       "Website_Customer_Service_Understand_Needs, "
                                       "0 as "
                                       "Website_Customer_Service_Avail_to_Assist, "
                                       "0 as Product_Material_Descriptions, "
                                       "0 as Size_finding_tools, "
                                       "0 as Product_Ratings_Reviews, "
                                       "0 as Selection_of_Sizes, "
                                       "0 as Selection_of_Colors, "
                                       "0 as Availability_of_Seasonal_products, "
                                       "0 as Selection_of_Handbags, "
                                       "0 as Selection_of_Backpacks, "
                                       "0 as Selection_of_Luggage, "
                                       "0 as Selection_of_Accessories, "
                                       "0 as Appeal_of_Prod_Presentation, "
                                       "0 as Use_of_Prod_pictures_imagery, "
                                       "0 as "
                                       "Number_of_Clicks_to_get_where_you_want, "
                                       "0 as "
                                       "Experienced_site_performance_issues, "
                                       "0 as Store_Atmosphere, "
                                       "0 as Review_Rating, "
                                       "0 as Review_Bottomline, 0 as Helpful,"
                                       "0 as Not_Helpful, Source_Type, Brand_ID, "
                                       "cast(Prev_Date as Date), "
                                       "ETL_INSERT_TIME, ETL_UPDATE_TIME, "
                                       "JOB_ID from mobile")

             elif filename.lower().find('browse') > -1:
                 column_condition1 = browser_columns_for_condition1
                 column_condition2 = browser_columns_for_condition2
                 column_condition3 = browser_columns_for_condition3

                 for columns in column_condition1:
                     print(columns)
                     df = df.withColumn(columns, when(col(columns).contains("10"), 10).
                                        when(length(col(columns)) == 1, 0).otherwise(col(columns).substr(1, 1)))
                 for columns in column_condition2:
                     print(columns)
                     df = df.withColumn(columns,
                                        when(length(col(columns)) > 1, regexp_extract(col(columns), "-(.*)", 1)).otherwise(
                                            None))
                 if column_condition3:
                     for columns in column_condition3:
                         df = df.withColumn(columns, when(length(col(columns)) > 1, col(columns).substr(1, 1)).otherwise(0))

                 df = df.withColumn("Date", date_format(
                     to_date(col("Date").cast("string"), "MM/dd/yyyy"),
                     "yyyy-MM-dd"))
                 df = df.withColumnRenamed("Look and Feel", "Look_and_Feel")
                 df = df.withColumnRenamed("Product Browsing", "Product_Browsing")
                 df = df.withColumnRenamed("Product Images", "Product_Images")
                 df = df.withColumnRenamed("Site Performance", "Site_Performance")
                 df = df.withColumnRenamed("Product Browsing - Narrow",
                                           "Product_Browsing_Narrow")
                 df = df.withColumnRenamed("Product Browsing - Sort",
                                           "Product_Browsing_Sort")
                 df = df.withColumnRenamed("Satisfaction - Expectations",
                                           "Satisfaction_Expectations")
                 df = df.withColumnRenamed("Satisfaction - Ideal",
                                           "Satisfaction_Ideal")
                 df = df.withColumnRenamed("Satisfaction - Overall",
                                           "Satisfaction_Overall")
                 df = df.withColumnRenamed("Site Performance - Consistency",
                                           "Site_Performance_Consistency")
                 df = df.withColumnRenamed("Site Performance - Errors",
                                           "Site_Performance_Errors")
                 df = df.withColumnRenamed("Brand Preference11",
                                           "Brand_Preference")
                 df = df.withColumnRenamed("Look and Feel - Appeal",
                                           "Look_and_Feel_Appeal")
                 df = df.withColumnRenamed("Look and Feel - Balance",
                                           "Look_and_Feel_Balance")
                 df = df.withColumnRenamed("Product Images - Realistic",
                                           "Product_Images_Realistic")
                 df = df.withColumnRenamed("Merchandise - Appeal",
                                           "Merchandise_Appeal")
                 df = df.withColumnRenamed("Merchandise - Availability",
                                           "Merchandise_Availability")
                 df = df.withColumnRenamed("Navigation - Layout",
                                           "Navigation_Layout")
                 df = df.withColumnRenamed("Navigation - Options",
                                           "Navigation_Options")
                 df = df.withColumnRenamed("Navigation - Organized",
                                           "Navigation_Organized")
                 df = df.withColumnRenamed("Product Images - Views",
                                           "Product_Images_Views")
                 df = df.withColumnRenamed("Site Performance - Loading",
                                           "Site_Performance_Loading")
                 df = df.withColumn("Brand", lit(brand))
                 df = df.withColumn("Survey_Type", lit(survey_type))
                 df = df.withColumn("Survey_Type",
                                    when(F.lower(df["Survey_Type"]) ==
                                         "browse",
                                         "DESKTOP")
                                    .otherwise(df["Survey_Type"]))
                 df = df.withColumn("Region", lit(country))
                 df = df.withColumn("Source_Type", lit("FORESEE"))
                 df = df.withColumn("ETL_INSERT_TIME", lit(now))
                 df = df.withColumn("ETL_UPDATE_TIME", lit(""))
                 df = df.withColumn("JOB_ID", lit(job_id))
                 # Derive prev year date
                 df = fiscal_lookup.join(df, fiscal_lookup["date"] ==
                                         df["Date"]).select(
                     df['*'], fiscal_lookup.prev_date.alias("Prev_Date"))
                 # Derive brand ID using brand lookup
                 # df = brand_lookup.join(df, brand_lookup["BRAND"]
                 #                        == df["Brand"]).select(
                 #     df['*'], brand_lookup.BRAND_ID.alias("Brand_ID"))
                 df = df.na.fill("0")
                 df.createOrReplaceTempView("desktop")
                 brand_lookup.createOrReplaceTempView("brand")
                 df = sq.sql("select desktop.*, brand.BRAND_ID from desktop "
                             "desktop cross join brand brand on brand.BRAND = desktop.Brand")
                 df.createOrReplaceTempView("desktop")
                 full_load_df = sq.sql("SELECT Date, Look_and_feel, "
                                       "Navigation, Price, "
                                       "Product_Browsing, "
                                       "0 as Product_Descriptions, "
                                       "Product_Images, "
                                       "Site_Performance, "
                                       "0 as Product_Descriptions_Understandable, "
                                       "Product_Browsing_Narrow, "
                                       "Product_Browsing_Sort, "
                                       "0 as Product_Descriptions_Answers, "
                                       "0 as "
                                       "Product_Descriptions_Thoroughness, "
                                       "0 as Product_Browsing_Features, "
                                       "Satisfaction_Expectations, "
                                       "Satisfaction_Ideal, "
                                       "Satisfaction_Overall, "
                                       "Site_Performance_Consistency, "
                                       "Site_Performance_Errors, "
                                       "0 as Brand_Commitment, "
                                       "0 as Purchase_Products, "
                                       "0 as Purchase_from_Store, "
                                       "0 as Purchase_from_Third_Party, "
                                       "Merchandise, "
                                       "Brand_Preference, Look_and_Feel_Appeal, Look_and_Feel_Balance, "
                                       "Product_Images_Realistic, "
                                       "Merchandise_Appeal, "
                                       "Merchandise_Availability, "
                                       "Navigation_Layout, "
                                       "Navigation_Options, "
                                       "Navigation_Organized, "
                                       "Product_Images_Views, "
                                       "Site_Performance_Loading, "
                                       "0 as Store_ID, Survey_Type, "
                                       "0 as NPS_Score, 0 as OVSAT_Score, "
                                       "0 as Product_Expectations, "
                                       "0 as Product_Availability, "
                                       "0 as Website_Look_Feel, "
                                       "0 as Website_Usability, "
                                       "0 as Website_Customer_Service, "
                                       "0 as Website_Customer_Service_Friendly, "
                                       "0 as "
                                       "Website_Customer_Service_Knowledgeable, "
                                       "0 as "
                                       "Website_Customer_Service_Understand_Needs, "
                                       "0 as "
                                       "Website_Customer_Service_Avail_to_Assist, "
                                       "0 as Product_Material_Descriptions, "
                                       "0 as Size_finding_tools, "
                                       "0 as Product_Ratings_Reviews, "
                                       "0 as Selection_of_Sizes, "
                                       "0 as Selection_of_Colors, "
                                       "0 as Availability_of_Seasonal_products, "
                                       "0 as Selection_of_Handbags, "
                                       "0 as Selection_of_Backpacks, "
                                       "0 as Selection_of_Luggage, "
                                       "0 as Selection_of_Accessories, "
                                       "0 as Appeal_of_Prod_Presentation, "
                                       "0 as Use_of_Prod_pictures_imagery, "
                                       "0 as "
                                       "Number_of_Clicks_to_get_where_you_want, "
                                       "0 as "
                                       "Experienced_site_performance_issues, "
                                       "0 as Store_Atmosphere, "
                                       "0 as Review_Rating, "
                                       "0 as Review_Bottomline, 0 as Helpful,"
                                       "0 as Not_Helpful, Source_Type, Brand_ID, "
                                       "cast(Prev_Date as Date), "
                                       "ETL_INSERT_TIME, ETL_UPDATE_TIME, "
                                       "JOB_ID from desktop")

             logger.info("Transformed DF Count : {}".format(full_load_df.count()))

         except Exception as error:
              full_load_df = None
              logger.info("Error Occured While processing tr_foresee due to : {}".format(error))
         return full_load_df, date
