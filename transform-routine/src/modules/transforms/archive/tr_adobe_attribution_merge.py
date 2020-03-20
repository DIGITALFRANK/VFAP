
from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge
import logging
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql import functions as sf
# import modules.config.config_merge as config



class tr_adobe_attribution_merge(Dataprocessor_merge):
    def __init__(self, filepattern, date):
        """Constructor for tr adobe attribution merge

        Arguments:
           file_name -- name of file which is being passed to base class
        """
        super(tr_adobe_attribution_merge, self).__init__(filepattern, date)
        pass


    def transform(self, df):
         logger = self.logger
         sq = self.spark
         print("##Applying tr_adobe_attribution_merge ##")
         try:
             df.withColumnRenamed('Revenue (Fixed)3', 'Sales_Local')
             df.withColumnRenamed('Revenue (Fixed)4', 'Sales_USD')
             df.withColumnRenamed('Daily Unique Visitors', 'Daily_Unique_Visitor')
             df.show(n=100)
             df.createOrReplaceTempView("final")
             full_load_df = sq.sql("select Date, Type_Of_Attribution, Channel as Channels, Fiscal_Week, Fiscal_Month,"
                                   " Fiscal_QTR, Fiscal_Year, Brand, Country, Orders, Sales_Local,"
                                   " Sales_USD,"
                                   " Visits, weekly_visits, bi_weekly_visits, Bounces, Entries, Units,"
                                   " Daily_Unique_Visitor, Weekly_Unique_Visitor,"
                                   " BI_Weekly_Unique_visitor, Prev_Orders,"
                                   " Prev_Sales_Local,Prev_Sales_USD, Prev_Visits, prev_weekly_visits,"
                                   " prev_bi_weekly_visits, Prev_Bounces, Prev_Entries, Prev_Units,"
                                   " Prev_Daily_Unique_Visitor, Prev_Weekly_Unique_Visitor,"
                                   " Prev_BI_Weekly_Unique_visitor, prev_date from final")
             logging.info("Getting the previous year data")
             print("Printing full load df")
         except Exception as error:
              full_load_df = None
              logger.info("Error Occured While processiong tr adobe attribution merge due to : {}".format(error))
         return full_load_df
