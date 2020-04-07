from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge


# import pyspark
# from pyspark.sql.functions import lit
# from pyspark.sql.functions import *
# from pyspark.sql import functions as sf


class tr_adobe_merge(Dataprocessor_merge):
    def __init__(self, file_pattern, date):
        """Constructor for tr adobe ecomm merge

        Arguments:
        file_pattern {String} -- Pattern of file that needs to be processed.
		Date (String) - Date preset in file which need to be processed
        """
        super(tr_adobe_merge, self).__init__(file_pattern, date)
        pass

    def transform(self, df):
        sq = self.spark
        logger = self.logger
        try:
            print("## Applying tr_adobe_merge ##")
            df.createOrReplaceTempView("adobe_merge")
            full_load_df = sq.sql(
                "select SYSTEM, BRAND, REGION, DEVICE_CATEGORY, cast(DATE as "
                "Date), TOTAL_UNITS, TOTAL_SALES, TOTAL_ORDERS, "
                "TOTAL_PAGE_VIEWS, TOTAL_VISITORS, TOTAL_PRODUCT_VIEWS, "
                "TOTAL_CART_ADDITIONS, TOTAL_SALES_EUR, TOTAL_SALES_USD, "
                "TOTAL_UNIQUE_VISITORS, TOTAL_VISITS from adobe_merge")

            logger.info("Merged Adobe Data frame Created")
        except Exception as error:
            full_load_df = None
            logger.info("Error Occured While processing tr_adobe_merge due "
                        "to : {}".format(error))
        return full_load_df
