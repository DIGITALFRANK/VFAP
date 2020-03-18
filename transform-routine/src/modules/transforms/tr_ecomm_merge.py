import pyspark
from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge
from modules.utils.utils_dataprocessor import utils
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from pyspark.sql import functions as sf
import modules.config.config as config


class tr_ecomm_merge(Dataprocessor_merge):
    def __init__(self, file_pattern, date):
        """Constructor for tr ecomm merge

          Arguments:
          file_pattern {String} -- Pattern of file that needs to be processed.
          Date (String) - Date preset in file which need to be processed
          """
        super(tr_ecomm_merge, self).__init__(file_pattern, date)
        pass

    def transform(self, df):
        logger = self.logger
        params = self.params
        sq = self.spark
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        job_id = utils.get_glue_job_run_id()
        fiscal_delimiter = ','
        master_file_delimiter = ','
        currency_file_delimiter = ','
        common_file_delimiter = ','
        print("##Applying tr_ecomm_merge ##")
        try:

            # Reading the Dataframe from fiscal file and master data file
            lookup_df = self.redshift_table_to_dataframe(self.env_params[
                                                             "fiscal_table"])
            master_df = self.redshift_table_to_dataframe(self.env_params[
                                                             "ecomml_table"])
            brand_adj_df = self.read_from_s3(common_file_delimiter,
                                             self.env_params["refined_bucket"],
                                             self.env_params[
                                                 "brand_adj_mapping"])
            logger.info("Converting the fiscal calender and master data into "
                        "data frames")
            fiscal_calender = lookup_df.withColumn('joined_column',
                                                   when((length(
                                                       lookup_df.fiscalmonth.cast("string")) == 1),
                                                        sf.concat(sf.col(
                                                            'fiscalyear'),
                                                            lit(0), sf.col(
                                                                'fiscalmonth')))
                                                   .otherwise(sf.concat(
                                                       sf.col('fiscalyear'),
                                                       sf.col('fiscalmonth'))))

            fiscal_calender1 = fiscal_calender.withColumn(
                'fiscal_year_quarter', when((lookup_df.fiscalqtr == 'Q1'),
                                            sf.concat(sf.col('fiscalyear'),
                                                      lit(1)))
                    .when((lookup_df.fiscalqtr == 'Q2'), sf.concat(sf.col(
                    'fiscalyear'), lit(2)))
                    .when((lookup_df.fiscalqtr == 'Q3'), sf.concat(sf.col(
                    'fiscalyear'), lit(3)))
                    .when((lookup_df.fiscalqtr == 'Q4'), sf.concat(sf.col(
                    'fiscalyear'), lit(4))))
            
            # Deriving the Brand Adj column from Brand column
            brand_adj_df.createOrReplaceTempView("mapper")
            df.createOrReplaceTempView("merged")
            df_final = sq.sql("select l.*, m.brand_adj as Brand_Adj from "
                              "mapper m, merged l where m.brand = l.BRAND")
            logger.info("Deriving the Brand Adjective Column from Brand")

            # Creating a temporary view of Merged Data frame
            df_final.createOrReplaceTempView("master")

            # Creating a temporary view of Fiscal Calender Data frame
            fiscal_calender1.createOrReplaceTempView("lookup")

            # Creating Data frame with previous year Values
            df_initial = sq.sql(
                "select m.SYSTEM, m.BRAND, m.Brand_Adj, m.REGION, "
                "m.DEVICE_CATEGORY, m.DATE, m.TOTAL_UNITS, m.TOTAL_SALES, "
                "m.TOTAL_ORDERS, m.TOTAL_PAGE_VIEWS, m.TOTAL_VISITORS, "
                "m.TOTAL_PRODUCT_VIEWS, m.TOTAL_CART_ADDITIONS, "
                "m.TOTAL_SALES_EUR, m.TOTAL_SALES_USD, "
                "m.TOTAL_UNIQUE_VISITORS, m.TOTAL_VISITS, l.fiscalmonth, "
                "l.fiscalqtr, l.fiscalyear, "
                "l.prev_date, l.fiscalweek, l.prev_fiscalweek, "
                "l.prev_fiscalmonth, l.prev_fiscalqtr,l.prev_fiscalyear, "
                "l.day_num, l.month_abbr, l.week_abbr, l.joined_column, "
                "l.fiscal_year_quarter, l.day_type from master m left outer "
                "join lookup l on cast(m.DATE as Date) = cast(l.date as "
                "Date)")
            logger.info("Generating the Fiscal values")
            
            # Creating Temporary View of Data frame with previous year
            # fiscal values
            df_initial.createOrReplaceTempView("master1")
            master_df.createOrReplaceTempView("master_final")

            # Creating dataframe with all the Data about previous values
            final_df = sq.sql("select m.SYSTEM, m.BRAND, m.Brand_Adj, "
                              "m.REGION, m.DEVICE_CATEGORY, m.DATE, "
                              "m.fiscalqtr, m.TOTAL_UNITS, m.fiscalyear, "
                              "m.fiscalmonth, m.TOTAL_SALES, m.TOTAL_ORDERS, "
                              "m.TOTAL_PAGE_VIEWS, m.TOTAL_VISITORS, "
                              "m.TOTAL_PRODUCT_VIEWS, m.TOTAL_CART_ADDITIONS, "
                              "m.TOTAL_SALES_EUR, m.TOTAL_SALES_USD, "
                              "m.TOTAL_UNIQUE_VISITORS, m.TOTAL_VISITS, "
                              "m.fiscal_year_quarter, m.joined_column as "
                              "Fiscal_year_month, m.month_abbr, m.fiscalweek, "
                              "m.week_abbr, m.day_type, m.prev_date, "
                              "m.prev_fiscalweek, m.prev_fiscalmonth, "
                              "m.prev_fiscalqtr,m.prev_fiscalyear, m.day_num, "
                              "l.total_units as units_sold, "
                              "l.total_sales as sales, "
                              "l.total_sales_usd as sales_USD, "
                              "l.total_sales_eur as sales_EUR, "
                              "l.total_orders as Orders, l.total_page_views "
                              "as page_views, l.total_visitors as Visitors, "
                              "l.total_product_views as product_views, "
                              "l.total_cart_additions as cart_additions, "
                              "l.total_unique_visitor as "
                              "prev_total_unique_visitor, l.total_visits as "
                              "prev_total_visits from master1 m, "
                              "master_final l where m.prev_date = l.day and "
                              "m.BRAND = l.brand and m.DEVICE_CATEGORY = "
                              "l.device_category and m.SYSTEM = l.system")

            logger.info("Generating the previous year values by looking up "
                        "with Master Data File")
            
            # Adding the Country Column in Final Data File
            final_country = final_df.withColumn('Country', when(
                final_df.BRAND.contains('ICEBREAKER AUS'), lit('AUSTRALIA')).when(
                final_df.BRAND.contains('UK'), lit('UK')).when(
                final_df.BRAND.contains('US'), lit('USA')).when(
                final_df.BRAND.contains('EU'), lit('EUROPE')).when(
                final_df.BRAND.contains('CANADA'), lit('CANADA')).when(
                final_df.BRAND.contains('NZ'), lit('NEW ZEALAND')).when(
                final_df.BRAND.contains('AUS'), lit('AUSTRALIA')).when(
                final_df.BRAND.contains('BRAZIL'), lit('BRAZIL')).when(
                final_df.BRAND.contains('CHINA'), lit('CHINA')).when(
                final_df.BRAND.contains('KOREA'), lit('KOREA')).when(
                final_df.BRAND.contains('ARGENTINA'), lit('ARGENTINA')).when(
                final_df.BRAND.contains('JANSPORT'),lit('USA')).when(
                final_df.BRAND.contains('SMARTWOOL'),lit('USA')).when(
                final_df.BRAND.contains('EAGLE CREEK'),lit('USA')).when(
                final_df.BRAND.contains('WORK AUTHORITY'), lit('CANADA')).when(
                final_df.BRAND.contains('ALTRA'), lit('USA')).when(
                final_df.BRAND.contains('DICKIES'), lit('USA')).
                                           otherwise(lit('EUROPE')))

            # reading the Currency Data frame
            currency_df = self.read_from_s3(currency_file_delimiter,
                                            self.env_params[
                                                "refined_bucket"],
                                            self.env_params["currency_converter_file"])
            logger.info("Reading the Currency exchange value into Data frame")
            
            # Creating Temporary View of the currency and the Master data frame
            final_country.createOrReplaceTempView("final")
            currency_df.createOrReplaceTempView("currency")

            # Looking up with the Currency Data frame to get the currency
            # conversion rates according to region and country
            final_df1 = sq.sql(
                "select m.SYSTEM as system, m.Brand_Adj as brand_adj, "
                "m.BRAND as brand, m.REGION as brand_region, "
                "m.DEVICE_CATEGORY as device_category, m.DATE as day, "
                "m.fiscalqtr as fiscal_qtr, m.fiscalyear, m.month_abbr as "
                "fiscal_month_abbr, m.fiscalmonth as fiscal_month, "
                "m.fiscal_year_quarter as fiscal_year_qtr, "
                "m.Fiscal_year_month as fiscal_year_month, m.fiscalweek, "
                "m.week_abbr as fiscal_week_abbr, m.day_num, m.day_type, "
                "m.TOTAL_UNITS as total_units, m.TOTAL_SALES as total_sales, "
                "m.TOTAL_SALES_USD as total_sales_usd, m.TOTAL_SALES_EUR as "
                "total_sales_eur, m.TOTAL_ORDERS as total_orders, "
                "m.TOTAL_PAGE_VIEWS as total_page_views, m.TOTAL_VISITORS as "
                "total_visitors, m.TOTAL_PRODUCT_VIEWS as "
                "total_product_views, m.TOTAL_CART_ADDITIONS as "
                "total_cart_additions, m.prev_date as prev_yr_date, "
                "m.prev_fiscalyear as prev_fiscal_year, m.prev_fiscalqtr as "
                "prev_fiscal_qtr, m.prev_fiscalmonth as prev_fiscal_month, "
                "m.prev_fiscalweek as prev_fiscal_week, m.units_sold as "
                "prev_total_units, m.sales as prev_total_sales, m.sales_USD "
                "as prev_total_sales_usd, m.sales_EUR as "
                "prev_total_sales_eur, m.Orders as prev_total_orders, "
                "m.page_views as prev_total_page_views, m.Visitors as "
                "prev_total_visitors, m.product_views as "
                "prev_total_product_views,m.cart_additions as "
                "prev_total_cart_additions, m.TOTAL_UNIQUE_VISITORS as "
                "total_unique_visitor, m.TOTAL_VISITS as total_visits, "
                "m.prev_total_unique_visitor, m.prev_total_visits, c.RATE "
                "from final m, currency c where m.REGION = c.REGION and "
                "m.Country = c.COUNTRY")

            final_df_sales_usd = final_df1.withColumn(
                "total_sales_eur", when(final_df1.brand_region == 'EMEA',
                                        final_df1.total_sales).otherwise(0))
            final_df_sales_eur = final_df_sales_usd.withColumn(
                "total_sales_usd", when(final_df1.brand_region == 'EMEA',
                                        final_df1.total_sales_usd)
                    .otherwise(final_df1.total_sales * final_df1.RATE))

            df_prev_week = final_df_sales_eur.withColumn(
                "prev_fiscal_week_abbr", concat(lit("Week -"),
                                                col("prev_fiscal_week")))
            df_insert = df_prev_week.withColumn("ETL_INSERT_TIME", lit(now))
            df_update = df_insert.withColumn("ETL_UPDATE_TIME", lit(""))
            df_jobid = df_update.withColumn("JOB_ID", lit(job_id))
            df_jobid.createOrReplaceTempView("final_to_merge")

            full_load_df = sq.sql("select system, brand_adj, brand, "
                                  "brand_region,device_category, cast(day as Date), "
                                  "fiscal_qtr, fiscalyear, "
                                  "fiscal_month_abbr, fiscal_month, "
                                  "fiscal_year_qtr, fiscal_year_month, "
                                  "fiscalweek, fiscal_week_abbr, day_num, "
                                  "day_type, total_units, total_sales, "
                                  "total_sales_usd, total_sales_eur, "
                                  "total_orders, total_page_views, "
                                  "total_visitors, total_product_views, "
                                  "total_cart_additions, cast(prev_yr_date "
                                  "as Date), prev_fiscal_year, "
                                  "prev_fiscal_qtr, prev_fiscal_month, "
                                  "prev_fiscal_week, prev_fiscal_week_abbr, "
                                  "prev_total_units, prev_total_sales, "
                                  "prev_total_sales_usd, "
                                  "prev_total_sales_eur, prev_total_orders, "
                                  "prev_total_page_views, "
                                  "prev_total_visitors, "
                                  "prev_total_product_views, "
                                  "prev_total_cart_additions, "
                                  "total_unique_visitor, total_visits, "
                                  "prev_total_unique_visitor, "
                                  "prev_total_visits, ETL_INSERT_TIME, "
                                  "ETL_UPDATE_TIME, JOB_ID from "
                                  "final_to_merge")
            full_load_df.show()

        except Exception as error:
            full_load_df = None
            logger.info("Error Occurred While processing tr_ecomm_merge due "
                        "to : {}".format(error))
            raise Exception("{}".format(error))
        return full_load_df
