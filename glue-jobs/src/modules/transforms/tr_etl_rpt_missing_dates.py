from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.types import StructField, StructType, StringType
from modules.utils.utils_core import utils
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class tr_etl_rpt_missing_dates(Core_Job):
    def __init__(self):
        """Constructor for tr_etl_rpt_missing_dates

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """

        super(tr_etl_rpt_missing_dates, self).__init__('file_name')
        self.logger.info("inside tr_etl_rpt_missing_dates constructor")

    def transform(self):

        try:
            spark = self.spark
            params = self.params
            sc = self.sc
            logger = self.logger
            logger.info(" Applying tr_etl_rpt_missing_dates ")
            tables_list_to_call = params["tr_params"].keys()
            structype_min_max_date = utils.convert_dynamodb_json_schema_to_struct_type_schema(
                params["schema_min_max_date"], logger)
            structype_missing_date = utils.convert_dynamodb_json_schema_to_struct_type_schema(
                params["schema_missing_date"], logger)
            min_max_date_append_df = spark.createDataFrame(
                [],
                schema=structype_min_max_date
            )
            missing_date_append_df = spark.createDataFrame(
                [],
                schema=structype_missing_date
            )
            min_max_date_empty_df = min_max_date_append_df
            missing_date_empty_df = missing_date_append_df
            logger.info("The list of tables for which the transformation is running {}".format(tables_list_to_call))
            for tbl in tables_list_to_call:
                tbl_nm = params["tr_params"][tbl]["table_name"]
                br_id = params["tr_params"][tbl]["sas_brand_id"]
                col_nm = params["tr_params"][tbl]["col_name"]
                filter_clause = "sas_brand_id == {}".format(br_id)
                logger.info("filter clause value is {}".format(filter_clause))
                df = self.redshift_table_to_dataframe(redshift_table=tbl_nm)
                refined_df = df.filter(filter_clause)
                load_date = col_nm
                max_date_df = (
                    refined_df.agg({load_date: "max"})
                        .withColumnRenamed("max({})".format(load_date), "max_load_date")
                        .collect()
                )
                max_str_ts = max_date_df[0].max_load_date
                if max_str_ts is not None:
                    max_ts = max_str_ts.date()
                    logger.info("max_ts value is {}".format(max_ts))
                    min_date_df = (
                        refined_df.agg({load_date: "min"})
                            .withColumnRenamed("min({})".format(load_date), "min_load_date")
                            .collect()
                    )
                    min_ts = min_date_df[0].min_load_date.date()
                    logger.info("min_ts value is {}".format(min_ts))
                    three_yr_str = (
                            datetime.date(datetime.now()) - timedelta(days=365 * 3)
                    ).strftime("%d-%m-%Y")
                    three_yr = datetime.strptime(three_yr_str, "%d-%m-%Y").date()
                    logger.info("three_yr value is {}".format(three_yr))
                    cnt = refined_df.count()
                    if min_ts is None:
                        min_f = min_ts
                        winner = min_ts
                    else:
                        min_f = max(three_yr, min_ts)
                        winner = max(three_yr, min_ts)
                        logger.info("winner value is {}".format(min_f))
                    dateList = []
                    while min_f < max_ts:
                        dats = (max_ts.strftime("%d-%m-%Y"), min_f.strftime("%d-%m-%Y"))
                        dateList.append(dats)
                        min_f = min_f + timedelta(days=1)
                    min_f = winner
                    logger.info("dataList elements are {}".format(dateList))
                    rdd = sc.parallelize(dateList)
                    schema = StructType(
                        [
                            StructField("to_dt", StringType(), True),
                            StructField("calendar_dt", StringType(), True),
                        ]
                    )
                    calendar_df = spark.createDataFrame(rdd, schema)
                    calendar_df.createOrReplaceTempView("calendar")
                    refined_df.createOrReplaceTempView("input_table")
                    logger.info("Processing report {}".format(params["tr_params"][tbl]["report_nm"]))
                    report_df = spark.sql(
                        """ SELECT '%s' as table_name,
                                                    cast('%s' as int) as sas_brand_id,
                                                    to_date(c.calendar_dt,'dd-MM-yyyy') as calendar_dt,
                                                    count(*) over() as total_missing_dates
                                                FROM calendar c 
                                                    LEFT OUTER JOIN 
                                                    ( SELECT DISTINCT {}  as dt 
                                                        FROM input_table ) em
                                                ON to_date(c.calendar_dt,'dd-MM-yyyy') = to_date(em.dt,'dd-MM-yyyy')
                                                WHERE em.dt IS null 
                                                    AND c.to_dt IS NOT null 
                                            """.format(load_date)
                        % (tbl_nm, br_id)
                    )
                    report_df.createOrReplaceTempView("report_table")
                    logger.info("count of records in report df {}".format(report_df.count()))
                    missing_date_df = report_df.select(
                        "table_name", "sas_brand_id", "calendar_dt"
                    ).withColumn("process_dtm", F.current_timestamp())
                    min_max_date_df = spark.sql(
                        """ SELECT  table_name,
                                                            '%s' as column_checked,
                                                            sas_brand_id,
                                                            cast('%s' as date) as min_f,
                                                            cast('%s' as date) as max_f,
                                                            total_missing_dates,
                                                            cast('%s' as int) as total_cnt
                                                    FROM report_table 
                                                """
                        % (col_nm, min_f, max_ts, cnt)
                    ).withColumn("process_dtm", F.current_timestamp())
                else:
                    logger.info("No records are present for the given brand id")
                    min_max_date_df = min_max_date_empty_df
                    missing_date_df = missing_date_empty_df
                min_max_date_append_df = min_max_date_append_df.union(min_max_date_df)
                missing_date_append_df = missing_date_append_df.union(missing_date_df)
            transformed_df_dict = {
                'min_max_date': min_max_date_append_df,
                'missing_date': missing_date_append_df
            }
        except Exception as error:
            transformed_df_dict = {}
            self.logger.error(
                "Error Occurred While processing tr_etl_rpt_missing_dates due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.TR_ETL_RPT_MISSING_DATES,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing tr_etl_rpt_missing_dates due to : {}".format(
                                     traceback.format_exc()))

        return transformed_df_dict
