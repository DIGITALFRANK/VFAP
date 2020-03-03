import datetime
import sys
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from modules.core.core_job import Core_Job
from modules.utils.utils_core import utils
from modules.utils import utils_ses
from modules.constants import constant
from pyspark.sql import functions as F
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from modules.exceptions.CustomAppException import CustomAppError
import traceback


class Reporting_Job(Core_Job):
    """This is a class for the Reporting Job that runs on a weekly basis.
    Methods of this class represents equivalent reporting jobs which produce
    email output in SAS.

    Arguments:
        Core_Job {class} -- Base class for the module
    """

    def __init__(self, file_name):
        """Default Constructor for Reporting_Job class
        """
        super(Reporting_Job, self).__init__(file_name)
        self.file_name = file_name

    def reporting_week_weather_sum(
        self,
        input_weather_table,
        output_weather_table,
        output_weather_table_write_mode,
        output_missing_weeks_table,
        output_missing_weeks_table_write_mode,
        bulk,
        bulk_begin_dt,
    ):
        """
        Parameters:

        input_weather_table: str
        output_weather_table: str
        output_weather_table_write_mode: str
        output_missing_weeks_table: str
        output_missing_weeks_table_write_mode: str
        bulk: str
        bulk_begin_dt: str

        Returns:

        True if success, raises Exception in the event of failure

        Job aggregates weather information for two weeks prior to last Sunday
        and generates summary report"""

        def get_last_sunday_midnight(input_dt):
            """
            Parameters:

            input_dt: datetime.datetime

            Returns:

            datetime.datetime

            This function returns a Python datetime object for the last Sunday at 00:00:00
            """
            last_sunday_midnight_dt = datetime.datetime.combine(
                (
                    input_dt - datetime.timedelta(days=(input_dt.weekday() + 1) % 7)
                ).date(),
                datetime.time(),
            )

            return last_sunday_midnight_dt

        def define_incomp_miss_summary_df(output_table_id, spark_session, log):
            """
            Parameters:

            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            This function registers an empty table following the internally defined schema
            for the incomp_miss_summary job summary table
            """
            incomp_miss_summary_df = spark_session.createDataFrame(
                [],
                schema=StructType(
                    [
                        StructField("dt", DateType(), True),
                        StructField("countlocations", IntegerType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
            )
            incomp_miss_summary_df.createOrReplaceTempView(output_table_id)
            return

        def define_summary_process_df(output_table_id, spark_session, log):
            """
            Parameters:

            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            This function registers an empty table following the internally defined schema
            for the summary_process job summary table
            """
            summary_process_df = spark_session.createDataFrame(
                [],
                schema=StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField("no_records", IntegerType(), True),
                    ]
                ),
            )

            summary_process_df.createOrReplaceTempView(output_table_id)
            return

        def set_aggregation_interval_and_bulk_flag(bulk, bulk_begin_dt, run_dt, log):
            """
            Parameters:

            bulk: str or None
            bulk_begin_dt: datetime.datetime or None
            run_dt: datetime.datetime
            log: logging.Logger

            Returns:

            Tuple[str, int]

            This function produces a where clause string depending on whether bulk == "T" and bulk_being_dt
            is equal to some arbitrary datetime

               x <= dt and dt < y

               or

               dt < x

            It also sets a flag to 1, 2 or 3 depending on which combination of bulk/bulk_being_dt
            have been set
            """
            if bulk == "T":
                if bulk_begin_dt is not None:
                    # Set filter to all dates between bulk_begin_dt to before last Sunday 00:00:00
                    start_point = bulk_begin_dt.strftime("%Y-%m-%d %H:%M:%S")
                    wherestring = "'{0}' <= dt and dt < '{1}'".format(
                        start_point, run_dt.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    bulk_flag = 1
                    log.info("Aggregating two weeks prior to last Sunday")
                else:
                    # Set filter to all dates before last Sunday 00:00:00
                    start_point = "Earliest available record"
                    wherestring = "dt < '{0}'".format(
                        run_dt.strftime("%Y-%m-%d %H:%M:%S")
                    )
                    bulk_flag = 2
                    log.info("Aggregating over entire history up till last Sunday")
            else:
                # Set filter to all dates before last Sunday 00:00:00
                start_point = (
                    run_dt - datetime.timedelta(seconds=86400 * 14)
                ).strftime("%Y-%m-%d %H:%M:%S")
                wherestring = "'{0}' <= dt and dt < '{1}'".format(
                    start_point, run_dt.strftime("%Y-%m-%d %H:%M:%S")
                )
                bulk_flag = 3

            log.info(
                "Timespan for aggregation set from {0} - {1}".format(
                    start_point, run_dt.strftime("%Y-%m-%d %H:%M:%S")
                )
            )

            return wherestring, bulk_flag

        def produce_delta_table_for_proc_append_force(
            base_df, delta_df, spark_session, log
        ):
            """
            Parameters:

            base_id: str,
            delta_id: str,
            spark_session: SparkSession,
            log: logging.Logger

            Returns:

            spark.sql.DataFrame

            This function prepares a delta table to be appended to a base table according to the behaviour
            of the following procedure in SAS:

                proc append force

            This function accepts a delta table and compares the schema to a base table. Any columns which
            are present in the base table but missing from the delta table are added to the delta table
            according to the column data type and column name, and populated with Nulls.

            Any column which is present in the delta table which is not present in the base table is dropped.

            If a column is present in both the delta table and the base table but the data types are not
            common, then the column is dropped from the delta table and repopulated as nulls.
            """
            # Get iterable schemas of base table and delta table
            delta_schema = delta_df.schema
            base_schema = base_df.schema
            # Iterate through fields of delta table
            for delta_field in delta_schema:
                # If field name does not exist in base schema, drop from delta table
                if delta_field.name.lower() not in [
                    base_field.name.lower() for base_field in base_schema
                ]:
                    log.error(
                        "Field - {0} present in delta table is missing from base table".format(
                            delta_field.name
                        )
                    )
                    raise Exception(
                        "Encountered field mismatch between DataFrames - Field - {0} present in delta table is missing from base table".format(
                            delta_field.name
                        )
                    )
                else:
                    # If field name matches a field in the base schema, get the data type of the base field
                    base_field_dataType = list(
                        filter(
                            lambda base_field: base_field.name == delta_field.name,
                            base_schema,
                        )
                    )[0].dataType
                    # If field data type does not match the base field data type, set to Null and cast to base field data type
                    if not delta_field.dataType == base_field_dataType:
                        log.error(
                            "Data type mismatch for field {0}: delta table dtype - {1} base table dtype - {2}".format(
                                delta_field.name,
                                delta_field.dataType,
                                base_field_dataType,
                            )
                        )
                        raise Exception(
                            "Encountered data type mismatch between DataFrames - mismatch for field {0}: delta table dtype - {1} base table dtype - {2}".format(
                                delta_field.name,
                                delta_field.dataType,
                                base_field_dataType,
                            )
                        )
            # Iterate through fields of base table
            for base_field in base_schema:
                # If field name does not match a field in the delta table, add a column of nulls with the base field data type
                if base_field.name.lower() not in [
                    delta_field.name.lower() for delta_field in delta_schema
                ]:
                    log.error("Field not present in delta table: " + base_field.name)
                    raise Exception(
                        "Expected field missing from DataFrame Field not present in delta table: "
                        + base_field.name
                    )

            return delta_df

        def extract_indsn(
            indsn_table_id, where_clause, spark_session, log, test_path=None
        ):
            """
            Parameters:

            indsn_table_id: str,
            where_clause: str,
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger
            test_path: str --- set to some local CSV path for testing, defaults to None for Redshift read

            Returns:

            pyspark.sql.DataFrame

            This function conducts the extraction of the indsn (input table containing weather
            information) and the subsequent filtering of relevant data is conducted according to
            the select clause defined here and the where_clause argument.

            The resulting table is registered with the SparkSQL catalogue as the indsn_table_id
            """
            log.info(
                "Pulling weather data from external table {0}".format(indsn_table_id)
            )
            if test_path is None:
                unfiltered_indsn_df = self.redshift_table_to_dataframe(
                    redshift_table=indsn_table_id
                )
                if unfiltered_indsn_df is None:
                    log.error(
                        "Unable to read table {0} from Redshift".format(indsn_table_id)
                    )
                    raise Exception(
                        "Unable to read table {0} from Redshift".format(indsn_table_id)
                    )
                log.info(
                    "Registering unfiltered external table indsn as {0} in SparkSQL catalogue".format(
                        indsn_table_id
                    )
                )
                unfiltered_indsn_df.createOrReplaceTempView(indsn_table_id)
            else:
                log.error(
                    "Extraction of weather_hist set to test mode. Should be set to read from Reshift"
                )
                raise Exception(
                    "Extraction of weather_hist set to test mode. Should be set to read from Reshift"
                )

            log.info(
                "Filtering table {0} by {1} on selected columns".format(
                    indsn_table_id, where_clause
                )
            )
            try:
                weather_history_filtered_df = spark_session.sql(
                    """
                                                SELECT cast(dt as timestamp) as dt,
                                                       cast(sas_brand_id as string) as sas_brand_id,
                                                       cast(gustmph as double) as gustmph,
                                                       cast(location as string) as location,
                                                       cast(maxtempdegf as double) as maxtemp,
                                                       cast(mintempdegf as double) as mintemp,
                                                       cast(prcpin as double) as prcpin,
                                                       cast(presmb as double) as presmb,
                                                       cast(rhpct as double) as rhpct,
                                                       cast(skycpct as double) as skycpct,
                                                       cast(snowin as double) as snowin,
                                                       cast(wspdmph as double) as wspdmph
                                                FROM {0}
                                                WHERE ({1})""".format(
                        indsn_table_id, where_clause
                    )
                )
            except Exception as e:
                raise Exception(
                    "Unable to filter table {0} by selected columns and where clause {1}: {2}".format(
                        indsn_table_id, where_clause, e
                    )
                )

            return weather_history_filtered_df

        def append_spark_table_to_html_string(
            html_string,
            table_id,
            column_list,
            label_list,
            title1,
            title2,
            title3,
            footnote,
            spark_session,
            log,
        ):
            """
            Parameters:

            html_string: str
            table_id: str
            column_list: List[str]
            label_list: List[str]
            title1: str
            title2: str
            title3: str
            footnote: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            str - html_string extended to include html formatted table

            This function appends the HTML for a small table from PySpark and finishes the message if a footnote is included
            """
            # Build an HTML string specifying line and 4 spaces delimited table headers
            col_string = "\n    ".join(
                ["<th>{0}</th>".format(label) for label in label_list]
            )
            html_string += """
        <p></p>
        <h4>{0}</h4>
        <h4>{1}</h4>
        <h4>{2}</h4>
        <table style="width:50%">
          <tr>
            {3}
          </tr>""".format(
                title1, title2, title3, col_string
            )
            # Get the content of a table from the SparkSQL catalogue
            # WARNING: The table must be small enough to collect to the Driver program
            df = spark_session.sql("select * from {0}".format(table_id)).collect()
            # Iterate through the row objects, adding a new tr HTML element for each row
            for row in df:
                html_string += "\n  <tr>"
                for col in column_list:
                    # Iterate through each field of a given row, adding a new td HTML element for each field
                    html_string += "\n    <td>{0}</td>".format(row[col])
                html_string += "\n  </tr>"
            # Close the table element in the HTML
            html_string += "\n</table>"
            # Add a footnote to the HTML if specified
            if footnote is not None:
                html_string += "\n<p>\n  <text>{0}</text>\n</p>\n</body>\n</html>".format(
                    footnote
                )
            return html_string

        def create_html_report_string_head(log):
            """
            Parameters:

            log: logging.Logger

            Returns:

            str - Beginning of HTML report

            This function produces summary HTML output which displays the content of a summary sized table in PySpark
            """
            # Build a templated HTML string defining a table with titles
            html_string = """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
        table, th, td {
            border: 1px solid black;
            border-collapse: collapse;
        }
        th, td {
          padding: 5px;
        }
        th {
          text-align: left;
        }
        </style>
        </head>
        <body>"""
            return html_string

        def exit_routine(
            bulk_begin_dt,
            bulk_flag,
            flag_email,
            run_dt,
            run_dt_show,
            indsn,
            _INTERNAL_EMAIL_LIST,
            _EMAIL_FROM,
            _LEVEL,
            _HOSTNAME,
            outdsn,
            flag_incom_missing,
            spark_session,
            log,
        ):
            """
            Parameters:

            bulk_begin_dt: datetime.datetime
            bulk_flag: int
            flag_email: int
            run_dt: datetime.datetime
            run_dt_show: str
            indsn: str
            _INTERNAL_EMAIL_LIST: List[str]
            _EMAIL_FROM: str,
            _LEVEL: str,
            _HOSTNAME: str,
            outdsn: str,
            flag_incom_missing: int
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function is intended to send an email containing job summary information and should
            be triggered only at the end of the run
            """
            reporting_dttm = datetime.datetime.now().strftime("%d%b%Y")
            reporting_subject_str = (
                "VFC/"
                + _LEVEL
                + "/"
                + reporting_dttm
                + " - Weekly Weather History Summary."
            )
            if bulk_flag == 1:
                bulk_begin_dt_show = bulk_begin_dt.strftime("%d%b%Y")
                txt_email = "The date range used between {0} and {1} to process information from {2}".format(
                    bulk_begin_dt_show, run_dt_show, indsn
                )
            elif bulk_flag == 2:
                txt_email = "The information processed for the dates before {0} from the table {1}".format(
                    run_dt_show, indsn
                )
            elif bulk_flag == 3:
                two_weeks_bf_show = (run_dt - datetime.timedelta(days=14)).strftime(
                    "%d%b%Y"
                )
                txt_email = "The date range used between {0} and {1} to process information from {2}".format(
                    two_weeks_bf_show, run_dt_show, indsn
                )

            if flag_email == 1:
                txt_email_2 = "did not return any rows"

                log.info(
                    "Sending plain-text email alert that no rows were returned for the window of aggregation"
                )
                utils_ses.send_absent_report_email(
                    job_name=self.file_name,
                    subject=reporting_subject_str,
                    message=txt_email + " " + txt_email_2,
                    log=log,
                )

            # If flag_email is 2, then send email with control table details
            elif flag_email == 2:
                txt_email_2 = "reports the following results for the summarization process to save to {0}.".format(
                    outdsn
                )
                # If flag_incom_missing is 0, then there were no missing weeks, send an HTML email alert with number of rows appended/updated
                if flag_incom_missing == 0:
                    utils_ses.send_report_email(
                        job_name=self.file_name,
                        subject=reporting_subject_str,
                        dataframes=[
                            spark_session.sql(
                                """SELECT no_records AS No_of_Records,
                                                                             type AS Type
                                                                      FROM summary_process"""
                            )
                        ],
                        table_titles=["{0} {1}.".format(txt_email, txt_email_2)],
                        log=log,
                    )
                else:
                    txt_email_missing_w = "Reported missing weeks"
                    utils_ses.send_report_email(
                        job_name=self.file_name,
                        subject=reporting_subject_str,
                        dataframes=[
                            spark_session.sql(
                                """SELECT no_records AS No_of_Records,
                                                                             type AS Type
                                                                      FROM summary_process"""
                            ),
                            spark_session.sql(
                                """SELECT dt AS Week_Date,
                                                                              countlocations as Total_Number_of_Locations,
                                                                              type AS Type_of_Exception
                                                                       FROM incomp_miss_summary"""
                            ),
                        ],
                        table_titles=[
                            "{0} {1}.".format(txt_email, txt_email_2),
                            txt_email_missing_w,
                        ],
                        log=log,
                    )
            return

        def compute_max_weather_metrics_by_date_location_brand(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: SparkSession
            log: logging.Logger

            Returns:

            pyspark.sql.DataFrame

            It yields the maximum of each of the numeric weather metrics, grouped by date, location and
            sas brand id, ordering the result by date, location and sas brand id, respectively.
            """
            log.info(
                "Taking the maximum value for each weather metric, grouped by date, location and sas_brand_id"
            )
            spark_session.sql(
                """
                 CREATE OR REPLACE TEMPORARY VIEW {0} AS
                 SELECT DATE(dt) AS dt,
                        location,
                        sas_brand_id,
                        max(gustmph) as gustmph,
                        max(mintemp)as mintemp,
                        max(maxtemp) as maxtemp,
                        max(prcpin) as prcpin,
                        max(presmb)as presmb,
                        max(rhpct) as rhpct,
                        max(skycpct) as skycpct,
                        max(snowin) as snowin,
                        max(wspdmph) as wspdmph
                FROM {1}
                GROUP BY DATE(dt),
                         location,
                         sas_brand_id
                ORDER BY dt,
                         location,
                         sas_brand_id
                         """.format(
                    output_table_id, input_table_id
                )
            )

            return

        def compute_next_saturday_date_column(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: SparkSession
            log: logging.Logger

            Returns:

            pyspark.sql.DataFrame

            This function transforms the dt date column from the input table to the timestamp for next Saturday at 00:00:00
            """
            log.info(
                "Creating additional column for date of next Saturday from existing DT column"
            )

            spark_session.sql(
                """
                CREATE OR REPLACE TEMPORARY VIEW {0} AS
                SELECT dt AS dt_orig,
                       CASE
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Mon" THEN CAST(DATE_ADD(DATE(dt), 5) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Tue" THEN CAST(DATE_ADD(DATE(dt), 4) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Wed" THEN CAST(DATE_ADD(DATE(dt), 3) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Thu" THEN CAST(DATE_ADD(DATE(dt), 2) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Fri" THEN CAST(DATE_ADD(DATE(dt), 1) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Sat" THEN CAST(DATE_ADD(DATE(dt), 0) AS TIMESTAMP)
                           WHEN DATE_FORMAT(DATE(dt), "E") = "Sun" THEN CAST(DATE_ADD(DATE(dt), 6) AS TIMESTAMP)
                       END AS dt,
                       location,
                       sas_brand_id,
                       gustmph,
                       prcpin,
                       mintemp,
                       maxtemp,
                       presmb,
                       rhpct,
                       skycpct,
                       snowin,
                       wspdmph
                FROM {1}""".format(
                    output_table_id, input_table_id
                )
            )

            return

        def compute_rangetemp_didsnow_didprcp_columns(
            input_table_id, output_table_id, spark_session, log
        ):

            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: SparkSession
            log: logging.Logger

            Returns:

            pyspark.sql.DataFrame

            This function computes the range of temperatures as mintemp subtracted from maxtemp, ignoring
            rows where maxtemp is equal to -999 or mintemp is equal to -999. It also notes whether snow
            or precipitation occured with a 1 or a 0 or a NULL where the respective column is -999.
            """
            log.info("Computing range temp column, snowin and prcpin flag columns")
            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT dt,
                           dt_orig,
                           location,
                           sas_brand_id,
                           gustmph,
                           mintemp,
                           maxtemp,
                           prcpin,
                           presmb,
                           rhpct,
                           skycpct,
                           snowin,
                           wspdmph,
                           CASE WHEN (MAXTEMP != -999 AND MINTEMP != -999) THEN (MAXTEMP - MINTEMP) ELSE NULL END AS rngtemp,
                           CASE WHEN SNOWIN > 0 THEN 1
                                WHEN SNOWIN = 0 THEN 0
                                WHEN SNOWIN = -999 THEN NULL END AS didsnow,
                           CASE WHEN PRCPIN > 0 THEN 1
                                WHEN PRCPIN = 0 THEN 0
                                WHEN PRCPIN = -999 THEN NULL END AS didprcp
                    FROM {1}""".format(
                    output_table_id, input_table_id
                )
            )

            return

        def compute_dt_location_count_left_join_weather_statistics(
            input_table_id, output_table_id, datetodaynum, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            datetodaynum: datetime.datetime
            spark_session: SparkSession
            log: logging.Logger

            Returns:

            pyspark.sql.DataFrame

            The function computes weather statistics for every week for each location and for each brand.

            For every weather metric equal to -999, these values are not included in the computation
            of the metrics ie they don't contribute to the mean values or counts

            It also adds a SAS_PROCESS_DT column which is the current time of processing
            """

            # Create left hand side base to join to
            # ndays behaviour is replicated - 7 if any elements > 0
            #                                 0 if no elements > 0 and at least one element = 0
            #                                NULL if all element are either null or -999
            # n_dt captures count behaviour where all rows are counted independently or null or -999 values

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW n_dt_stats AS
                         SELECT dt,
                                location,
                                max(sas_brand_id) AS sas_brand_id,
                                CAST(COUNT(*) AS INTEGER) AS n_dt,
                                CAST('{0}' AS TIMESTAMP) AS sas_process_dt
                         FROM {1}
                         GROUP BY dt, location""".format(
                    datetodaynum.strftime("%Y-%m-%d %H:%M:%S"), input_table_id
                )
            )

            # Compute statistics for each column

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW mintemp_stats AS
                         SELECT dt,
                                location,
                                cast(count(mintemp) as int) as mintemp_n,
                                min(mintemp) AS mintemp_min,
                                max(mintemp) AS mintemp_max,
                                avg(mintemp) AS mintemp_avg
                         FROM {0}
                         WHERE mintemp > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW maxtemp_stats AS
                         SELECT dt,
                                location,
                                cast(count(maxtemp) as int) as maxtemp_n,
                                min(maxtemp) AS maxtemp_min,
                                max(maxtemp) AS maxtemp_max,
                                avg(maxtemp) AS maxtemp_avg
                         FROM {0}
                         WHERE maxtemp > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW rngtemp_stats AS
                         SELECT dt,
                                location,
                                cast(count(rngtemp) as int) as rngtemp_n,
                                min(rngtemp) AS rngtemp_min,
                                max(rngtemp) AS rngtemp_max,
                                avg(rngtemp) AS rngtemp_avg
                         FROM {0}
                         GROUP BY dt, location""".format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW prcpin_stats AS
                         SELECT dt,
                                location,
                                cast(count(prcpin) as int) as prcpin_n,
                                min(prcpin) AS prcpin_min,
                                max(prcpin) AS prcpin_max,
                                avg(prcpin) AS prcpin_avg,
                                cast(7 * ceil(sum(didprcp) / 7) as int) as prcpin_ndays
                         FROM {0}
                         WHERE prcpin > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW snowin_stats AS
                         SELECT dt,
                                location,
                                cast(count(snowin) as int) as snowin_n,
                                min(snowin) AS snowin_min,
                                max(snowin) AS snowin_max,
                                avg(snowin) AS snowin_avg,
                                cast(7 * ceil(sum(didsnow) / 7) as int) as snowin_ndays
                         FROM {0}
                         WHERE snowin> - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW gustmph_stats AS
                         SELECT dt,
                                location,
                                cast(count(gustmph) as int) as gustmph_n,
                                min(gustmph) AS gustmph_min,
                                max(gustmph) AS gustmph_max,
                                avg(gustmph) AS gustmph_avg
                         FROM {0}
                         WHERE gustmph > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW rhpct_stats AS
                         SELECT dt,
                                location,
                                cast(count(rhpct) as int) AS rhpct_n,
                                min(rhpct) AS rhpct_min,
                                max(rhpct) AS rhpct_max,
                                avg(rhpct) AS rhpct_avg
                         FROM {0}
                         WHERE rhpct > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW skycpct_stats AS
                         SELECT dt,
                                location,
                                cast(count(skycpct) as int) AS skycpct_n,
                                min(skycpct) AS skycpct_min,
                                max(skycpct) AS skycpct_max,
                                avg(skycpct) AS skycpct_avg
                         FROM {0}
                         WHERE skycpct > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW wspdmph_stats AS
                         SELECT dt,
                                location,
                                cast(count(wspdmph) as int) as wspdmph_n,
                                min(wspdmph) AS wspdmph_min,
                                max(wspdmph) AS wspdmph_max,
                                avg(wspdmph) AS wspdmph_avg
                         FROM {0}
                         WHERE wspdmph > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """CREATE OR REPLACE TEMPORARY VIEW presmb_stats AS
                         SELECT dt,
                                location,
                                cast(count(presmb) as int) AS presmb_n,
                                min(presmb) AS presmb_min,
                                max(presmb) AS presmb_max,
                                avg(presmb) AS presmb_avg
                         FROM {0}
                         WHERE presmb > - 999
                         GROUP BY dt, location
                         """.format(
                    input_table_id
                )
            )

            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                       SELECT a.*,
                              b.mintemp_min,
                              b.mintemp_max,
                              b.mintemp_avg,
                              b.mintemp_n,
                              c.maxtemp_min,
                              c.maxtemp_max,
                              c.maxtemp_avg,
                              c.maxtemp_n,
                              d.rngtemp_min,
                              d.rngtemp_max,
                              d.rngtemp_avg,
                              d.rngtemp_n,
                              e.prcpin_min,
                              e.prcpin_max,
                              e.prcpin_avg,
                              e.prcpin_ndays,
                              e.prcpin_n,
                              f.snowin_min,
                              f.snowin_max,
                              f.snowin_avg,
                              f.snowin_ndays,
                              f.snowin_n,
                              g.wspdmph_min,
                              g.wspdmph_max,
                              g.wspdmph_avg,
                              g.wspdmph_n,
                              h.gustmph_min,
                              h.gustmph_max,
                              h.gustmph_avg,
                              h.gustmph_n,
                              i.rhpct_min,
                              i.rhpct_max,
                              i.rhpct_avg,
                              i.rhpct_n,
                              j.skycpct_min,
                              j.skycpct_max,
                              j.skycpct_avg,
                              j.skycpct_n,
                              k.presmb_min,
                              k.presmb_max,
                              k.presmb_avg,
                              k.presmb_n
                        FROM n_dt_stats AS a
                        LEFT JOIN mintemp_stats AS b ON a.dt == b.dt AND a.location == b.location
                        LEFT JOIN maxtemp_stats AS c ON a.dt == c.dt AND a.location == c.location
                        LEFT JOIN rngtemp_stats AS d ON a.dt == d.dt AND a.location == d.location
                        LEFT JOIN prcpin_stats AS e ON a.dt == e.dt AND a.location == e.location
                        LEFT JOIN snowin_stats AS f ON a.dt == f.dt AND a.location == f.location
                        LEFT JOIN wspdmph_stats AS g ON a.dt == g.dt AND a.location == g.location
                        LEFT JOIN gustmph_stats AS h ON a.dt == h.dt AND a.location == h.location
                        LEFT JOIN rhpct_stats AS i ON a.dt == i.dt AND a.location == i.location
                        LEFT JOIN skycpct_stats AS j ON a.dt == j.dt AND a.location == j.location
                        LEFT JOIN presmb_stats AS k ON a.dt == k.dt AND a.location == k.location
                              """.format(
                    output_table_id
                )
            )
            return

        def round_weather_statistics(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: SparkSession
            log: logging.Logger

            Returns:

            pyspark.sql.DataFrame

            This function performs rounding using SQL in-built function round() on selected columns and registers
            the result set as a DataFrame
            """

            log.info("Performing rounding of weather metric descriptive statistics")

            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                SELECT dt,
                       location,
                       sas_brand_id,
                       round(mintemp_min, 1) as mintemp_min,
                       round(mintemp_max, 1) as mintemp_max,
                       round(mintemp_avg, 1) as mintemp_avg,
                       round(maxtemp_min, 1) as maxtemp_min,
                       round(maxtemp_max, 1) as maxtemp_max,
                       round(maxtemp_avg, 1) as maxtemp_avg,
                       round(rngtemp_min, 1) as rngtemp_min,
                       round(rngtemp_max, 1) as rngtemp_max,
                       round(rngtemp_avg, 1) as rngtemp_avg,
                       round(prcpin_min, 2) as prcpin_min,
                       round(prcpin_max, 2) as prcpin_max,
                       round(prcpin_avg, 2) as prcpin_avg,
                       round(snowin_min,1) as snowin_min,
                       round(snowin_max,1) as snowin_max,
                       round(snowin_avg,1) as snowin_avg,
                       round(wspdmph_min,1) as wspdmph_min,
                       round(wspdmph_max,1) as wspdmph_max,
                       round(wspdmph_avg,1) as wspdmph_avg,
                       round(gustmph_min,1) as gustmph_min,
                       round(gustmph_max,1) as gustmph_max,
                       round(gustmph_avg,1) as gustmph_avg,
                       round(rhpct_min,1) as rhpct_min,
                       round(rhpct_max,1) as rhpct_max,
                       round(rhpct_avg,1) as rhpct_avg,
                       round(skycpct_min,1) as skycpct_min,
                       round(skycpct_max,1) as skycpct_max,
                       round(skycpct_avg,1) as skycpct_avg,
                       round(presmb_min,1) as presmb_min,
                       round(presmb_max,1) as presmb_max,
                       round(presmb_avg,1) as presmb_avg,
                       prcpin_ndays,
                       snowin_ndays,
                       n_dt,
                       mintemp_n,
                       maxtemp_n,
                       rngtemp_n,
                       prcpin_n,
                       snowin_n,
                       wspdmph_n,
                       gustmph_n,
                       rhpct_n,
                       skycpct_n,
                       presmb_n,
                       sas_process_dt
                    FROM {1}""".format(
                    output_table_id, input_table_id
                )
            )

            return

        def build_master_append_base_and_register_destination_table(
            external_table_id, output_table_id, spark_session, log, test_path=None
        ):
            """
            Parameters:

            external_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger
            test_path: str

            Returns:

            True if successful, raises Exception if unable to read external table

            This function constructs an empty in-memory SparkSQL table which serves as the base for the master_append
            table. This table will have all weather metric data appended to it before ultimately being loaded in append
            mode to an external table
            """
            log.info(
                "Building base of master_append table for collecting all deduplicated weather data"
            )

            weather_hist_weekly_whole_df = self.redshift_table_to_dataframe(
                redshift_table=external_table_id
            )
            if weather_hist_weekly_whole_df is None:
                raise Exception(
                    "Unable to read from external Redshift table {0} into memory".format(
                        external_table_id
                    )
                )

            log.info(
                "Registering unfiltered external table as {0} in SparkSQL catalogue".format(
                    external_table_id
                )
            )
            weather_hist_weekly_whole_df.createOrReplaceTempView(external_table_id)

            # Create a base table for appending to
            spark_session.sql(
                """
                CREATE OR REPLACE TEMPORARY VIEW {0} AS
                SELECT *
                FROM {1}
                WHERE Location = '1'""".format(
                    output_table_id, external_table_id
                )
            )

            return True

        def left_join_incoming_data_with_existing_data_for_chosen_window(
            incoming_input_table_id,
            existing_input_table_id,
            output_table_id,
            spark_session,
            log,
        ):
            """
            Parameters:

            incoming_input_table_id: str
            existing_input_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function makes a left join of incoming data with existing data for chosen window of aggregation on location/dt/sas_brand_id and enriches
            the successful joins with a duplicate location column labelled as location_orig, otherwise this field will be null
            """

            log.info(
                "left joining incoming weather data with existing weather data for each location/dt/sas_brand_id combination over the window of aggregation"
            )
            spark_session.sql(
                """
                        CREATE OR REPLACE TEMPORARY VIEW {0} AS
                        SELECT a.*,
                               b.Location AS Location_Orig
                        FROM {1} AS a
                        LEFT JOIN {2} AS b
                            ON a.dt = b.dt
                            AND a.location = b.location
                            AND a.sas_brand_id = b.sas_brand_id
                            """.format(
                    output_table_id, incoming_input_table_id, existing_input_table_id
                )
            )

            return

        def append_rows_to_master_append(
            input_delta_table_id,
            input_base_table_id,
            output_table_id,
            spark_session,
            log,
        ):
            """
            Parameters:

            input_delta_table_id: str
            input_base_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function appends weather data to master append table
            """
            master_append_df = spark_session.sql(
                "select * from {0}".format(input_base_table_id)
            )
            records_update_df = spark_session.sql(
                "select * from {0}".format(input_delta_table_id)
            )

            records_update_append_master_append_df = produce_delta_table_for_proc_append_force(
                base_df=master_append_df,
                delta_df=records_update_df,
                spark_session=spark_session,
                log=log,
            )

            master_append_df.unionByName(
                records_update_append_master_append_df
            ).createOrReplaceTempView(output_table_id)

            return

        def update_summary_process_table(
            input_table_id, output_table_id, mode, num_rows, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            mode: str
            num_rows: int
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function updates the summary process table with user specified number of rows and load type
            """

            log.info(
                "Update summary_process table with number of records inserted into master_append table: {0} records in {1} mode".format(
                    num_rows, mode
                )
            )
            spark_session.createDataFrame(
                [[mode, num_rows]],
                schema=StructType(
                    [
                        StructField("type", StringType(), True),
                        StructField("no_records", IntegerType(), True),
                    ]
                ),
            ).createOrReplaceTempView("control_row")
            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT *
                    FROM {1}
                    UNION ALL
                        SELECT *
                        FROM control_row
                    """.format(
                    output_table_id, input_table_id
                )
            )
            return

        def right_join_incoming_data_with_existing_data_for_chosen_window(
            incoming_input_table_id,
            existing_input_table_id,
            output_table_id,
            spark_session,
            log,
        ):
            """
            Parameters:

            incoming_input_table_id: str
            existing_input_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function takes rows of old data from the destination table where the DT/sas_brand_id/location combined field values do not exist in the new data for the
            process window of aggregation
            """
            log.info(
                "Finding rows with dimensions for which weather data previously existed in destination table"
            )
            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT b.*,
                           a.location AS location_calculated
                    FROM {1} AS a
                    RIGHT JOIN {2} AS b
                        ON a.dt = b.dt
                        AND a.location = b.location
                        AND a.sas_brand_id = b.sas_brand_id
                        """.format(
                    output_table_id, incoming_input_table_id, existing_input_table_id
                )
            )
            return

        def compute_consecutive_weeks_by_location_from_master_append(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function adds the following columns to the input table
            Create temp column called prevweek which contains the last sequential DT value, with NULL for first week
            Creates column copy of prevweek as prevweek_2 . Special case for the first week where prevweek_2 is set to DT.
            Creates column containing the number of weeks are between DT and prevweek AS flagweek. Special case for first week where flagweek is set to 0
            """

            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT *
                    FROM {0}
                    ORDER BY location,
                             dt""".format(
                    input_table_id
                )
            )

            # Adds the following columns to master_append
            # Create temp column called prevweek which contains the last sequential DT value, with NULL for first week
            # Creates column copy of prevweek as prevweek_2 . Special case for the first week where prevweek_2 is set to DT.
            # Creates column containing the number of weeks are between DT and prevweek AS flagweek. Special case for first week where flagweek is set to 0

            temp_eval_week_cont_flag_df = spark_session.sql(
                """
                    SELECT *,
                           CASE WHEN prevweek IS NULL THEN dt
                                ELSE prevweek END AS prevweek_2,
                           CASE WHEN prevweek IS NULL THEN 0
                           ELSE CAST((DATEDIFF(DATE(dt), DATE(prevweek)) / 7) AS INTEGER) END AS flagweek
                    FROM (SELECT *,
                                 LAG(dt) OVER (PARTITION BY location ORDER BY dt) AS prevweek
                          FROM {0})""".format(
                    input_table_id
                )
            )

            temp_eval_week_cont_flag_df.drop("prevweek").withColumnRenamed(
                "prevweek_2", "prevweek"
            ).createOrReplaceTempView(output_table_id)

            log.info(
                "Successfully calculated previous week of data by location for each row and included a flagweek to count the number of weeks between dt and the most recent week where data is available for the location"
            )

            return

        def create_datesmissing_table(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function takes a table containing weather metric data for weeks where previous weeks are missing for the given location and produces a table of
            duplicate weather metric data for the missing weeks per location
            """

            distinct_flagweeks = [
                x[0]
                for x in spark_session.sql(
                    "SELECT DISTINCT(flagweek) FROM {0}".format(input_table_id)
                ).collect()
            ]

            # Iterate through each distinctive flagweek value and build sub-view
            for index in range(len(distinct_flagweeks)):
                n = distinct_flagweeks[index]
                qs = """CREATE OR REPLACE TEMPORARY VIEW datesmissing_{0} AS
                            SELECT *,
                                   (flagweek - 1) AS n,
                                   CAST(CONCAT(CAST(DATE_ADD(CAST(prevweek AS DATE), (13 - CAST(from_unixtime(unix_timestamp(prevweek), 'u') AS INTEGER))) AS STRING), 'T23:59:59') AS TIMESTAMP)  AS missingdate
                            FROM {1}
                            WHERE flagweek == {2}""".format(
                    index, input_table_id, n
                )
                for union_index in range(2, n):
                    qs += """ UNION ALL
                                     SELECT *,
                                     (flagweek -  {0}) as n,
                                     CAST(CONCAT(CAST(DATE_ADD(CAST(prevweek AS DATE), ((((1 + {0}) * 7) - 1) - CAST(from_unixtime(unix_timestamp(prevweek), 'u') AS INTEGER))) AS STRING), 'T23:59:59') AS TIMESTAMP)  AS missingdate
                                     FROM {1} WHERE flagweek == {2}""".format(
                        union_index, input_table_id, n
                    )
                spark_session.sql(qs)

            qs = """CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT *
                    FROM datesmissing_0""".format(
                output_table_id
            )

            for index in range(1, len(distinct_flagweeks)):
                qs += """ UNION ALL
                           SELECT * FROM datesmissing_{0}""".format(
                    index
                )
            spark_session.sql(qs)

            return

        def create_missing_week_summary_report_table(
            input_table_id, delta_table_id, base_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            delta_table_id: str
            base_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            int = number of rows in summary data

            This function computes summary information for the datesmissing table by counting the number of unique missing weeks of data from the whole master_append table.
            The resulting summary data is appended to an existing base table designed for this summary data.

            The base table is re-registered under the same name to include the appended summary data
            """
            spark_session.sql(
                """
                    CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT DATE(missingdate) as dt,
                           CAST(COUNT(*) AS INTEGER) AS countlocations,
                           'Missing' as type
                    FROM {1}
                    GROUP BY missingdate""".format(
                    delta_table_id, input_table_id
                )
            )

            missing_summary_df = spark_session.sql(
                "select * from {0}".format(delta_table_id)
            )
            missing_summary_df.persist()

            incomp_miss_summary_df = spark_session.sql(
                "select * from {0}".format(base_table_id)
            )

            missing_summary_append_incomp_miss_summary_df = produce_delta_table_for_proc_append_force(
                base_df=incomp_miss_summary_df,
                delta_df=missing_summary_df,
                spark_session=spark_session,
                log=log,
            )

            incomp_miss_summary_df.unionByName(
                missing_summary_append_incomp_miss_summary_df
            ).createOrReplaceTempView("incomp_miss_summary")

            # number of missing weeks to be recorded in reporting table during exit process
            flag_incom_missing = missing_summary_df.count()

            return flag_incom_missing

        def filter_missing_weeks_by_column(
            input_table_id, output_table_id, spark_session, log
        ):
            """
            Parameters:

            input_table_id: str
            output_table_id: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            Create a filtered dataset of missing weeks data to load to an external table containing key missing weeks data
            """
            log.info(
                "Filtering relevant missing weeks data for pushing to external missing weeks table"
            )

            spark_session.sql(
                """
                CREATE OR REPLACE TEMPORARY VIEW {0} AS
                    SELECT location,
                           sas_brand_id,
                           missingdate as dt,
                           0 as numdays,
                           'Missing' as status,
                           sas_process_dt
                    FROM {1}""".format(
                    output_table_id, input_table_id
                )
            )

            return

        def combine_all_weather_hist_weekly_data(
            registered_external_table_id,
            processed_table_id,
            output_table_id,
            where_clause,
            spark_session,
            log,
        ):
            """
            Parameters:

            registered_external_table_id: str
            processed_table_id: str
            output_table_id: str
            where_clause: str
            spark_session: pyspark.sql.SparkSession
            log: logging.Logger

            Returns:

            None

            This function concatenates the processed weather data from the calling ETL process with the unprocessed data from the external table which
            has not been included in the window of aggregation specified by the where_clause string. Typically the where_clause filters data by some time window of two weeks or more.

            This function is employed as a workaround for updating data in an external table. By pulling the entire table into memory and concatenating it with the newly processed data,
            we can overwrite the external table with a complete in-memory table, thereby accomplishing all necessary DB writes exclusively through the Spark DataFrame writer
            """
            log.info(
                "Concatenating processed weather data {0} with all unprocessed data from external table {1} which does not satisfy the clause {2}".format(
                    processed_table_id, registered_external_table_id, where_clause
                )
            )

            (
                spark_session.sql("select * from {0}".format(processed_table_id))
                .unionByName(
                    spark_session.sql(
                        "select * from {0} where !{1}".format(
                            registered_external_table_id, where_clause
                        )
                    )
                )
                .createOrReplaceTempView(output_table_id)
            )

            return

        def interpret_window_params(bulk, bulk_begin_dt, log):
            """
            Parameters:

            bulk: str
            bulk_begin_dt: str
            log: logging.Logger

            Returns:

            Tuple[Union[str, None], Union[datetime.datetime, None]]

            This function handles string representations of window of aggregation parameters.
            Empty string values are recast as None values, bulk_begin_dt is recast as
            a datetime.datetime object

            bulk -> None or "T"
            bulk_begin_dt -> None or datetime.datetime object
            """
            log.info(
                "Interpreting bulk and bulk_begin_dt flags - establishing time interval for aggregation"
            )
            try:
                if bulk == "Null":
                    bulk = None
                if bulk_begin_dt == "Null":
                    bulk_begin_dt = None
                else:
                    bulk_begin_dt = datetime.datetime.fromisoformat(bulk_begin_dt)
            except Exception as error:
                log.error(
                    "Encountered error while interpreting time interval flags: {0}".format(
                        error
                    )
                )
                raise Exception(
                    "Encountered error while interpreting time interval flags: {0}".format(
                        error
                    )
                )

            return bulk, bulk_begin_dt

        def process(
            input_weather_table,
            output_weather_table,
            output_weather_table_write_mode,
            output_missing_weeks_table,
            output_missing_weeks_table_write_mode,
            bulk,
            bulk_begin_dt,
        ):
            """
            Parameters:

            input_weather_table: str
            output_weather_table: str
            output_weather_table_write_mode: str
            output_missing_weeks_table: str
            output_missing_weeks_table_write_mode: str
            bulk: str
            bulk_begin_dt: str

            Returns:

            True or raises exception in case of failure in reading/writing from/to external sources and/or
            field name/type mismatches

            This function implements the flow of execution for the reporting_week_weather_sum reporting job
            """
            # Configure Spark and log for application
            spark = self.spark
            log = self.logger
            log.info("csv_weather_week_sum ETL program started")

            # Assign Redshift table IDs
            indsn = input_weather_table
            outdsn = output_weather_table
            whistweek_ctrl_dsn = output_missing_weeks_table

            # Handle window aggregation parameters
            bulk, bulk_begin_dt = interpret_window_params(bulk, bulk_begin_dt, log)

            # Temporal parameters

            # timestamp for current time
            datetodaynum = datetime.datetime.now()
            # end of two week data aggregation window - last Sunday 00:00:00
            run_dt = get_last_sunday_midnight(datetime.datetime.now())
            # reader friendly version of end of two week data aggregation window
            run_dt_show = run_dt.strftime("%d%b%Y")

            # Initialize ETL exit parameters

            # Exit function parameters
            bulk_flag = 0
            flag_email = 0
            flag_incom_missing = 0

            # TODO: Email parameters
            _INTERNAL_EMAIL_LIST = ""
            _EMAIL_FROM = ""
            _LEVEL = self.env_params["env_name"]
            _HOSTNAME = ""

            # Instantiate empty table for job summary
            define_summary_process_df(
                output_table_id="summary_process", spark_session=spark, log=log
            )

            spark.sql("select * from summary_process")

            # Instantiate empty table for collecting potential missing weeks of data later
            define_incomp_miss_summary_df(
                output_table_id="incomp_miss_summary", spark_session=spark, log=log
            )

            log.info(
                "Registered empty tables incomp_miss_summary and summary_process to SparkSQL catalogue"
            )

            # Set where clause and bulk flag according to window of aggregation
            wherestring, bulk_flag = set_aggregation_interval_and_bulk_flag(
                bulk=bulk, bulk_begin_dt=bulk_begin_dt, run_dt=run_dt, log=log
            )

            w_hist_extract1_df = extract_indsn(
                indsn_table_id=indsn,
                where_clause=wherestring,
                spark_session=spark,
                log=log,
            )

            # Count number of rows in filtered historical weather data
            w_hist_extract1_df.persist()
            w_hist_extract1_count = w_hist_extract1_df.count()
            w_hist_extract1_df.createOrReplaceTempView("w_hist_extract1")
            log.info(
                "Spark has persisted {0} rows of table {1} in memory".format(
                    w_hist_extract1_count, indsn
                )
            )

            if w_hist_extract1_count == 0:
                log.error(
                    "Table {0} filtered by {1} is empty. Unable to continue".format(
                        indsn, wherestring
                    )
                )
                flag_email = 1
                exit_routine(
                    bulk_begin_dt,
                    bulk_flag,
                    flag_email,
                    run_dt,
                    run_dt_show,
                    indsn,
                    _INTERNAL_EMAIL_LIST,
                    _EMAIL_FROM,
                    _LEVEL,
                    _HOSTNAME,
                    outdsn,
                    flag_incom_missing,
                    spark_session=spark,
                    log=log,
                )

                raise Exception(
                    "Input weather metric table for chosen window of aggregation"
                )

            # Compute summary statistics for the weather metrics by location, week and brand

            compute_max_weather_metrics_by_date_location_brand(
                input_table_id="w_hist_extract1",
                output_table_id="max_weather_metrics",
                spark_session=spark,
                log=log,
            )
            compute_next_saturday_date_column(
                input_table_id="max_weather_metrics",
                output_table_id="w_hist_extract2",
                spark_session=spark,
                log=log,
            )
            spark.sql(
                """
                CREATE OR REPLACE TEMPORARY VIEW w_hist_extract3 AS
                SELECT *
                FROM w_hist_extract2
                ORDER BY location, dt
                """
            )

            # data step 1

            compute_rangetemp_didsnow_didprcp_columns(
                input_table_id="w_hist_extract3",
                output_table_id="w_hist_extract3_expanded",
                spark_session=spark,
                log=log,
            )

            compute_dt_location_count_left_join_weather_statistics(
                input_table_id="w_hist_extract3_expanded",
                output_table_id="w_hist_weekly_1",
                datetodaynum=datetodaynum,
                spark_session=spark,
                log=log,
            )

            round_weather_statistics(
                input_table_id="w_hist_weekly_1",
                output_table_id="w_hist_weekly_2",
                spark_session=spark,
                log=log,
            )

            # Get existing data from destination table which correspond with aggregation window
            status = build_master_append_base_and_register_destination_table(
                external_table_id=outdsn,
                output_table_id="master_append",
                spark_session=spark,
                log=log,
            )

            # Select all records from outdsn table over time aggregation specified by wherestring
            weather_hist_week_orig1_df = spark.sql(
                """
                SELECT *
                FROM {0}
                WHERE {1}""".format(
                    outdsn, wherestring
                )
            )

            weather_hist_week_orig1_df.persist()
            weather_hist_week_orig1_row_count = weather_hist_week_orig1_df.count()
            weather_hist_week_orig1_df.createOrReplaceTempView(
                "weather_hist_week_orig1"
            )

            if weather_hist_week_orig1_row_count > 0:

                # This flow of execution occurs if there exists data in the destination table already for our processes window of aggregation

                flag_email = 2

                left_join_incoming_data_with_existing_data_for_chosen_window(
                    incoming_input_table_id="w_hist_weekly_2",
                    existing_input_table_id="weather_hist_week_orig1",
                    output_table_id="rec_upd_append_all",
                    spark_session=spark,
                    log=log,
                )

                # Filter and count all rows of new data where the DT/sas_brand_id/location already existed in the destination table for the process window of aggregation

                records_update_df = spark.sql(
                    """
                                              SELECT *
                                              FROM rec_upd_append_all
                                              WHERE Location_Orig IS NOT NULL"""
                )
                records_update_df = records_update_df.drop("Location_Orig")
                records_update_df.persist()
                records_update_row_count = records_update_df.count()
                records_update_df.createOrReplaceTempView("records_update")

                if records_update_row_count > 0:
                    log.info(
                        "appending rows where dimensions already exist in external table to master append table"
                    )
                    append_rows_to_master_append(
                        input_delta_table_id="records_update",
                        input_base_table_id="master_append",
                        output_table_id="master_append",
                        spark_session=spark,
                        log=log,
                    )

                update_summary_process_table(
                    input_table_id="summary_process",
                    output_table_id="summary_process",
                    mode="Updated",
                    num_rows=records_update_row_count,
                    spark_session=spark,
                    log=log,
                )

                # Filter all rows of new data where the DT/sas_brand_id/location did not already exist in the destination table for the process window of aggregation

                records_append_df = spark.sql(
                    """
                SELECT *
                FROM rec_upd_append_all
                WHERE Location_Orig IS NULL"""
                )
                records_append_df = records_append_df.drop("Location_Orig")
                records_append_df.createOrReplaceTempView("records_append")
                records_append_df.persist()
                records_append_row_count = records_append_df.count()

                if records_append_row_count > 0:
                    append_rows_to_master_append(
                        input_delta_table_id="records_append",
                        input_base_table_id="master_append",
                        output_table_id="master_append",
                        spark_session=spark,
                        log=log,
                    )

                # Update summary_process table with number of records inserted into master_append table

                update_summary_process_table(
                    input_table_id="summary_process",
                    output_table_id="summary_process",
                    mode="Added",
                    num_rows=records_append_row_count,
                    spark_session=spark,
                    log=log,
                )

                right_join_incoming_data_with_existing_data_for_chosen_window(
                    incoming_input_table_id="w_hist_weekly_2",
                    existing_input_table_id="weather_hist_week_orig1",
                    output_table_id="records_keep1",
                    spark_session=spark,
                    log=log,
                )
                # Append these records to the master output table

                records_keep2_df = spark.sql(
                    """
                        SELECT *
                        FROM records_keep1
                        WHERE location_calculated IS NULL"""
                )
                records_keep2_df = records_keep2_df.drop("location_calculated")
                records_keep2_df.persist()
                records_keep2_row_count = records_keep2_df.count()
                records_keep2_df.createOrReplaceTempView("records_keep2")

                if records_keep2_row_count > 0:
                    append_rows_to_master_append(
                        input_delta_table_id="records_keep2",
                        input_base_table_id="master_append",
                        output_table_id="master_append",
                        spark_session=spark,
                        log=log,
                    )

            else:

                # This flow of execution occurs only if there was no data in the destination table for our processes window of aggregation
                # Append all records to the master output table

                w_hist_weekly_2_df = spark.sql("SELECT * FROM w_hist_weekly_2")
                log.info(
                    "Incoming data exclusively for previously unrecorded SAS_BRAND_ID/location/DT fields"
                )
                append_rows_to_master_append(
                    input_delta_table_id="w_hist_weekly_2",
                    input_base_table_id="master_append",
                    output_table_id="master_append",
                    spark_session=spark,
                    log=log,
                )

                # Update summary_process table with number of records inserted into master_append table

                w_hist_weekly_2_df.persist()
                w_hist_weekly_2_row_count = w_hist_weekly_2_df.count()
                w_hist_weekly_2_df.createOrReplaceTempView("w_hist_weekly_2")

                update_summary_process_table(
                    input_table_id="summary_process",
                    output_table_id="summary_process",
                    mode="Added",
                    num_rows=w_hist_weekly_2_row_count,
                    spark_session=spark,
                    log=log,
                )

                log.info("All records will be appended, no updates required")
                flag_email = 2

            # Evaluate if there are missing weeks
            # This block replaces a key data step

            # record previous week by location for each row and count number of weeks between these for each row

            compute_consecutive_weeks_by_location_from_master_append(
                input_table_id="master_append",
                output_table_id="eval_week_cont_flag",
                spark_session=spark,
                log=log,
            )

            # Select only the rows which are more than 1 week after the previous DT in master_append where flagweek > 1

            not_continuous1_df = spark.sql(
                """
                    SELECT *
                    FROM eval_week_cont_flag
                    WHERE flagweek > 1"""
            )
            not_continuous1_df.persist()
            not_continuous_row_count = not_continuous1_df.count()
            not_continuous1_df.createOrReplaceTempView("not_continuous1")
            # fill in the missing prior weeks with copies of all weather metrics in new table datesmissing with missing_date and missing_flag column added

            if not_continuous_row_count > 0:

                create_datesmissing_table(
                    input_table_id="not_continuous1",
                    output_table_id="datesmissing",
                    spark_session=spark,
                    log=log,
                )

                # summarize dates missing by counting the number of entries grouped by date and add to incomp_miss_summary report table

                flag_incom_missing = create_missing_week_summary_report_table(
                    input_table_id="datesmissing",
                    delta_table_id="missing_summary",
                    base_table_id="incomp_miss_summary",
                    spark_session=spark,
                    log=log,
                )

                # filter missing dates dimension data for every row before appending to external table whistweek_ctrl_dsn_df

                filter_missing_weeks_by_column(
                    input_table_id="datesmissing",
                    output_table_id="missing_toload",
                    spark_session=spark,
                    log=log,
                )

                status = self.write_df_to_redshift_table(
                    df=spark.sql("select * from missing_toload"),
                    redshift_table=whistweek_ctrl_dsn,
                    load_mode=output_missing_weeks_table_write_mode,
                )
                if status is False:
                    raise Exception(
                        "Unable to write missing week data to {0} table in Redshift".format(
                            whistweek_ctrl_dsn
                        )
                    )

            ################################################### Update aggregated weather data in external table #######################################################

            # Pull all weather_hist_weekly weather aggregation into Spark before overwriting it with the updated data

            combine_all_weather_hist_weekly_data(
                registered_external_table_id=outdsn,
                processed_table_id="master_append",
                output_table_id="master_overwrite",
                where_clause=wherestring,
                spark_session=spark,
                log=log,
            )
            master_overwrite_df = spark.sql("select * from master_overwrite")

            # Persist followed by action used to ensure that all data is pulled from destination table before overwriting process starts (Spark dataframe writer truncates destination table first in overwrite mode)
            master_overwrite_df.persist()
            log.info(
                "Writing {0} rows to {1}".format(master_overwrite_df.count(), outdsn)
            )

            status = self.write_df_to_redshift_table(
                df=master_overwrite_df,
                redshift_table=outdsn,
                load_mode=output_weather_table_write_mode,
            )

            exit_routine(
                bulk_begin_dt,
                bulk_flag,
                flag_email,
                run_dt,
                run_dt_show,
                indsn,
                _INTERNAL_EMAIL_LIST,
                _EMAIL_FROM,
                _LEVEL,
                _HOSTNAME,
                outdsn,
                flag_incom_missing,
                spark_session=spark,
                log=log,
            )

            return constant.success

        return process(
            input_weather_table,
            output_weather_table,
            output_weather_table_write_mode,
            output_missing_weeks_table,
            output_missing_weeks_table_write_mode,
            bulk,
            bulk_begin_dt,
        )

    def reporting_etl_rpt_missing_dates(self, load_mode):
        """
        Parameters: load_mode

        Returns:

        True if success, raises Exception in the event of failure
        """
        try:
            def get_missing_dates():

                try:
                    spark = self.spark
                    params = self.params
                    sc = self.sc
                    logger = self.logger
                    logger.info(" Applying tr_etl_rpt_missing_dates ")
                    dbschema = self.whouse_details["dbSchema"]
                    missing_date_tbl = params["tr_params"]["target_tbl"]["missing_date"]
                    min_max_date_tbl = params["tr_params"]["target_tbl"]["min_max_date"]
                    tables_list_to_call = params["tr_params"]["table_list"].keys()
                    logger.info(
                        "The list of tables for which the transformation is running {}".format(
                            tables_list_to_call
                        )
                    )
                    for tbl in tables_list_to_call:
                        tbl_nm = params["tr_params"]["table_list"][tbl]["table_name"]
                        br_id = params["tr_params"]["table_list"][tbl]["sas_brand_id"]
                        col_nm = params["tr_params"]["table_list"][tbl]["col_name"]
                        filter_clause = "sas_brand_id = {}".format(br_id)
                        logger.info("filter clause value is {}".format(filter_clause))
                        load_date = col_nm
                        drop_table_query1 = (
                            """drop table if exists {1}.{0}_report_stage"""
                        ).format(tbl_nm, dbschema)
                        utils.execute_query_in_redshift(drop_table_query1, self.whouse_details, logger)
                        drop_table_query2 = (
                            """drop table if exists {1}.{0}_date_stage"""
                        ).format(tbl_nm, dbschema)
                        utils.execute_query_in_redshift(drop_table_query2, self.whouse_details, logger)
                        create_min_max_table_query = (
                            """create table {3}.{0}_date_stage DISTSTYLE EVEN as select min({1})::date as min_load_date,
                            max({1})::date as max_load_date,count(*) as count from {3}.{0} where {2}"""
                        ).format(tbl_nm, load_date, filter_clause, dbschema)
                        logger.info("generic query to create stage table : {}".format(create_min_max_table_query))
                        utils.execute_query_in_redshift(create_min_max_table_query, self.whouse_details, logger)
                        date_stage_df = self.redshift_table_to_dataframe(redshift_table=tbl_nm + "_date_stage")
                        max_str_ts = date_stage_df.first()["max_load_date"]
                        if max_str_ts is not None:
                            max_ts = max_str_ts
                            logger.info("max_ts value is {}".format(max_ts))
                            min_ts = date_stage_df.first()["min_load_date"]
                            logger.info("min_ts value is {}".format(min_ts))
                            three_yr_str = (
                                    datetime.datetime.date(datetime.datetime.now())
                                    - datetime.timedelta(days=365 * 3)
                            ).strftime("%d-%m-%Y")
                            three_yr = datetime.datetime.strptime(
                                three_yr_str, "%d-%m-%Y"
                            ).date()
                            logger.info("three_yr value is {}".format(three_yr))
                            cnt = date_stage_df.first()["count"]
                            if min_ts is None:
                                min_f = min_ts
                                winner = min_ts
                            else:
                                min_f = max(three_yr, min_ts)
                                winner = max(three_yr, min_ts)
                                logger.info("winner value is {}".format(min_f))
                            dateList = []
                            while min_f < max_ts:
                                dats = (
                                    max_ts.strftime("%d-%m-%Y"),
                                    min_f.strftime("%d-%m-%Y"),
                                )
                                dateList.append(dats)
                                min_f = min_f + datetime.timedelta(days=1)
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
                            self.write_df_to_redshift_table(
                                df=calendar_df,
                                redshift_table="calendar_stage",
                                load_mode="overwrite",
                            )

                            logger.info(
                                "Processing report {}".format(
                                    params["tr_params"]["table_list"][tbl]["report_nm"]
                                )
                            )
                            create_report_table_query = (
                                """ create table {4}.{0}_report_stage as SELECT '{0}' as table_name,
                                                            '{2}' as sas_brand_id,
                                                            to_date(c.calendar_dt,'dd-MM-yyyy') as missing_date,
                                                            getdate() as process_dtm,
                                                            count(*) over() as total_missing_dates
                                                        FROM {4}.calendar_stage c 
                                                            LEFT OUTER JOIN 
                                                            ( SELECT DISTINCT {1}::date  as dt 
                                                                FROM {4}.{0} where {3}) em
                                                        ON to_date(c.calendar_dt,'dd-MM-yyyy') = em.dt
                                                        WHERE em.dt IS null 
                                                            AND c.to_dt IS NOT null 
                                                    """.format(
                                    tbl_nm, load_date, br_id, filter_clause, dbschema
                                )
                            )
                            utils.execute_query_in_redshift(create_report_table_query, self.whouse_details, logger)  ##
                            missing_dates_query = ("""insert into {1}.{2} (select table_name,sas_brand_id,missing_date::timestamp,process_dtm from 
                            {1}.{0}_report_stage)""".format(tbl_nm, dbschema, missing_date_tbl))
                            utils.execute_query_in_redshift(missing_dates_query, self.whouse_details, logger)

                            logger.info("missing date count is {}")
                            min_max_date_query = (
                                    """insert into {1}.{2} 
                                    (select '%s' as table_name,'%s' as column_checked,
                                    '%s' as sas_brand_id,'%s'::timestamp as min_dt,'%s'::timestamp as max_dt,
                                    (select count (*)  from  {1}.{0}_report_stage) as total_missing_dates,
                                    count(*) as total_cnt,getdate() as process_dtm from 
                                    {1}.{0} where {3})""".format(tbl_nm, dbschema, min_max_date_tbl, filter_clause)
                                    % (tbl_nm, col_nm, br_id, min_f, max_ts)
                            )
                            status = utils.execute_query_in_redshift(min_max_date_query, self.whouse_details, logger)
                        else:
                            logger.info("No records are present for the given brand id")  ##
                        drop_table_query3 = (
                            """drop table if exists {1}.{0}_report_stage"""
                        ).format(tbl_nm, dbschema)
                        utils.execute_query_in_redshift(drop_table_query3, self.whouse_details, logger)
                        drop_table_query4 = (
                            """drop table if exists {1}.{0}_date_stage"""
                        ).format(tbl_nm, dbschema)
                        utils.execute_query_in_redshift(drop_table_query4, self.whouse_details, logger)
                except Exception as error:
                    logger.info(
                        "Error Occurred While processing etl_rpt_missing_dates due to : {}".format(error)
                    )
                    raise Exception(
                        "Error Occurred while processing etl_rpt_missing_dates due to: {}".format(
                            error
                        )
                    )
                return status

            def process(load_mode):
                """
                Parameters:load_mode

                Returns:

                True or raises exception in case of failure in reading/writing from/to external sources and/or
                field name/type mismatches

                This function implements the flow of execution for the etl_rpt_missing_dates reporting job
                """
                # Configure Spark and logger for application
                spark = self.spark
                logger = self.logger
                logger.info("rpt_missing_dates ETL program started")
                params = self.params
                get_missing_dates()
                return constant.success
        except Exception as error:
            raise Exception(
                "Error occurred in etl_rpt_missing_dates: {}".format(error)
            )

        return process(load_mode)

    def reporting_csv_build_email_inputs(self, load_mode):
        """
        Parameters: load_mode

        Returns:

        True if success, raises Exception in the event of failure
        """
        try:

            def util_read_etl_parm_table():
                spark = self.spark
                params = self.params
                logger = self.logger
                whouse_etl_parm = self.redshift_table_to_dataframe(
                    redshift_table="etl_params_test"
                )
                logger.info("enter into util_read_etl_parm_table")
                today = datetime.datetime.today()
                calculated_date = today - datetime.timedelta(days=(today.weekday() - 1))
                logger.info("the calculated date is {}".format(calculated_date))
                _brand_name_prefix = params["brand"]
                ##Uncomment this line to run the CSV on a day that is out of the week from where is supposed to run, comment the line above
                # calculated_date = today - datetime.timedelta(days=(today.weekday()+6))
                whouse_etl_parm.createOrReplaceTempView("whouse_etl_parm_view")
                df = spark.sql(
                    """select 
                        case when UPPER(TRIM(CHAR_VALUE))= "CURRENT" then date_format('{}',"yyyy-MM-dd")
                        else date_format(CHAR_VALUE,"yyyy-MM-dd")
                        end as cutoff_date
                    from whouse_etl_parm_view
                    where LOWER(KEY2)="cutoff_date"
                    AND LOWER(KEY1) = LOWER('{}')
                    AND CHAR_VALUE IS NOT NULL
                    AND NUM_VALUE=1""".format(
                        calculated_date, _brand_name_prefix
                    )
                )
                df.show()
                logger.info("end of util_read_etl_parm_table")
                df.createOrReplaceTempView("date_table")

                cutoff_date_df = spark.sql(
                    """select date_format(cutoff_date,"ddMMMyyyy") as cutoff_date from date_table where cutoff_date BETWEEN date_format('2000-01-01',"yyyy-MM-dd") AND date_format(current_date(),"yyyy-MM-dd") AND cutoff_date IS NOT NULL"""
                )
                cutoff_date_df.show()
                cut_off_list = cutoff_date_df.select("cutoff_date").collect()
                _cutoff_date = cut_off_list[0].cutoff_date
                logger.info("the cutoff date is {}".format(_cutoff_date))

                return _cutoff_date

            def run_csv_tnf_build_email_inputs():
                """ Builds email responsys
                """
                transformed_df_dict = {}

                try:
                    transformed_df = None
                    spark = self.spark
                    params = self.params
                    logger = self.logger
                    logger.info("enter into try block")
                    _cutoff_date = util_read_etl_parm_table()

                    whouse_tnf_email_launch_view_df = self.redshift_table_to_dataframe(
                        redshift_table="tnf_launch_view_test"
                    )
                    whouse_tnf_email_sent_view_1df = self.redshift_table_to_dataframe(
                        redshift_table="tnf_sent_view_test"
                    )
                    whouse_tnf_email_open_view_1df = self.redshift_table_to_dataframe(
                        redshift_table="tnf_open_view_test"
                    )
                    whouse_tnf_email_click_view_1df = self.redshift_table_to_dataframe(
                        redshift_table="tnf_click_view_test"
                    )

                    whouse_tnf_email_sent_view_df = (
                        whouse_tnf_email_sent_view_1df.withColumn(
                            "event_captured_dt_con",
                            F.concat(
                                whouse_tnf_email_sent_view_1df[
                                    "event_captured_dt"
                                ].substr(0, 2),
                                F.lit("-"),
                                whouse_tnf_email_sent_view_1df[
                                    "event_captured_dt"
                                ].substr(3, 3),
                                F.lit("-"),
                                whouse_tnf_email_sent_view_1df[
                                    "event_captured_dt"
                                ].substr(6, 4),
                            ),
                        )
                        .drop("event_captured_dt")
                        .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
                    )

                    whouse_tnf_email_open_view_df = (
                        whouse_tnf_email_open_view_1df.withColumn(
                            "event_captured_dt_con",
                            F.concat(
                                whouse_tnf_email_open_view_1df[
                                    "event_captured_dt"
                                ].substr(0, 2),
                                F.lit("-"),
                                whouse_tnf_email_open_view_1df[
                                    "event_captured_dt"
                                ].substr(3, 3),
                                F.lit("-"),
                                whouse_tnf_email_open_view_1df[
                                    "event_captured_dt"
                                ].substr(6, 4),
                            ),
                        )
                        .drop("event_captured_dt")
                        .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
                    )

                    whouse_tnf_email_click_view_df = (
                        whouse_tnf_email_click_view_1df.withColumn(
                            "event_captured_dt_con",
                            F.concat(
                                whouse_tnf_email_click_view_1df[
                                    "event_captured_dt"
                                ].substr(0, 2),
                                F.lit("-"),
                                whouse_tnf_email_click_view_1df[
                                    "event_captured_dt"
                                ].substr(3, 3),
                                F.lit("-"),
                                whouse_tnf_email_click_view_1df[
                                    "event_captured_dt"
                                ].substr(6, 4),
                            ),
                        )
                        .drop("event_captured_dt")
                        .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
                    )

                    logger.info("df's defined")
                    whouse_tnf_email_launch_view_df.createOrReplaceTempView(
                        "whouse_tnf_email_launch_view"
                    )
                    whouse_tnf_email_sent_view_df.createOrReplaceTempView(
                        "whouse_tnf_email_sent_view"
                    )
                    whouse_tnf_email_open_view_df.createOrReplaceTempView(
                        "whouse_tnf_email_open_view"
                    )
                    whouse_tnf_email_click_view_df.createOrReplaceTempView(
                        "whouse_tnf_email_click_view"
                    )

                    tmp_tnf_email_launch_clean_csv_df1 = (
                        spark.sql(
                            """ 
                                            SELECT *,
                                                CASE WHEN UPPER(launch_type) in ('S', 'P', 'R') AND UPPER(launch_status)=='C'
                                                    THEN UPPER(campaign_name) 
                                                    ELSE campaign_name
                                               END AS campaign_name_tmp,
                                               CASE WHEN UPPER(launch_type) in ('S', 'P', 'R') AND UPPER(launch_status)=='C'
                                                    THEN UPPER(subject) 
                                                    ELSE subject
                                               END AS  subject_tmp
                                            FROM whouse_tnf_email_launch_view """
                        )
                        .drop("campaign_name", "subject")
                        .withColumnRenamed("campaign_name_tmp", "CAMPAIGN_NAME")
                        .withColumnRenamed("subject_tmp", "SUBJECT")
                    )

                    tmp_tnf_email_launch_clean_csv_df1.createOrReplaceTempView(
                        "tmp_tnf_email_launch_clean_csv_1"
                    )

                    logger.info(
                        "count of records in tmp_tnf_email_launch_clean_csv_df1 {}".format(
                            tmp_tnf_email_launch_clean_csv_df1.count()
                        )
                    )

                    tmp_tnf_email_launch_clean_csv_df11 = spark.sql(
                        """
                                            SELECT * FROM tmp_tnf_email_launch_clean_csv_1
                                            where INSTR(campaign_name,"UNSUB") <= 0 AND
                                            INSTR(campaign_name,"SHIPPING") <= 0 AND
                                            INSTR(campaign_name,"SHIP_") <= 0 AND
                                            INSTR(campaign_name,"PARTIALSHIP") <= 0 AND
                                            INSTR(campaign_name,"PARTIAL_SHIP") <= 0 AND
                                            INSTR(campaign_name,"REVIEW") <= 0 AND
                                            INSTR(campaign_name,"RETURN") <= 0 AND
                                            INSTR(campaign_name,"EXCHANGE") <= 0 AND
                                            INSTR(campaign_name,"CANCEL") <= 0 AND
                                            INSTR(campaign_name,"CONFIRM") <= 0 AND
                                            INSTR(campaign_name,"PREFERENCECENTER") <= 0 AND
                                            INSTR(campaign_name,"TEST") <= 0 AND
                                            INSTR(campaign_name,"SHIP-ACCOM") <= 0 AND
                                            INSTR(campaign_name,"OUTOFSTOCK") <= 0 AND
                                            INSTR(campaign_name,"USSHIPTOSTORE") <= 0 AND
                                            INSTR(subject,"TEST") <= 0 AND
                                            INSTR(subject,"TRIGGERED") <= 0"""
                    )

                    tmp_tnf_email_launch_clean_csv_df11.createOrReplaceTempView(
                        "tmp_tnf_email_launch_clean_csv_11"
                    )

                    tmp_tnf_email_launch_clean_csv_df2 = (
                        spark.sql(
                            """ SELECT *,
                                    CASE WHEN INSTR(campaign_name,"FALL") > 0 OR 
                                                INSTR(subject,"FALL") > 0 OR 
                                                INSTR(subject,"OCTOBER") > 0 OR
                                                INSTR(subject,"ROCK FEST") > 0 
                                            THEN "F"
                                        WHEN INSTR(campaign_name,"SPRING") > 0 
                                            THEN "P"
                                        WHEN INSTR(campaign_name,"WINTER SALE") > 0 OR
                                                INSTR(campaign_name,"FEB-") > 0 OR 
                                                INSTR(subject,"OMNI_SALE") > 0 
                                            THEN "WS"
                                        WHEN INSTR(campaign_name,"WINTER") > 0 OR  
                                                INSTR(subject,"WINTER") > 0 OR 
                                                INSTR(campaign_name,"NEWYEAR") > 0 OR 
                                                INSTR(subject,"NEW YEAR") > 0 OR 
                                                INSTR(subject,"NEW_YEAR") > 0 
                                            THEN "W"
                                        WHEN INSTR(campaign_name,"SS-") > 0 
                                            THEN "SS"
                                        WHEN INSTR(campaign_name,"HOLIDAY") > 0 OR 
                                                INSTR(campaign_name,"GIFT") > 0 OR 
                                                INSTR(subject,"GIFT") > 0 OR 
                                                INSTR(campaign_name,"DECEMBER") > 0 OR 
                                                INSTR(campaign_name,"BLACKFRIDAY") > 0  OR 
                                                INSTR(subject,"UNDER_100") > 0 OR 
                                                INSTR(subject,"BLACK FRI") > 0  OR 
                                                INSTR(campaign_name,"BLACK-FRI") > 0 OR 
                                                INSTR(campaign_name,"CYBER_MONDAY") > 0 OR 
                                                INSTR(campaign_name,"CYBER-MONDAY") > 0 OR 
                                                INSTR(campaign_name,"CYBERMONDAY") > 0 OR 
                                                INSTR(subject,"CYBER MONDAY") > 0 OR 
                                                INSTR(subject,"CYBER_MONDAY") > 0 OR 
                                                INSTR(subject,"TAX FREE") > 0 
                                            THEN "H"
                                        WHEN INSTR(campaign_name,"BTS") > 0 OR
                                                INSTR(campaign_name,"SUMMER") > 0 OR 
                                                INSTR(subject,"SUMMER") > 0 OR
                                                INSTR(subject,"FATHERSDAY") > 0 OR
                                                INSTR(campaign_name,"FATHERSDAY") > 0 
                                            THEN "B"
                                    ELSE ""
                                    END AS email_ssn,

                                    CASE WHEN INSTR(campaign_name,"GO-VACA") > 0 OR 
                                                INSTR(campaign_name,"_NATL_PARKS") > 0 OR 
                                                INSTR(subject,"EXPLORE IN") > 0 OR 
                                                INSTR(campaign_name,"NATIONALPARK") > 0 OR 
                                                INSTR(campaign_name,"BEST-OF-THE-BAY") > 0 
                                            THEN  "TRAVEL"
                                        WHEN INSTR(campaign_name,"RUN") > 0 OR  
                                                INSTR(campaign_name,"_ECS_") > 0 OR 
                                                INSTR(campaign_name,"GOLIATHON") > 0 OR 
                                                INSTR(subject,"MARATHON") > 0 OR 
                                                INSTR(subject,"RUN") > 0 OR 
                                                INSTR(campaign_name,"OE_FUND") > 0 OR 
                                                INSTR(subject,"ENDURAN") > 0 OR 
                                                INSTR(subject,"LACE UP FOR") > 0 
                                            THEN  "RUN"
                                        WHEN INSTR(campaign_name,"TRAIN") > 0 OR 
                                                INSTR(subject,"GYM") > 0 OR 
                                                INSTR(subject,"EQUIPPED") > 0 OR 
                                                INSTR(campaign_name,"WORKOUT") > 0 OR 
                                                INSTR(subject,"CROSS FIT")  > 0 OR 
                                                INSTR(subject,"XFITMN") > 0 
                                            THEN  "TRN"
                                        WHEN INSTR(campaign_name,"HIK") > 0 OR 
                                                INSTR(subject,"HIK") > 0 OR 
                                                INSTR(subject,"TRAIL") > 0 
                                            THEN  "HIK"
                                        WHEN INSTR(campaign_name,"WATER") > 0 OR 
                                                INSTR(campaign_name,"GO-SF") > 0 
                                            THEN "SURF"
                                        WHEN INSTR(campaign_name,"CLIMB") > 0 OR 
                                                INSTR(subject,"PREPARED FOR THE MOUNTAIN") > 0 OR 
                                                INSTR(campaign_name,"SUMMIT") > 0 OR 
                                                INSTR(campaign_name,"NEPAL")> 0 OR 
                                                INSTR(campaign_name,"MERU") > 0    OR 
                                                INSTR(campaign_name,"BANFF") > 0 OR 
                                                INSTR(campaign_name,"MTN-D-DOW") > 0 OR 
                                                INSTR(campaign_name,"ANGOLA") > 0 OR  
                                                INSTR(campaign_name,"ALPINE") > 0 OR 
                                                INSTR(subject,"ALPINE") > 0 OR 
                                                INSTR(subject,"CLIMB") > 0 OR 
                                                INSTR(subject,"CONRAD ANKER") > 0 OR 
                                                INSTR(subject,"ALEX HONNOLD") > 0 
                                            THEN  "MTNCLM"
                                        WHEN INSTR(campaign_name,"HIPCAMP") > 0 OR 
                                                INSTR(campaign_name,"CAMPING") > 0 OR 
                                                INSTR(campaign_name,"BACKPACK") > 0 OR 
                                                INSTR(subject,"BACKPACK") > 0 OR 
                                                INSTR(subject,"TENT") > 0 OR 
                                                INSTR(subject,"HOMESTEAD") > 0 OR 
                                                INSTR(campaign_name,"HOMESTEAD") > 0 OR 
                                                INSTR(subject,"CAMP") > 0 
                                            THEN  "BCPKCAMP"
                                        WHEN INSTR(campaign_name,"SKI") > 0 OR  
                                                INSTR(campaign_name,"ALL-MTN") > 0 OR 
                                                INSTR(subject,"MEET INGRID") > 0 OR 
                                                INSTR(subject,"DESLAURIERS") > 0 OR 
                                                INSTR(subject,"SKI") > 0 OR 
                                                INSTR(campaign_name,"SNOWSPORTS") > 0 OR 
                                                INSTR(SUBJECT,"SLOPE") > 0 OR
                                                INSTR(SUBJECT,"STEEP") > 0 
                                            THEN  "SKI"
                                        WHEN INSTR(campaign_name,"SNOW") > 0 OR 
                                                INSTR(campaign_name,"SNOWSPORTS") > 0 OR 
                                                INSTR(subject,"KAITLYN FARRINGTON") > 0 
                                            THEN  "SNWB"
                                        WHEN INSTR(campaign_name,"YOGA") > 0 OR 
                                                INSTR(subject,"YOGA") > 0 
                                            THEN  "YOGA"
                                        WHEN INSTR(campaign_name,"BOXING") > 0 OR 
                                                INSTR(SUBJECT,"BOXING") > 0 
                                        THEN  "BOXING"
                                        WHEN INSTR(subject,"HUNT-SEA") > 0 OR 
                                                INSTR(campaign_name,"HUNT-SEA") > 0 
                                        THEN  "WATER"
                                    ELSE ""
                                    END AS email_activity,

                                    CASE WHEN INSTR(campaign_name,"-MEN") > 0 THEN "M"
                                        WHEN INSTR(campaign_name,"-WOMEN") > 0 THEN "F"
                                    ELSE ""
                                    END AS email_gender,

                                    CASE WHEN INSTR(campaign_name,"RETAIL") > 0 OR  
                                                INSTR(subject,"RETAIL") > 0 
                                            THEN  "RETAIL"
                                            WHEN INSTR(campaign_name,"ECOM") > 0 OR 
                                                    INSTR(subject,"ECOM") > 0 OR  
                                                    INSTR(campaign_name,"NEW_SITE") > 0 
                                                THEN "ECOM"					
                                            WHEN INSTR(campaign_name,"OUTLET") > 0 OR  
                                                    INSTR(subject,"OUTLET") > 0  
                                                THEN "OUTLET"
                                    ELSE ""
                                    END AS email_channel,

                                    CASE WHEN  INSTR(campaign_name,"EQUIPMENT") > 0 OR 
                                                INSTR(subject,"EQUIPPED") > 0 OR 
                                                INSTR(subject,"GEAR") > 0 
                                            THEN  "EQUIP"
                                        WHEN INSTR(campaign_name,"JACKET") > 0 OR 
                                                INSTR(subject,"JACKET") > 0 OR 
                                                INSTR(campaign_name,"WATSON") > 0 
                                            THEN  "JKT"
                                        WHEN INSTR(campaign_name,"BOOT") > 0 OR 
                                                INSTR(campaign_name,"XTRAFOAM") > 0 OR 
                                                INSTR(subject,"FOOTWEAR") > 0 
                                            THEN  "FW"
                                        WHEN INSTR(campaign_name,"BACKPACK") > 0 OR 
                                                INSTR(campaign_name,"DAY-PACK") > 0 OR 
                                                INSTR(subject,"DAY-PACK") > 0 OR 
                                                INSTR(subject,"BACKPACK") > 0 
                                            THEN  "BCPK"
                                        WHEN INSTR(campaign_name,"ASCENTIAL") > 0 THEN "ASCNTL"		
                                        WHEN INSTR(campaign_name,"THERM") > 0 OR  
                                                INSTR(subject,"3 WAYS") > 0 OR  
                                                INSTR(subject,"COLD") > 0 OR 
                                                INSTR(campaign_name,"COLD") > 0 OR 
                                                INSTR(campaign_name,"WINTERJACKET") > 0 OR 
                                                INSTR(campaign_name,"DOWN_JACKET") > 0 OR 
                                                INSTR(campaign_name,"SUMMIT") > 0	OR 
                                                INSTR(campaign_name,"_FUSE_CHI_") > 0 OR 
                                                INSTR(campaign_name,"_FUSE_SEATTLE") > 0 OR 
                                                INSTR(campaign_name,"_FUSE_BOSTON_") > 0 OR 
                                                INSTR(campaign_name,"APEX-FLEX") > 0 OR 
                                                INSTR(SUBJECT,"FAR-NORTH") > 0 OR 
                                                INSTR(SUBJECT,"FAR NORTH") > 0 OR 
                                                INSTR(campaign_name,"FARNORTHERN") > 0 OR 
                                                INSTR(campaign_name,"INSULATED") > 0 OR 
                                                INSTR(campaign_name,"URBAN_INS") > 0 OR  
                                                INSTR(campaign_name,"ALPINE") > 0 OR 
                                                INSTR(campaign_name,"_SOFT_") > 0 OR 
                                                INSTR(campaign_name,"URBAN-INS") > 0 OR 
                                                INSTR(campaign_name,"CORE") > 0 OR 
                                                INSTR(campaign_name,"TBALL") > 0 OR 
                                                INSTR(subject,"TBALL") > 0 OR 
                                                INSTR(subject,"THERMOBALL") > 0 OR 
                                                INSTR(campaign_name,"ARCTIC") > 0 OR 
                                                INSTR(subject,"ARCTIC") > 0 OR  
                                                INSTR(subject,"NEW DIMENSION TO WARMTH") > 0 OR  
                                                INSTR(subject,"NEW DIMENSION OF WARMTH") > 0 
                                            THEN "INS"
                                        WHEN INSTR(campaign_name,"FLEECE") > 0 OR 
                                                INSTR(campaign_name,"URBAN_EXP") > 0 OR 
                                                INSTR(campaign_name,"TRICLIM") > 0 OR 
                                                INSTR(campaign_name,"VILLAGEWEAR") > 0 OR 
                                                INSTR(campaign_name,"OSITO") > 0 OR 
                                                INSTR(campaign_name,"WARMTH") > 0 OR 
                                                INSTR(campaign_name,"FAVES") > 0 OR 
                                                INSTR(subject,"FLEECE PONCHO") > 0 OR 
                                                INSTR(subject,"LIGHTER JACKET") > 0 OR 
                                                INSTR(campaign_name,"DENALI") > 0 
                                            THEN  "MILDJKT"
                                        WHEN INSTR(campaign_name,"_FUSEFORM_") > 0 OR 
                                                INSTR(campaign_name,"_VENTURE_") > 0 OR 
                                                (INSTR(campaign_name,"RAIN") > 0 AND INSTR(campaign_name,"TRAIN") <= 0) OR
                                                (INSTR(subject,"RAIN") > 0 AND INSTR(subject,"TRAIN") <= 0) 
                                            THEN "RAIN_WR"
                                        WHEN INSTR(subject,"HAT") > 0 OR 
                                                INSTR(subject,"BEANIE") > 0 OR 
                                                INSTR(subject,"EAR GEAR") > 0 OR 
                                                INSTR(subject,"MITTEN") > 0 OR 
                                                INSTR(subject,"SCARF") > 0 OR 
                                                INSTR(subject,"VISOR") > 0 OR 
                                                INSTR(subject," CAP ") > 0  OR 
                                                INSTR(subject,"GLOVES") > 0   OR 
                                                INSTR(subject,"SOCKS") > 0 OR 
                                                (INSTR(subject,"PACK") > 0 AND INSTR(subject,"BACKPACK") <= 0 ) OR 
                                                INSTR(subject," BAG") > 0 OR 
                                                INSTR(subject,"BOTTLE") > 0 
                                            THEN "ACCSR"			
                                    ELSE ""
                                    END AS Product_category_tmp
                                FROM tmp_tnf_email_launch_clean_csv_11
                                    """
                        )
                        .drop("Product_category")
                        .withColumnRenamed("Product_category_tmp", "Product_category")
                    )

                    tmp_tnf_email_launch_clean_csv_df2.createOrReplaceTempView(
                        "tmp_tnf_email_launch_clean_csv_2"
                    )
                    logger.info(
                        "count of records in tmp_tnf_email_launch_clean_csv_df2 {}".format(
                            tmp_tnf_email_launch_clean_csv_df2.count()
                        )
                    )

                    tmp_tnf_email_launch_clean_csv_df3 = spark.sql(
                        """ SELECT *,
                                            CASE WHEN INSTR(campaign_name,"OUTDOOR") > 0 OR 
                                                INSTR(subject,"OUTDOOR") > 0  OR 
                                                INSTR(campaign_name,"OUTERWEAR") > 0 OR  
                                                INSTR(subject,"EXPLORATION") > 0 OR  
                                                INSTR(subject,"GO OUTSIDE") > 0 OR  
                                                INSTR(subject,"GET OUTSIDE") > 0 OR 
                                                INSTR(subject,"OUTERWEAR") > 0 OR 
                                                INSTR(subject,"SEEFORYOURSELF") > 0 OR 
                                                INSTR(campaign_name,"SEEFORYOURSELF") > 0 
                                            THEN  "OUTDOOR"
                                        WHEN INSTR(campaign_name,"ADVENTUR") > 0 OR 
                                                INSTR(subject,"ADVENTUR") > 0 OR 
                                                INSTR(subject,"SUPERHERO") > 0 OR 
                                                INSTR(subject,"SPEAKER") > 0 OR 
                                                INSTR(subject,"CROWN") > 0 OR 
                                                INSTR(campaign_name,"CROWN") > 0 OR 
                                                INSTR(subject,"ULTIMATE EXPLORATION")  > 0 OR 
                                                INSTR(campaign_name,"FILM")> 0 OR 
                                                INSTR(campaign_name,"VALLEY")> 0 OR 
                                                INSTR(subject,"FACE SPEAK") > 0 OR 
                                                INSTR(subject,"FILM") > 0 OR 
                                                INSTR(subject,"PROGRES") > 0 OR 
                                                INSTR(campaign_name,"-FLIP-") > 0  OR 
                                                INSTR(subject,"MADNESS") > 0 OR 
                                                INSTR(CAMPAIGN_NAME,"EXPLORE-FUND") > 0 OR 
                                                INSTR(subject,"EXPLORE-FUND") > 0 	OR 
                                                INSTR(subject,"EXPLORE FUND") > 0 	OR 
                                                TRIM(email_activity) in ("TRAVEL", "RUN", "HIK", 
                                                "TRAIN", "SURF", "MTNCLM", "BCPK_CAMP", "SKI","SNWB")	OR 
                                                INSTR(subject,"TUNE IN LIVE") > 0 OR 
                                                INSTR(subject,"PREPARED FOR THE MOUNTAIN") > 0	OR 
                                                INSTR(subject,"DESLAURIERS") > 0 OR 
                                                INSTR(campaign_name,"_SS_") > 0 OR 
                                                INSTR(subject,"SS_LIVE") > 0 OR 
                                                INSTR(campaign_name,"SS_LIVE") > 0 
                                            THEN  "PE"
                                        WHEN (INSTR(campaign_name,"MA") > 0 
                                                AND INSTR(campaign_name,"MAIL") <= 0 )	OR 
                                                INSTR(campaign_name,"MTATHLETICS") > 0 OR 
                                                INSTR(subject,"MOUNTAIN ATHLETICS") > 0 
                                            THEN  "MA"
                                        WHEN INSTR(subject,"RECYCLE") > 0 OR 
                                                INSTR(campaign_name,"BACKYARD") > 0 OR 
                                                INSTR(campaign_name,"EARTH_DAY") > 0 OR 
                                                INSTR(subject,"EARTH DAY") > 0
                                            THEN "NL"
                                        WHEN INSTR(campaign_name,"YOUTH") > 0 OR 
                                                INSTR(campaign_name,"KID") > 0 OR 
                                                INSTR(subject,"KID") > 0 OR 
                                                INSTR(campaign_name,"INFANT") > 0 OR 
                                                INSTR(campaign_name,"TODDLER") > 0 
                                            THEN  "FAMILY"
                                        WHEN INSTR(campaign_name,"REWARD") > 0 OR 
                                                INSTR(campaign_name,"SOCHI_PROMO") > 0 OR 
                                                INSTR(subject,"VIPEAK") > 0 OR 
                                                INSTR(subject,"FREE T") > 0 OR 
                                                INSTR(subject,"GET A FREE") > 0 OR 
                                                INSTR(campaign_name,"VIPEAK") > 0 OR 
                                                INSTR(campaign_name,"BONUS") > 0 OR 
                                                INSTR(campaign_name,"VIPEAK_REMINDER") > 0 OR 
                                                INSTR(subject,"EARN MORE POINTS") > 0 OR 
                                                INSTR(subject,"CLAIM YOUR REWARD") > 0 OR 
                                                INSTR(subject,"YOUR VIP") > 0 OR 
                                                INSTR(subject,"VIP TICKET") > 0 
                                            THEN  "VIPRWRD"
                                        WHEN INSTR(campaign_name,"WELCOME EMAIL") > 0 OR 
                                                INSTR(campaign_name,"WELCOME_SIGNUP") > 0 OR 
                                                INSTR(campaign_name,"WELCOMEEMAIL") > 0 OR 
                                                INSTR(campaign_name,"WELCOME_SERIES") > 0 OR 
                                                INSTR(subject,"WELCOME TO") > 0 OR 
                                                INSTR(subject,"THANKS FOR JOINING") > 0 OR  
                                                INSTR(subject,"BEGINNER") > 0 
                                            THEN  "NEW_CUST"
                                        WHEN INSTR(campaign_name,"LOYALTY WELCOME") > 0 OR 
                                                INSTR(campaign_name,"LOYALTYWELCOME") > 0 OR 
                                                INSTR(subject,"PEAKPOINT") > 0 
                                            THEN  "NEW_VIPK"
                                        WHEN INSTR(campaign_name,"ABANDON") > 0 
                                            THEN  "ABNCART"
                                        WHEN INSTR(campaign_name,"WISH LIST") > 0 OR 
                                                INSTR(campaign_name,"WISHLIST") > 0 OR 
                                                INSTR(campaign_name,"WISH_LIST") > 0 
                                            THEN  "WISHLIST"
                                        WHEN INSTR(campaign_name,"BACK IN STOCK") > 0 OR 
                                                INSTR(campaign_name,"BACK_IN_STOCK") > 0 OR 
                                                INSTR(campaign_name,"BACKINSTOCK") > 0 OR 
                                                INSTR(campaign_name,"NEW_ARRIVAL") > 0 OR 
                                                INSTR(subject,"NEW ARRIVAL") > 0 OR 
                                                INSTR(subject,"NEW_ARRIVAL") > 0 OR 
                                                INSTR(subject,"CATALOG") > 0 OR 
                                                INSTR(subject,"BOUNCE_BACK") > 0 OR 
                                                INSTR(campaign_name,"BOUNCE_BACK") > 0 OR  
                                                INSTR(subject,"INVITE") > 0 OR  
                                                INSTR(subject,"CONVIE") > 0 
                                            THEN  "REP_CUST"
                                        WHEN INSTR(campaign_name,"SURVEY") > 0 OR  
                                                INSTR(subject,"SURVEY") > 0 
                                            THEN "SURVEY"
                                    ELSE ""
                                    END AS email_persona
                                FROM tmp_tnf_email_launch_clean_csv_2 """
                    )

                    tmp_tnf_email_launch_clean_csv_df3.createOrReplaceTempView(
                        "tmp_tnf_email_launch_clean_csv"
                    )
                    logger.info(
                        "count of records in tmp_tnf_email_launch_clean_csv_df3 {}".format(
                            tmp_tnf_email_launch_clean_csv_df3.count()
                        )
                    )
                    tmp_tnf_email_launch_clean_csv_df3.show()

                    x_tmp_tnf_email_launch_clean_tmp_df = spark.sql(
                        """ 
                                SELECT sub.* FROM  
                                        ( SELECT *, 
                                            ROW_NUMBER() OVER(PARTITION BY account_id,campaign_id,launch_id,list_id order by account_id) as row_num FROM tmp_tnf_email_launch_clean_csv 
                                        ) sub 
                                WHERE row_num = 1
                            """
                    ).drop("row_num")

                    x_tmp_tnf_email_launch_clean_df = (
                        x_tmp_tnf_email_launch_clean_tmp_df.withColumn(
                            "event_captured_dt_con",
                            F.concat(
                                x_tmp_tnf_email_launch_clean_tmp_df[
                                    "event_captured_dt"
                                ].substr(0, 2),
                                F.lit("-"),
                                x_tmp_tnf_email_launch_clean_tmp_df[
                                    "event_captured_dt"
                                ].substr(3, 3),
                                F.lit("-"),
                                x_tmp_tnf_email_launch_clean_tmp_df[
                                    "event_captured_dt"
                                ].substr(6, 4),
                            ),
                        )
                        .drop("row_num", "event_captured_dt")
                        .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
                    )
                    x_tmp_tnf_email_launch_clean_df.createOrReplaceTempView(
                        "whouse_x_tmp_tnf_email_launch_clean"
                    )
                    logger.info(
                        "count of records in x_tmp_tnf_email_launch_clean_df {}".format(
                            x_tmp_tnf_email_launch_clean_df.count()
                        )
                    )

                    whouse_x_tmp_tnf_email_sent_clean_df = spark.sql(
                        """
                                    SELECT 
                                        distinct
                                        st.campaign_id,
                                        st.launch_id,
                                        st.list_id,
                                        st.riid,
                                        to_date(st.event_captured_dt , "dd-MMM-yyyy") as sent_date,
                                        st.customer_id,
                                        lh.email_gender     AS gen,
                                        lh.email_activity   AS act,
                                        lh.email_ssn        AS ssn,
                                        lh.email_persona    AS prs,
                                        lh.email_channel    AS chnl,
                                        lh.product_category AS pcat
                                    FROM whouse_tnf_email_sent_view st
                                    INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                                    ON st.campaign_id = lh.campaign_id
                                        AND st.launch_id = lh.launch_id
                                        AND st.list_id = lh.list_id
                                    WHERE 
                                        LOWER(TRIM(st.email_ISP))  <> 'vfc.com' 
                                        AND to_date(st.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                                """.format(
                            _cutoff_date
                        )
                    )

                    whouse_x_tmp_tnf_email_sent_clean_df.createOrReplaceTempView(
                        "whouse_x_tmp_tnf_email_sent_clean"
                    )
                    logger.info(
                        "count of records in whouse_x_tmp_tnf_email_sent_clean_df {}".format(
                            whouse_x_tmp_tnf_email_sent_clean_df.count()
                        )
                    )

                    x_tmp_tnf_email_open_clean_df = spark.sql(
                        """
                                    SELECT distinct
                                            op.campaign_id,
                                            op.launch_id,
                                            op.list_id,
                                            op.riid,
                                            MIN (to_date(op.event_captured_dt , 'dd-MMM-yyyy')) AS open_date,
                                            MAX (to_date(op.event_captured_dt , 'dd-MMM-yyyy')) AS most_recent_o
                                        FROM whouse_tnf_email_open_view op
                                        INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                                                ON op.campaign_id = lh.campaign_id
                                                AND op.launch_id  = lh.launch_id
                                                AND op.list_id    = lh.list_id
                                        WHERE 
                                                op.event_captured_dt IS NOT NULL
                                                AND to_date(op.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                                        GROUP BY 
                                                op.campaign_id,
                                                op.launch_id,
                                                op.list_id,
                                                op.riid
                                        """.format(
                            _cutoff_date
                        )
                    )
                    x_tmp_tnf_email_open_clean_df.createOrReplaceTempView(
                        "whouse_x_tmp_tnf_email_open_clean"
                    )
                    logger.info(
                        "count of records in x_tmp_tnf_email_open_clean_df {}".format(
                            x_tmp_tnf_email_open_clean_df.count()
                        )
                    )

                    x_tmp_tnf_email_click_clean_df = spark.sql(
                        """
                                    SELECT 
                                        distinct
                                            cl.campaign_id,
                                            cl.launch_id,
                                            cl.list_id,
                                            cl.riid,
                                            MIN (to_date(cl.event_captured_dt , 'dd-MMM-yyyy')) AS click_date
                                        FROM whouse_tnf_email_click_view cl
                                            INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                                                ON cl.campaign_id = lh.campaign_id
                                                AND cl.launch_id  = lh.launch_id
                                                AND cl.list_id = lh.list_id
                                        WHERE 
                                                LOWER(trim(cl.offer_name)) <> 'unsubscribe_footer'
                                                AND to_date(cl.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                                        GROUP BY cl.campaign_id,
                                                cl.launch_id,
                                                cl.list_id,
                                                cl.riid
                                    """.format(
                            _cutoff_date
                        )
                    )
                    x_tmp_tnf_email_click_clean_df.createOrReplaceTempView(
                        "whouse_x_tmp_tnf_email_click_clean"
                    )
                    logger.info(
                        "count of records in x_tmp_tnf_email_click_clean_df {}".format(
                            x_tmp_tnf_email_click_clean_df.count()
                        )
                    )
                    x_tmp_tnf_email_click_clean_df.show()

                    whouse_x_tmp_tnf_email_inputs_df = spark.sql(
                        """ 
                                    SELECT 	t3.CUSTOMER_ID,
                                        t3.ACT,						
                                        t3.CHNL,					
                                        t3.GEN,		
                                        t3.PCAT,
                                        t3.PRS,						
                                        t3.SENT_DATE,
                                        t3.SSN,
                                        t3.open_date,
                                        t3.days_to_open ,
                                        t3.most_recent_o,
                                        t3.open_ind,
                                        CAST(t4.click_date as date) as click_date,
                                        datediff(CAST(t4.click_date as date),CAST(t3.open_date as date)) AS days_to_click ,
                                        CASE
                                            WHEN t4.click_date IS NOT NULL THEN 1
                                        ELSE 0
                                        END AS click_ind
                                    FROM
                                        (SELECT t1.ACT,
                                            t1.CAMPAIGN_ID,
                                            t1.CHNL,
                                            t1.CUSTOMER_ID,											
                                            t1.GEN,
                                            t1.LAUNCH_ID,
                                            t1.LIST_ID,
                                            t1.PCAT,
                                            t1.PRS,
                                            t1.RIID,
                                            CAST(t1.SENT_DATE as date) as SENT_DATE,
                                            t1.SSN,
                                            CAST(t2.open_date as date) as open_date,
                                            datediff(cast(t2.open_date as date),cast(t1.sent_date as date)) AS days_to_open ,
                                            CAST(t2.most_recent_o as date) as most_recent_o,
                                            CASE
                                                WHEN t2.open_date IS NOT NULL THEN 1
                                            ELSE 0
                                            END AS open_ind
                                        FROM whouse_x_tmp_tnf_email_sent_clean t1
                                        LEFT JOIN whouse_x_tmp_tnf_email_open_clean t2
                                                ON t1.campaign_id = t2.campaign_id
                                                AND t1.list_id = t2.list_id
                                                AND t1.launch_id = t2.launch_id
                                                AND t1.riid = t2.riid 
                                        ) t3
                                    LEFT JOIN whouse_x_tmp_tnf_email_click_clean t4
                                    ON t4.campaign_id = t3.campaign_id
                                        AND t4.list_id = t3.list_id
                                        AND t4.launch_id = t3.launch_id
                                        AND t4.riid = t3.riid 
                                    """
                    )
                    whouse_x_tmp_tnf_email_inputs_df.createOrReplaceTempView(
                        "whouse_x_tmp_tnf_email_inputs"
                    )
                    logger.info(
                        "count number of records in whouse_x_tmp_tnf_email_inputs_df {}".format(
                            whouse_x_tmp_tnf_email_inputs_df.count()
                        )
                    )

                    # cat_list = ["ssn","act"]
                    cat_list = ["ssn", "gen", "act", "prs", "chnl", "pcat"]
                    # loops through all category and var lists to create median days to open, median days to click, #sent, #open, #click, %open and %click
                    for i in cat_list:
                        df_list = spark.sql(
                            """
                                    select tmp.*,
                                    datediff(to_date('{}', 'ddMMMyyyy'), dsince_o_tmp) as dsince_o,
                                        ROUND((freq_o * 100 / freq_s),1) as pct_o,
                                        ROUND((freq_c * 100 / freq_o),1) as pct_c
                                    FROM 
                                        (SELECT  customer_id, %s, 
                                            percentile_approx(days_to_open,0.5) as md2o,
                                            percentile_approx(days_to_click,0.5) as md2c,
                                            max(most_recent_o)as dsince_o_tmp,
                                            count(*) as freq_s,
                                            sum(open_ind) as freq_o,
                                            sum(click_ind) as freq_c					
                                        FROM whouse_x_tmp_tnf_email_inputs
                                        WHERE %s is not null
                                        GROUP BY customer_id, %s ) tmp """.format(
                                _cutoff_date
                            )
                            % (i, i, i)
                        )
                        table_nm = "temp_tnf_" + i + "_metrics"
                        df_list.createOrReplaceTempView(table_nm)

                        logger.info("count is {}".format(df_list.count()))

                        var_list = [
                            "md2o",
                            "md2c",
                            "dsince_o",
                            "freq_s",
                            "freq_o",
                            "freq_c",
                            "pct_o",
                            "pct_c",
                        ]
                        for j in var_list:
                            pivotDF = df_list.groupBy(["customer_id"]).pivot(i).max(j)
                            df_pivot = pivotDF.select(
                                [
                                    F.col(c).alias(i + "_" + j + "_" + c)
                                    for c in pivotDF.columns
                                ]
                            ).withColumnRenamed(
                                i + "_" + j + "_" + "customer_id", "customer_id"
                            )
                            tbl_nm = "temp_" + i + "_csv_" + j
                            df_pivot.createOrReplaceTempView(tbl_nm)
                            df_pivot.show()
                            logger.info(tbl_nm)
                        df_list.show()
                    logger.info("entering inner join")
                    tmp_csv_tnf_email_inputs_df = (
                        spark.sql(
                            """select temp_ssn_csv_md2o.customer_id as customer_id_temp,* from temp_ssn_csv_md2o 
                    inner join temp_ssn_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_md2c.customer_id
                    inner join temp_ssn_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_dsince_o.customer_id
                    inner join temp_ssn_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_s.customer_id
                    inner join temp_ssn_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_o.customer_id
                    inner join temp_ssn_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_c.customer_id
                    inner join temp_ssn_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_pct_o.customer_id
                    inner join temp_ssn_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_pct_c.customer_id
                    inner join temp_gen_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_md2o.customer_id
                    inner join temp_gen_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_md2c.customer_id
                    inner join temp_gen_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_dsince_o.customer_id
                    inner join temp_gen_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_s.customer_id
                    inner join temp_gen_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_o.customer_id
                    inner join temp_gen_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_c.customer_id
                    inner join temp_gen_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_pct_o.customer_id
                    inner join temp_gen_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_pct_c.customer_id
                    inner join temp_act_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_md2o.customer_id
                    inner join temp_act_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_md2c.customer_id
                    inner join temp_act_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_dsince_o.customer_id
                    inner join temp_act_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_s.customer_id
                    inner join temp_act_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_o.customer_id
                    inner join temp_act_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_c.customer_id
                    inner join temp_act_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_pct_o.customer_id
                    inner join temp_act_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_pct_c.customer_id
                    inner join temp_prs_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_md2o.customer_id
                    inner join temp_prs_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_md2c.customer_id
                    inner join temp_prs_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_dsince_o.customer_id
                    inner join temp_prs_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_s.customer_id
                    inner join temp_prs_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_o.customer_id
                    inner join temp_prs_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_c.customer_id
                    inner join temp_prs_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_pct_o.customer_id
                    inner join temp_prs_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_pct_c.customer_id
                    inner join temp_chnl_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_md2o.customer_id
                    inner join temp_chnl_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_md2c.customer_id
                    inner join temp_chnl_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_dsince_o.customer_id
                    inner join temp_chnl_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_s.customer_id
                    inner join temp_chnl_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_o.customer_id
                    inner join temp_chnl_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_c.customer_id
                    inner join temp_chnl_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_pct_o.customer_id
                    inner join temp_chnl_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_pct_c.customer_id
                    inner join temp_pcat_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_md2o.customer_id
                    inner join temp_pcat_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_md2c.customer_id
                    inner join temp_pcat_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_dsince_o.customer_id
                    inner join temp_pcat_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_s.customer_id
                    inner join temp_pcat_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_o.customer_id
                    inner join temp_pcat_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_c.customer_id
                    inner join temp_pcat_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_pct_o.customer_id
                    inner join temp_pcat_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_pct_c.customer_id"""
                        )
                        .drop("customer_id")
                        .withColumnRenamed("customer_id_temp", "customer_id")
                    )
                    logger.info("exiting join")
                    tmp_csv_tnf_email_inputs_df.show()
                    tmp_csv_tnf_email_inputs_df.createOrReplaceTempView(
                        "tmp_csv_tnf_email_inputs"
                    )
                    tmp_csv_tnf_email_inputs_df.cache()
                    #                    csv_tnf_email_inputs_df = (
                    #                        spark.sql(
                    #                            """SELECT *,
                    #                                    CASE WHEN act_dsince_o_WATER is null AND act_dsince_o_SURF > 0
                    #                                        THEN act_dsince_o_SURF
                    #                                        ELSE act_dsince_o_WATER
                    #                                    END AS act_dsince_o_WATER_tmp,
                    #                                    CASE WHEN act_pct_o_WATER is null  AND act_pct_o_SURF > 0
                    #                                        THEN act_pct_o_SURF
                    #                                        ELSE act_pct_o_WATER
                    #                                    END AS act_pct_o_WATER_tmp
                    #                                FROM     tmp_csv_tnf_email_inputs """
                    #                        )
                    #                        .drop(act_dsince_o_WATER, act_pct_o_WATER)
                    #                        .withColumnRenamed("act_dsince_o_WATER_tmp", "act_dsince_o_WATER")
                    #                        .withColumnRenamed("act_pct_o_WATER_tmp", "act_pct_o_WATER")
                    #                    )
                    #                    csv_tnf_email_inputs_df.show()
                    transformed_df = tmp_csv_tnf_email_inputs_df
                    transformed_df_dict = {params["tgt_dstn_tbl_name"]: transformed_df}
                except Exception as error:
                    transformed_df = None
                    transformed_df_dict = {}
                    logger.info(
                        "Error Occurred while processing run_csv_tnf_build_email_inputs due to : {}".format(
                            error
                        )
                    )
                    raise Exception(
                        "Error Occurred while processing run_csv_tnf_build_email_inputs due to: {}".format(
                            error
                        )
                    )
                return transformed_df_dict

            def process(load_mode):
                """
                Parameters:load_mode

                Returns:

                True or raises exception in case of failure in reading/writing from/to external sources and/or
                field name/type mismatches

                This function implements the flow of execution for the run_csv_tnf_build_email_inputs reporting job
                """
                # Configure Spark and logger for application

                spark = self.spark
                logger = self.logger
                logger.info("reporting csv build email inputs program started")
                params = self.params

                transformed_df_data_dict = run_csv_tnf_build_email_inputs()
                #            if len(transformed_df_data_dict) == 0:
                #                transformed_df_to_redshift_table_status = False
                #            else:
                status = True
                for (target_table, transformed_df) in transformed_df_data_dict.items():
                    if status:
                        logger.info(
                            "Inside datadict loop writing transformed_df to : {}".format(
                                target_table
                            )
                        )
                        status = self.write_df_to_redshift_table(
                            df=transformed_df,
                            redshift_table=target_table,
                            load_mode=load_mode,
                        )
                        logger.info(
                            "Response from writing to redshift is {}".format(status)
                        )

                    else:
                        status = False
                        logger.info("Failed to Load Transformed Data Dict To Redshift")

                logger.info("Response from writing to redshift is {}".format(status))
                if status == False:
                    raise Exception("Unable to write the data to the table in Redshift")
                return constant.success

        except Exception as error:
            raise Exception(
                "Error occurred in reporting_csv_build_email_inputs: {}".format(error)
            )

        return process(load_mode)

    def reporting_send_daily_etl_job_status_report(
        self,
        glue_db,
        etl_status_table,
        etl_status_job_column_id,
        etl_status_dttm_column_id,
        etl_status_job_status_column_id,
        etl_status_record_count_column_id,
        etl_parameter_table,
        etl_parameter_job_column_id,
        etl_parameter_target_column_id,
        redshift_output_table,
        redshift_load_mode,
    ):

        """
        Parameters:

        glue_db: str
        etl_status_table: str
        etl_status_job_column_id: str
        etl_status_dttm_column_id: str
        etl_status_job_status_column_id: str
        etl_status_record_count_column_id: str
        etl_parameter_table: str
        etl_parameter_job_column_id: str
        etl_parameter_target_column_id: str
        redshift_output_table: str
        redshift_load_mode: str

        Returns:

        Externally configured success value or raises an exception

        This function sends the ETL status report for the day in an email and loads the report into a Redshift table.

        It does this by reading the crawled DynamoDB table which contains the ETL status information for all previous runs of VF Glue jobs.
        We are able to read this into a Glue Dynamic Frame directly. By casting this to a SparkSQL DataFrame, we may then filter
        the ETL status table by todays jobs only. We want to report on the job id, the status of the job and the datetime which it was
        processed. The resulting DataFrame is sent in an email to an externally configured list of recipients and then the DataFrame is loaded
        into a Redshift table according to an externally configured load mode
        """

        def get_glue_table(
            source_table, source_database, transformation_context, glueContext, log
        ):
            """
            Parameters:

            source_table: str
            source_database: str
            transformation_context: str
            glueContext: str
            log: logging.Logger

            Returns:

            DynamicFrame

            This function returns an AWS Glue DynamicFrame containing the contents of a table
            from the Glue catalogue of databases/tables. The structure of the DynamicFrame is
            determined by the structure of the crawled table in the Glue catalogue
            """
            log.info(
                "Reading crawled table {0} table from {1} database in glue catalogue".format(
                    source_table, source_database
                )
            )
            try:
                output_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                    database=source_database,
                    table_name=source_table,
                    transformation_ctx=transformation_context,
                )
            except Exception as error:
                err_msg = "Encountered error while reading crawled table {0} from {1} database in glue cataloge: {2}".format(
                    source_table, source_database, error
                )
                log.error(err_msg)
                raise Exception(err_msg)

            log.info(
                "Successfully read crawled DynamoDB table {0} into dynamic dataframe".format(
                    source_table
                )
            )
            return output_dynamic_frame

        def get_todays_etl_job_status_report(
            job_column_id,
            dttm_column_id,
            status_column_id,
            record_count_column_id,
            table_id,
            log,
            todays_date=str(datetime.datetime.now().date()),
        ):
            """
            Parameters:

            job_column_id: str
            dttm_column_id: str
            status_column_id: str
            record_count_column_id: str
            table_id: str
            log: logging.Logger
            todays_date: str

            Returns:

            pyspark.sql.DataFrame

            This function produces a DataFrame which filters an existing table in the Spark SQL catalogue by
            a job id column, the datetime for which the job was recorded and the completion status of the job.
            The result is registered to replace the source table in the SparkSQL catalogue
            """
            log.info("Filtering ETL status report data by todays date")
            spark.sql(
                """CREATE OR REPLACE TEMPORARY VIEW {3} AS
                   SELECT {0},
                          CAST({1} AS TIMESTAMP) AS {1},
                          {2},
                          {5}
                   FROM {3}
                   WHERE DATE(CAST({1} AS TIMESTAMP)) = CAST('{4}' AS DATE)
                   ORDER BY {0} ASC""".format(
                    job_column_id,
                    dttm_column_id,
                    status_column_id,
                    etl_status_table,
                    todays_date,
                    record_count_column_id,
                )
            )
            log.info("Successfully filtered ETL status report data by todays date")
            return

        def dynamic_frame_to_spark_catalogue(dynamic_frame, table_id, log):
            """
            Parameters:

            dynamic_frame: DynamicFrame
            table_id: str
            log: logging.Logger

            Returns:

            None

            This function registers a DynamicFrame as a table in the SparkSQL catalogue
            """
            log.info("Converting Dynamic Frame to standard DataFrame")
            df = dynamic_frame.toDF()
            log.info(
                "Registering dataframe of etl job status records to SparkSQL catalogue"
            )
            df.createOrReplaceTempView(table_id)
            log.info(
                "Successfully registered dataframe to SparkSQL catalogue as temp view: {0}".format(
                    table_id
                )
            )
            return

        def remove_timestamp_csv_file_extension(
            table_id,
            job_column_id,
            dttm_column_id,
            status_column_id,
            record_count_column_id,
            log,
        ):
            """
            Parameters:

            table_id: str
            job_column_id: str
            dttm_column_id: str
            status_column_id: str
            record_count_column_id: str
            log: logging.Logger

            Returns:

            None

            This function modifies a table registered in SparkSQL catalogue by removing all numeric
            characters and '.csv' from the input column
            """
            log.info(
                "Removing timestamp and .csv extension from job IDs in table {0} from column {1}".format(
                    table_id, job_column_id
                )
            )
            spark.sql(
                """CREATE OR REPLACE TEMPORARY VIEW {1} AS
                   SELECT REPLACE(REGEXP_REPLACE({0}, '[0-9]', ''), '.csv', '') AS {0},
                          {2},
                          {3},
                          {4}
                          FROM {1}""".format(
                    job_column_id,
                    table_id,
                    dttm_column_id,
                    status_column_id,
                    record_count_column_id,
                )
            )

            return

        def remove_trailing_underscore(
            table_id,
            job_column_id,
            dttm_column_id,
            status_column_id,
            record_count_column_id,
            log,
        ):
            """
            Parameters:

            table_id: str
            job_column_id: str
            dttm_column_id: str
            status_column_id: str
            record_count_column_id: str
            log: logging.Logger

            Returns:

            None

            This function modifies a table registered in SparkSQL catalogue by removing the trailing
            underscore from the job column id
            """

            log.info(
                "Removing trailing underscore in table {0} from column {1}".format(
                    table_id, job_column_id
                )
            )

            spark.sql(
                """CREATE OR REPLACE TEMPORARY VIEW {1} AS
                   SELECT CASE
                            WHEN SUBSTR({0}, -1) = '_' THEN SUBSTR({0}, 1, LENGTH({0}) - 1)
                            ELSE {0}
                          END AS {0},
                          {2},
                          {3},
                          {4}
                          FROM {1}""".format(
                    job_column_id,
                    table_id,
                    dttm_column_id,
                    status_column_id,
                    record_count_column_id,
                )
            )

            return

        def enrich_etl_job_status_report(
            etl_status_job_column_id,
            etl_status_dttm_column_id,
            etl_status_job_status_column_id,
            etl_status_record_count_column_id,
            etl_parameter_target_column_id,
            etl_status_table,
            etl_parameter_table,
            etl_parameter_job_column_id,
            spark_session,
        ):
            """
            Parameters:

            etl_status_job_column_id: str
            etl_status_dttm_column_id: str
            etl_status_job_status_column_id: str
            etl_status_record_count_column_id: str
            etl_parameter_target_column_id: str
            etl_status_table: str
            etl_parameter_table: str
            etl_parameter_job_column_id: str

            Returns:

            pyspark.sql.DataFrame

            This function produces an enriched form of an ETL job status report by reading from some status table
            and joining with an additional etl parameter table
            """
            log.info(
                "Performing enrichment of {0} through left join with {1}".format(
                    etl_status_table, etl_parameter_table
                )
            )
            try:
                output_df = spark_session.sql(
                    """SELECT t1.{0}, t1.{1}, t1.{2}, t1.{7}, t2.{3}
                       FROM {4} AS t1
                       LEFT JOIN {5} AS t2
                       ON t1.{0} = t2.{6}""".format(
                        etl_status_job_column_id,
                        etl_status_dttm_column_id,
                        etl_status_job_status_column_id,
                        etl_parameter_target_column_id,
                        etl_status_table,
                        etl_parameter_table,
                        etl_parameter_job_column_id,
                        etl_status_record_count_column_id,
                    )
                )
            except Exception as error:
                error_msg = "Failed to perform enrichment of {0}: {1}".format(
                    etl_status_table, error
                )
                log.error(error_msg)
                raise Exception(error_msg)

            log.info(
                "Successfully performed enrichment of {0}".format(etl_status_table)
            )

            return output_df

        log = self.logger
        log.info(
            "Initializing dynamic dataFrame access for sending daily AWS glue job summary report"
        )
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
        glueContext = self.glueContext
        spark = self.spark
        job = Job(glueContext)
        job.init(args["JOB_NAME"], args)
        log.info("Job successfully initialized")

        etl_job_status_report_dynamic_frame = get_glue_table(
            source_table=etl_status_table,
            source_database=glue_db,
            transformation_context="read_crawled_etl_status_table",
            glueContext=glueContext,
            log=log,
        )

        job_parameter_dynamic_frame = get_glue_table(
            source_table=etl_parameter_table,
            source_database=glue_db,
            transformation_context="read_crawled_etl_parameter_table",
            glueContext=glueContext,
            log=log,
        )

        dynamic_frame_to_spark_catalogue(
            dynamic_frame=etl_job_status_report_dynamic_frame,
            table_id=etl_status_table,
            log=log,
        )

        dynamic_frame_to_spark_catalogue(
            dynamic_frame=job_parameter_dynamic_frame,
            table_id=etl_parameter_table,
            log=log,
        )

        get_todays_etl_job_status_report(
            job_column_id=etl_status_job_column_id,
            dttm_column_id=etl_status_dttm_column_id,
            status_column_id=etl_status_job_status_column_id,
            record_count_column_id=etl_status_record_count_column_id,
            table_id=etl_status_table,
            log=log,
        )

        spark.sql("select * from {0}".format(etl_status_table)).show()

        remove_timestamp_csv_file_extension(
            table_id=etl_status_table,
            job_column_id=etl_status_job_column_id,
            dttm_column_id=etl_status_dttm_column_id,
            status_column_id=etl_status_job_status_column_id,
            record_count_column_id=etl_status_record_count_column_id,
            log=log,
        )

        spark.sql("select * from {0}".format(etl_status_table)).show()

        remove_trailing_underscore(
            table_id=etl_status_table,
            job_column_id=etl_status_job_column_id,
            dttm_column_id=etl_status_dttm_column_id,
            status_column_id=etl_status_job_status_column_id,
            record_count_column_id=etl_status_record_count_column_id,
            log=log,
        )

        remove_trailing_underscore(
            table_id=etl_status_table,
            job_column_id=etl_status_job_column_id,
            dttm_column_id=etl_status_dttm_column_id,
            status_column_id=etl_status_job_status_column_id,
            record_count_column_id=etl_status_record_count_column_id,
            log=log,
        )

        spark.sql("select * from {0}".format(etl_status_table)).show()

        daily_etl_job_status_report_df = enrich_etl_job_status_report(
            etl_status_job_column_id=etl_status_job_column_id,
            etl_status_dttm_column_id=etl_status_dttm_column_id,
            etl_status_job_status_column_id=etl_status_job_status_column_id,
            etl_status_record_count_column_id=etl_status_record_count_column_id,
            etl_parameter_target_column_id=etl_parameter_target_column_id,
            etl_status_table=etl_status_table,
            etl_parameter_table=etl_parameter_table,
            etl_parameter_job_column_id=etl_parameter_job_column_id,
            spark_session=spark,
        )

        daily_etl_job_status_report_df.show()

        spark.sql("select * from {0}".format(etl_status_table)).show()

        # Send report via email
        log.info("Sending todays ETL job status report by email")
        utils_ses.send_report_email(
            job_name=self.file_name,
            subject="{0} {1} daily ETL status summary report ".format(
                self.env_params["env_name"], str(datetime.datetime.utcnow().date())
            ),
            dataframes=[daily_etl_job_status_report_df],
            table_titles=[
                "VFAP Files processing status for "
                + datetime.datetime.utcnow().date().strftime("%b %d %Y")
            ],
            log=log,
        )

        # Load daily report into Redshift
        log.info(
            "Writing todays ETL summary report to Redshift table - {0}".format(
                redshift_output_table
            )
        )
        self.write_df_to_redshift_table(
            df=daily_etl_job_status_report_df,
            redshift_table=redshift_output_table,
            load_mode=redshift_load_mode,
        )

        return constant.success

    def reporting_crm_file_checklist(
        self,
        input_glue_job_status_table,
        input_glue_job_status_db,
        input_glue_etl_file_broker,
        input_glue_etl_file_broker_db,
        redshift_crm_file_summary_table,
        redshift_crm_file_not_present_this_week_table,
    ):
        """
        Parameters:

        input_glue_job_status_table: str
        input_glue_job_status_db: str
        input_glue_etl_file_broker: str
        input_glue_etl_file_broker_db: str
        redshift_crm_file_summary_table: str
        redshift_crm_file_not_present_this_week_table: str

        Returns:

        TODO: Fill out function description
        This function xxx
        """
        try:
            log = self.logger
            glueContext = self.glueContext
            spark = self.spark
            whouse_details = self.whouse_details
            _LEVEL = self.env_params["env_name"]
            # TODO: Make these function parameters

            log.info("Connecting to Athena and get data from it..")
            df_file_status = glueContext.create_dynamic_frame.from_catalog(
                database=input_glue_job_status_db,
                table_name=input_glue_job_status_table,
                transformation_ctx="dynFrame1",
            ).toDF()
            df_file_broker = glueContext.create_dynamic_frame.from_catalog(
                database=input_glue_etl_file_broker_db,
                table_name=input_glue_etl_file_broker,
                transformation_ctx=" dynFrame2",
            ).toDF()
            # Persist tables in memory due to multiple subsequent actions being called
            log.info(
                "Successfully read {0} and {1} from Glue catalogue".format(
                    input_glue_job_status_table, input_glue_etl_file_broker
                )
            )
            log.info("ETL file broker table count: {}".format(df_file_broker.count()))
            df_file_status.createOrReplaceTempView("df_file_status_table")
            df_file_broker.createOrReplaceTempView("df_file_broker_table")

            df_redshift = utils.run_file_checklist(whouse_details, log, spark)
            df_redshift.createOrReplaceTempView("df_redshift_table")

            log.info("Executing query to compute CRM job status")
            df_broker_status = spark.sql(
                """
            SELECT file_broker.feed_name AS input_config_file_name,
                   file_status.file_name AS status_file_name,
                   file_status.load_date AS status_load_date
            FROM (SELECT * from df_file_broker_table
                  WHERE upper(data_source) = 'CRM') AS file_broker
            LEFT JOIN (SELECT file_name,
                              SPLIT(refined_to_transformed.update_dttm,' ')[1] AS LOAD_DATE,
                              REGEXP_REPLACE(file_name,'[0-9]','')  AS file_name_wo_date
                       FROM df_file_status_table
                       WHERE UPPER(refined_to_transformed.status) = 'COMPLETED' AND refined_to_transformed.update_dttm BETWEEN DATE_FORMAT((current_date - interval '4' day),'%Y-%m-%d') AND DATE_FORMAT((current_date - interval '0' day),'%Y-%m-%d')
                       ORDER BY file_name, load_date) AS file_status
            ON file_broker.feed_name = file_status.file_name_wo_date
            """
            )
            log.info("Successfully computed CRM job status")
            df_broker_status.createOrReplaceTempView("df_broker_status_table")

            log.info("Executing query to compute CRM job summary")
            df_crm_file_summary = spark.sql(
                """
            SELECT df_broker_status.input_config_file_name,
                   df_broker_status.status_file_name,
                   df_broker_status.status_load_date,
                   df_redshift_daily_data.file_name AS redshift_file_name,
                   df_redshift_daily_data.Brand AS Brand,
                   df_redshift_daily_data.brand_count AS brand_count,
                   df_redshift_daily_data.load_date AS redshift_load_date

                   FROM df_broker_status_table df_broker_status
                   LEFT JOIN df_redshift_table df_redshift_daily_data
                   ON df_broker_status.input_config_file_name = df_redshift_daily_data.file_name
            """
            )
            log.info("Computed CRM job summary successfully")
            df_crm_file_summary.createOrReplaceTempView("df_crm_file_summary_table")
            df_crm_file_not_present_this_week = spark.sql(
                """
            SELECT df_crm_file_summary.input_config_file_name AS files_not_present_this_week
            FROM df_crm_file_summary_table AS df_crm_file_summary
            WHERE status_file_name IS NULL
            """
            )
            log.info("Successfully executed query to compute CRM file summary")
            df_crm_file_summary.persist()
            df_crm_file_not_present_this_week.persist()

            transformed_tables_dict = {
                redshift_crm_file_summary_table: df_crm_file_summary,
                redshift_crm_file_not_present_this_week_table: df_crm_file_not_present_this_week,
            }

            transformed_df_to_redshift_table_status = True
            for target_table, transformed_df in transformed_tables_dict.items():
                if transformed_df_to_redshift_table_status:
                    log.info(
                        "Inside datadict loop writing transformed_df to : {}".format(
                            target_table
                        )
                    )
                    # TODO: parameterize load mode
                    transformed_df_to_redshift_table_status = self.write_df_to_redshift_table(
                        df=transformed_df,
                        redshift_table=target_table,
                        load_mode="overwrite",
                    )
                    log.info(
                        "Response from writing to redshift is {}".format(
                            transformed_df_to_redshift_table_status
                        )
                    )
                else:
                    transformed_df_to_redshift_table_status = False
                    error_msg = "Failed to Load Transformed Data Dict To Redshift"
                    log.error(error_msg)
                    raise Exception(error_msg)
            log.info("Sending CRM file checklist report via email")

            email_subject = (
                "VFC/"
                + _LEVEL
                + "/"
                + datetime.datetime.now().strftime("%Y-%m-%d")
                + "/"
                + "CRM file checklist report"
            )

            utils_ses.send_report_email(
                job_name=self.file_name,
                subject=email_subject,
                dataframes=[df_crm_file_summary, df_crm_file_not_present_this_week],
                table_titles=["CRM file summary", "CRM files not present this week"],
                log=log,
            )

            return constant.success

        except Exception as error:
            log.error(
                "Error Occurred While processing run_full_file_checklist due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise CustomAppError(
                moduleName=constant.RUN_FULL_FILE_CHECKLIST,
                exeptionType=constant.TRANSFORMATION_EXCEPTION,
                message="Error Occurred While processing run_full_file_checklist due to : {}".format(
                    traceback.format_exc()
                ),
            )