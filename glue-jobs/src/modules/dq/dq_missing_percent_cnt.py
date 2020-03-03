from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class dq_missing_percent_cnt(Core_Job):
    def __init__(self, file_name, dq_item):
        self.dq_item = dq_item

        """Constructor for tr_sr_weather

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            params {dict} -- dynamodb item python dictionary params related to file from
                             file_broker table
            module
        """
        super(dq_missing_percent_cnt, self).__init__(file_name)

        

    def dq(self, df):
        """This function is overriding the default transform method in the base class
        Arguments:
            df {spark.dataframe} -- source dataframe for input file

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """
        """
        """

        dq_status = False
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
            dq_item = self.dq_item
            logger.info("Applying dq_missing_percent_cnt")

            whr_condition = params["dq_params"][dq_item]["whr_condition"]
            threshold = params["dq_params"][dq_item]["threshold"]

            # Getting the input_tbl,whr_condition & threshold parameters from DynamoDB.
            df.createOrReplaceTempView("source_tbl")
            dq_query = "select count(*) as counts from {} where {}".format("source_tbl",whr_condition)
            sql_out = spark.sql(dq_query).first()["counts"]/spark.sql("select count(*) as counts from source_tbl").first()["counts"]
            #sql_out = df.filter(whr_condition).count() / df.count()
            # Get the threshold value for the respective dq. If sql_out is less than threshold value
            # then it is considered as PASS. Also, returning the status of the DQ. This Status is added
            # to the Empty list in the core job. We are adding it to a list because, For example if the source
            # file requires 3 dq's to perform, then we are keeping the Status of each DQ and if all 3 status is
            # PASS then it will trigger respective TR>
            logger.info("sql_out : {}".format(sql_out))
            if eval(threshold.format(sql_out=sql_out)):
                # If the sql_out is not meeting the threshold condition then it is considered as Fail.
                dq_status = False
                logger.info("Status of DQ - {}".format(dq_status))
            else:
                dq_status = True
            logger.info("dq_status : {}".format(dq_status))

        except Exception as error:
            dq_status = False
            logger.error(
                "Error Ocuured While processiong dq_missing_percent_cnt due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.DQ_MISSING_PERCENT_CNT,
                                 exeptionType=constant.DQ_EXCEPTION,
                                 message="Error Ocuured While processiong dq_missing_percent_cnt due to : {}".format(
                                     traceback.format_exc()))
        return dq_status
