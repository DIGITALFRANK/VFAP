from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
class dq_missing_cnt(Core_Job):
    def __init__(self, file_name, dq_item):
        self.dq_item = dq_item
        """Constructor for dq_missing_cnt

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            params {dict} -- dynamodb item python dictionary params related to file from file_broker table
            module
        """
        
        super(dq_missing_cnt, self).__init__(file_name)
        # need to change depending on read method and params
        # super(tr_weather_historical, self).__init__(logger)

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
            params = self.params
            logger = self.logger
            dq_item = self.dq_item
            logger.info("Applying dq_missing_cnt")
            
            # Getting the input_tbl,whr_condition & threshold parameters from DynamoDB.
            whr_condition = params["dq_params"][dq_item]["whr_condition"]
            threshold = params["dq_params"][dq_item]["threshold"]
            logger.info("Applying Filter condition")
            sql_out = df.filter(whr_condition).count()
            # Get the threshold value for the respective dq. If sql_out is less than threshold value
            # then it is considered as PASS. Also, returning the status of the DQ. This Status is added
            # to the Empty list in the core job. We are adding it to a list because, For example if the source
            # file requires 3 dq's to perform, then we are keeping the Status of each DQ and if all 3 status is
            # PASS then it will trigger respective TR>
            #
            logger.info("SQL_Out Count: {}".format(sql_out))
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
                "Error Ocuured While processiong dq_missing_cnt due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName =constant.DQ_MISSING_CNT , exeptionType = constant.DQ_EXCEPTION, message ="Error Ocuured While processiong dq_missing_cnt due to : {}".format(traceback.format_exc()) )
        return dq_status
