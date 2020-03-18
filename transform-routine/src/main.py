from modules.dataprocessor.dataprocessor_job import Dataprocessor_Job
from modules.dataprocessor.dataprocessor_merge import Dataprocessor_merge
import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from modules.utils.utils_dataprocessor import utils
from modules.utils.utils_dynamo import DynamoUtils
import logging
from modules.config import config
from datetime import datetime
from modules.app_log.create_logger import get_logger
import boto3


def driver(file_pattern, date):
    """This driver function is created to trigger the ETL job for files that
    need to be merged.

    Arguments:
        file_pattern {String} -- Pattern of file that needs to be processed.
		Date (String) - Date preset in file which need to be processed
    """

    # job_process datetime
    job_process_dttm = datetime.utcnow()
    driver_start_time = str(datetime.utcnow())
    job_exception = config.job_exception
    key = utils.get_parameter_store_key()
    params = utils.get_param_store_configs(key)
    logger = get_logger()
    status = None
    try:
        response = DynamoUtils.check_record(file_pattern, params)
        if response == False:
            # initializing etl status table record
            print("Initiating data load")
            create_etl_status_record_status, job_process_dttm = \
                utils.create_etl_status_record(
                    etl_status_record_parameters=
                    utils.get_etl_status_tbl_initilize_params(file_pattern),
                    etl_status_tbl_sort_key_as_job_process_dttm=
                    str(job_process_dttm),
                    env_params=params
                )
            status = Dataprocessor_merge.process(file_pattern, date,
                                                 job_process_dttm)
            driver_end_time = str(datetime.utcnow())
            print("Job exited with status : {}".format(status))
            if status == 'Success':
                log_file_upload_status, log_file_path = utils.upload_file(
                    config.LOG_FILE, params["log_bucket"])
                job_status_params = utils.get_glue_job_params(
                    job_status=status, log_file_path=log_file_path)
                job_status_params.update({'error_traceback': job_exception})
                Dataprocessor_merge(file_pattern, date).update_stage_status(
                    stages_to_be_updated=job_status_params,
                    etl_status_sort_key_as_job_process_dttm=str(
                        job_process_dttm))

        else:
            sc = SparkContext.getOrCreate()
            print("Record already exist in status table")
            status = "FALSE"

    except Exception as error:
        status = 'failed'
        log_file_upload_status, log_file_path = utils.upload_file(
            config.LOG_FILE, params["log_bucket"])
        job_status_params = utils.get_glue_job_params(
            job_status=status, log_file_path=log_file_path)
        Dataprocessor_Job(file_name, job_run_id).update_stage_status(
            stages_to_be_updated=job_status_params,
            etl_status_sort_key_as_job_process_dttm=str(
                job_process_dttm))
        print("Error Occurred: {}".format(error))
        raise Exception("{}".format(error))


if __name__ == "__main__":
    # access the arguments that are passed to script when job is initiated
    args = getResolvedOptions(sys.argv, ['JOB_NAME', "FILE_NAME", "DATE"])
    file_pattern = args["FILE_NAME"]
    date = args["DATE"]
    driver(file_pattern, date)
