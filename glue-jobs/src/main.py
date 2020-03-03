from modules.core.core_job import Core_Job
import sys
from awsglue.utils import getResolvedOptions
import logging
from modules.config import config
from modules.constants import constant
from modules.utils import utils_ses
from datetime import datetime
from modules.utils.utils_core import utils
from modules.app_log.create_logger import get_logger
from modules.exceptions.CustomAppException import CustomAppError


def driver(file_name, args):
    """This driver function is created to trigger the ETL job for any given file.The
    file_name is received through an event

    Arguments:
        file_name {String} -- Name of the file that needs to be processed.
        args {dict} --        Dictionary of Glue job parameters
    """
    # job_process datetime
    job_process_dttm = datetime.utcnow()
    status = constant.failure
    driver_start_time = str(datetime.utcnow())
    job_exception = constant.job_exception
    key = utils.get_parameter_store_key()
    params = utils.get_param_store_configs(key)
    logger = get_logger()
    try:
        # initializing etl status table record
        utils.create_etl_status_record(
            etl_status_record_parameters=utils.get_etl_status_tbl_initilize_params(
                file_name
            ),
            etl_status_tbl_sort_key_as_job_process_dttm=str(job_process_dttm),
            env_params=params,
        )
        if utils.check_day_of_week("daily"):
            status = Core_Job.process(file_name, job_process_dttm)
        else:
            logger.info(constant.daily_job_skip)
            status = constant.skipped
    except CustomAppError as error:
        logger.error("Error Occurred Main {}".format(error), exc_info=True)
        status = constant.failure
        job_exception = str(error)
        raise CustomAppError(
            moduleName=error.moduleName,
            exeptionType=error.exeptionType,
            message=error.message,
        )
    finally:
        driver_end_time = str(datetime.utcnow())
        logging.info("Job exited with status : {}".format(status))
        log_file_upload_status, log_file_path = utils.upload_file(
            config.LOG_FILE, params["log_bucket"], args
        )
        job_status_params = utils.get_glue_job_params(
            job_status=status, log_file_path=log_file_path
        )
        job_status_params.update({"error_traceback": job_exception})
        Core_Job(file_name).update_stage_status(
            stages_to_be_updated=job_status_params,
            etl_status_sort_key_as_job_process_dttm=str(job_process_dttm),
        )
        if status is constant.failure:
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=file_name,
                log=logger,
                exception=job_exception,
            )
            # utils.upload_file(config.LOG_FILE, params["log_bucket"], args)
            raise Exception(
                "Job failed gracefully - check log and/or email notification for details"
            )
        else:
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=file_name,
                log=logger,
                exception=None,
            )


if __name__ == "__main__":
    # *TODO: Pass file name through event

    # to do check for filename is valid or not ,if not raise filenamenotvalid exceptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "FILE_NAME"])
    file_name = args["FILE_NAME"]
    driver(file_name, args)
