import sys
from awsglue.utils import getResolvedOptions
from modules.utils.utils_core import utils
from modules.core.core_job import Core_Job
from datetime import datetime
import modules.config.config as config
from modules.utils.utils_core import utils
from modules.app_log.create_logger import get_logger
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
from modules.utils import utils_ses
import traceback

def driver(job_type, map_name):
    """
    :param job_type:
    :param job_param:
    :return:
    """
    core_job = Core_Job(map_name)
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
            etl_status_record_parameters=utils.get_etl_status_tbl_initilize_params(map_name),
            etl_status_tbl_sort_key_as_job_process_dttm=str(job_process_dttm),
            env_params=params
        )
        if utils.check_day_of_week("map"):
            if job_type == "MAP" and map_name != "":
                # status=Map.run_map(job_param,job_process_dttm)
                status = core_job.map(map_name, job_process_dttm)
                if status == True:
                    status = constant.success

                else:
                    status = constant.failure
                logger.info("status of map : {}".format(status))
            else:
                status = constant.skipped
                logger.info("Missing Job Parameters : {}".format(status))

        else:
            logger.info("Skipping Weekly Map jobs as its a weekend")
            status = constant.skipped
    except Exception as error:
        status = constant.failure
        job_exception = str(traceback.format_exc())
        logger.error("Error ocurred in weekly_main due to : {}".format(job_exception), exc_info=True)
        raise CustomAppError(moduleName=error.moduleName, exeptionType=error.exeptionType, message=error.message)

    finally:
        driver_end_time = str(datetime.utcnow())
        logger.info("Job exited with status : {}".format(status))
        log_file_upload_status, log_file_path = utils.upload_file(config.LOG_FILE, params["log_bucket"], args)
        job_status_params = utils.get_glue_job_params(job_status=status, log_file_path=log_file_path)
        job_status_params.update({'error_traceback': job_exception})
        core_job.update_stage_status(
            stages_to_be_updated=job_status_params,
            etl_status_sort_key_as_job_process_dttm=str(job_process_dttm))
        if status is constant.failure:
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=map_name,
                log=logger,
                exception=job_exception,
            )
            # utils.upload_file(config.LOG_FILE, params["log_bucket"], args)
            raise Exception("Job failed gracefully - check log and/or email notification for details")
        else:
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=map_name,
                log=logger,
                exception=None)


if __name__ == "__main__":
    # for weekly job pass job_type and job_param as parameters
    # for weekly Xref JOB_TYPE="XREF" and JOB_PARAM={XREF}
    # for weekly map JOB_TYPE="MAP" and JOB_PARAM={Name of the map that should run}
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "JOB_TYPE", "JOB_PARAM"])
    job_type = args["JOB_TYPE"]
    job_param = args["JOB_PARAM"]
    map_processing_dt = datetime.utcnow().strftime(config.FILE_DATE_FORMAT)
    map_name = job_param + "_{}".format(map_processing_dt)
    driver(job_type, map_name)
