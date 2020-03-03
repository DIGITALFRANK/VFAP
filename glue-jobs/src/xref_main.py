from modules.xrefs.xr_xref_job import Xref_Job
import sys
from awsglue.utils import getResolvedOptions
from modules.utils.utils_core import utils
import logging
from datetime import datetime
from modules.config import config
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
from modules.utils import utils_ses
from modules.config import xref_global_config


def driver(file_name, args):
    """This driver function is created to trigger the Weekly Xref job.
    Arguments:
        file_name {String} -- Name of the weekly xref that needs to be processed.
        args {dict} --        Dictionary of Glue job parameters
    """
    xref_stage_params_to_be_updated = {
        "job_id": utils.get_glue_job_run_id(),
        "job_start_time": str(datetime.utcnow()),
    }
    # appending current_timestamp to file_name
    file_name_to_be_updated_in_etl_status = file_name + "_{}".format(
        datetime.utcnow().strftime(config.FILE_DATE_FORMAT)
    )
    Xref_job_obj = Xref_Job(file_name_to_be_updated_in_etl_status)
    job_process_dttm = datetime.utcnow()
    etl_status_sort_key_as_job_process_dttm = str(job_process_dttm)
    driver_start_time = str(datetime.utcnow())
    job_exception = constant.job_exception
    logger = Xref_job_obj.logger
    status = False

    try:

        # initializing etl status table record
        etl_status_record_parameters = {
            "file_name": file_name_to_be_updated_in_etl_status
        }

        create_etl_record_status = utils.create_etl_status_record(
            etl_status_record_parameters=etl_status_record_parameters,
            etl_status_tbl_sort_key_as_job_process_dttm=str(job_process_dttm),
            env_params=Xref_job_obj.env_params,
            logger=Xref_job_obj.logger,
        )

        stage_status_to_be_updated = False
        stage_error_info_to_be_updated = "null"

        if utils.check_day_of_week("xrefs"):
            # if True:
            # **Order of Xrefs
            """        cust_xref,loyalty_xref and email_xref are independent
            clean_tnf is dependent on TNF_TABLE_SENT_VIEW which is created by email_xref
            and cust_xref
            session_xref is dependent on email_xref
            Adobe Xref will be implemented independently
            """

            if file_name == constant.session_xref:
                status = Xref_job_obj.xr_create_cm_session_xref()
                Xref_job_obj.logger.info("cm_status is {}".format(status))
            elif file_name == constant.tnf_style_xref:
                status = Xref_job_obj.xr_build_style_xref_clean_tnf()
                Xref_job_obj.logger.info("status is {}".format(status))
            elif file_name == constant.email_xref:
                status = Xref_job_obj.xr_email_xref()
                Xref_job_obj.logger.info("status is {}".format(status))
            elif file_name == constant.loyalty_xref:
                status = Xref_job_obj.xr_create_loyalty_xref()
                Xref_job_obj.logger.info("status is {}".format(status))
            elif file_name == constant.customer_xref:
                status = Xref_job_obj.xr_build_cust_xref_final()
                status = Xref_job_obj.xr_build_cust_xref_final()
                Xref_job_obj.logger.info("status is {}".format(status))
            elif file_name == constant.vans_clean_xref:
                status = Xref_job_obj.xr_build_style_xref_clean_vans()
                Xref_job_obj.logger.info("status is {}".format(status))
            else:
                Xref_job_obj.logger.info("Skipping Xref {}".format(file_name))
        else:
            status = constant.skipped
            Xref_job_obj.logger.info(constant.weekly_job_skip)

        if status == True:
            stage_status_to_be_updated = config.STAGE_COMPLETED_STATUS
            stage_error_info_to_be_updated = "null"
            status = constant.success
        elif status == constant.skipped:
            stage_status_to_be_updated = constant.skipped
            stage_error_info_to_be_updated = "null"
        else:
            stage_status_to_be_updated = config.STAGE_FAILED_STATUS
            stage_error_info_to_be_updated = "XREF Failed"
        xref_stage_params_to_be_updated.update(
            {
                "xref_status": {
                    "error_info": stage_error_info_to_be_updated,
                    "status": stage_status_to_be_updated,
                    "update_dttm": str(datetime.utcnow()),
                }
            }
        )

    except Exception as error:
        status = constant.failure
        job_exception = str(traceback.format_exc())
        logger.error("Error Occurred Main {}".format(error), exc_info=True)
        xref_stage_params_to_be_updated.update(
            {
                "xref_status": {
                    "error_info": "XREF Failed",
                    "status": constant.failure,
                    "update_dttm": str(datetime.utcnow()),
                    "error_traceback": str(error),
                },
                "error_traceback": job_exception,
            }
        )
        raise CustomAppError(
            moduleName=error.moduleName,
            exeptionType=error.exeptionType,
            message=error.message,
        )
    finally:
        driver_end_time = str(datetime.utcnow())
        logger.info("Job exited with status : {}".format(status))
        # *!Hardcoded to test.Make this configurable
        xref_stage_params_to_be_updated.update({"job_end_time": str(datetime.utcnow())})
        logger.info(
            "Updating Status in db : {}".format(xref_stage_params_to_be_updated)
        )
        Xref_job_obj.update_stage_status(
            xref_stage_params_to_be_updated, etl_status_sort_key_as_job_process_dttm
        )
        key = utils.get_parameter_store_key()
        params = utils.get_param_store_configs(key)
        log_file_upload_status, log_file_path = utils.upload_file(
            config.LOG_FILE, params["log_bucket"], args
        )
        Xref_job_obj.update_stage_status(
            stages_to_be_updated=utils.get_glue_job_params(
                job_status=status, log_file_path=log_file_path
            ),
            etl_status_sort_key_as_job_process_dttm=str(job_process_dttm),
        )
        Xref_job_obj.logger.info("Status in main : {}".format(status))
        if status is constant.failure:
            Xref_job_obj.logger.info("Inside if ")
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=file_name,
                log=Xref_job_obj.logger,
                exception=str(job_exception),
            )
            # utils.upload_file(config.LOG_FILE, params["log_bucket"], args)
            raise Exception(
                "Job failed gracefully - check log and/or email notification for details"
            )
        else:
            Xref_job_obj.logger.info("Inside else")
            utils_ses.send_job_status_email(
                job_status=status,
                start_time=driver_start_time,
                end_time=driver_end_time,
                file_name=file_name,
                log=Xref_job_obj.logger,
            )


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "FILE_NAME"])
    file_name = args["FILE_NAME"]
    # file_name = "I_VANS_WCS_VANS_WISHLIST_20191101000000.csv"
    driver(file_name, args)
