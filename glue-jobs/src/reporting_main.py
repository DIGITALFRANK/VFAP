from modules.reporting.reporting_job import Reporting_Job
import sys
from awsglue.utils import getResolvedOptions
from modules.utils.utils_core import utils
import datetime
from modules.config import config
from modules.constants import constant
from modules.utils import utils_ses
from modules.app_log.create_logger import get_logger


def driver(file_name, args):
    """
    Parameters:

    file_name: str
    args: Dict[str, str]

    Returns:

    None

    This driver function triggers the reporting job which corresponds to the file_name parameter
    """
    # Instantiate job
    try:
        logger = get_logger()
        key = utils.get_parameter_store_key()
        job_start_time = datetime.datetime.utcnow()
        status_params = {
            "job_id": utils.get_glue_job_run_id(),
            "job_start_time": str(job_start_time),
        }
        etl_status_params = {"file_name": file_name}
        reporting_job_obj = Reporting_Job(file_name)
        status_creation_result, status_load_key = utils.create_etl_status_record(
            etl_status_record_parameters=etl_status_params,
            env_params=reporting_job_obj.env_params,
            logger=reporting_job_obj.logger,
        )

        if status_creation_result == constant.ETL_STATUS_CREATION_FAILED:
            raise Exception("Unable to create ETL status record in DynamoDB")

    except Exception as error:
        error_msg = "Failed to instantiate job: {0}".format(error)
        logger.error(error_msg)
        utils.upload_file(
            config.LOG_FILE, utils.get_param_store_configs(key)["log_bucket"], args
        )
        raise Exception(error_msg)

    # Attempt job run
    try:
        if file_name == "reporting_week_weather_sum":
            # Only runs on Mondays - 0
            if (
                datetime.datetime.utcnow().weekday()
                == reporting_job_obj.params["tr_params"]["run_on_day_of_week"]
            ):
                stage_status = reporting_job_obj.reporting_week_weather_sum(
                    input_weather_table=reporting_job_obj.params["tr_params"][
                        "input_tables"
                    ]["weather_in"],
                    output_weather_table=reporting_job_obj.params["tr_params"][
                        "output_tables"
                    ]["weather_out"],
                    output_weather_table_write_mode=reporting_job_obj.params[
                        "tr_params"
                    ]["output_tables"]["weather_out_write_mode"],
                    output_missing_weeks_table=reporting_job_obj.params["tr_params"][
                        "output_tables"
                    ]["missing_weeks"],
                    output_missing_weeks_table_write_mode=reporting_job_obj.params[
                        "tr_params"
                    ]["output_tables"]["missing_weeks_write_mode"],
                    bulk=reporting_job_obj.params["tr_params"]["window_aggregation"][
                        "bulk"
                    ],
                    bulk_begin_dt=reporting_job_obj.params["tr_params"][
                        "window_aggregation"
                    ]["bulk_begin_dt"],
                )

            else:
                logger.info("Job week_weather_sum only set to run on Mondays")
                stage_status = constant.skipped

        if file_name == "reporting_crm_file_checklist":
            logger.info("Job reporting_crm_file_checklist started")
            stage_status = reporting_job_obj.reporting_crm_file_checklist(
                redshift_crm_file_summary_table=reporting_job_obj.params["tr_params"][
                    "redshift_crm_file_summary_table"
                ],
                redshift_crm_file_not_present_this_week_table=reporting_job_obj.params[
                    "tr_params"
                ]["redshift_crm_file_not_present_this_week_table"],
                status_query_end_date=reporting_job_obj.params["tr_params"][
                    "status_query_end_date"
                ],
                status_query_interval_days=reporting_job_obj.params["tr_params"][
                    "status_query_interval_days"
                ],
                crm_file_count_constraint=reporting_job_obj.params["tr_params"][
                    "crm_file_count_constraint"
                ],
            )

        if file_name == "reporting_etl_rpt_missing_dates":
            logger.info("Job reporting_etl_rpt_missing_dates started")
            stage_status = reporting_job_obj.reporting_etl_rpt_missing_dates(
                load_mode=reporting_job_obj.params["write_mode"]
            )

        if file_name == "reporting_csv_build_email_inputs":
            logger.info("Job reporting_csv_build_email_inputs started")
            stage_status = reporting_job_obj.reporting_csv_build_email_inputs()

        if file_name == "reporting_send_daily_etl_job_status_report":
            stage_status = reporting_job_obj.reporting_send_daily_etl_job_status_report(
                etl_status_job_column_id=reporting_job_obj.params["tr_params"][
                    "etl_status_job_column_id"
                ],
                etl_status_dttm_column_id=reporting_job_obj.params["tr_params"][
                    "etl_status_dttm_column_id"
                ],
                etl_status_job_status_column_id=reporting_job_obj.params["tr_params"][
                    "etl_status_job_status_column_id"
                ],
                etl_status_record_count_column_id=reporting_job_obj.params["tr_params"][
                    "etl_status_record_count_column_id"
                ],
                etl_parameter_job_column_id=reporting_job_obj.params["tr_params"][
                    "etl_parameter_job_column_id"
                ],
                etl_parameter_target_column_id=reporting_job_obj.params["tr_params"][
                    "etl_parameter_target_column_id"
                ],
                redshift_output_table=reporting_job_obj.params["tr_params"][
                    "redshift_output_table"
                ],
                redshift_load_mode=reporting_job_obj.params["tr_params"][
                    "redshift_load_mode"
                ],
            )
        # Insert additional reporting jobs here

        if stage_status == constant.success:
            stage_error = constant.error_null
            logger.info("Job completed successfully")

        elif stage_status == constant.skipped:
            stage_error = constant.error_null
            logger.info("Job skipped")

        status_params["reporting_status"] = {
            "error_info": stage_error,
            "status": stage_status,
            "update_dttm": str(datetime.datetime.utcnow()),
        }
        status_params["job_status"] = stage_status

    except Exception as error:
        logger.error("Error occurred in reporting_main driver: {}".format(error))
        stage_status = constant.failure
        status_params["reporting_status"] = {
            "error_info": str(error),
            "status": stage_status,
            "update_dttm": str(datetime.datetime.utcnow()),
        }
        status_params["job_status"] = stage_status
        job_exception = error

    finally:
        status_params["job_end_time"] = str(datetime.datetime.utcnow())
        # Guarantee failed status for glue
        if status_params["reporting_status"]["status"] == constant.failure:
            utils_ses.send_job_status_email(
                job_status=status_params["reporting_status"]["status"],
                start_time=status_params["job_start_time"],
                end_time=status_params["job_end_time"],
                file_name=file_name,
                log=reporting_job_obj.logger,
                exception=job_exception,
            )
            # Load status report to DDB
            logger.info("Updating job status in Dynamo DB: {}".format(status_params))
            try:
                reporting_job_obj.update_stage_status(status_params, status_load_key)
            except Exception as error:
                logger.error(
                    "Encountered error while attempting to update stage status: {0}".format(
                        error
                    )
                )
            finally:
                # Load log to S3
                utils.upload_file(
                    config.LOG_FILE,
                    utils.get_param_store_configs(key)["log_bucket"],
                    args,
                )

            raise Exception(
                "Job failed gracefully - check log and/or email notification for details"
            )
        else:
            utils_ses.send_job_status_email(
                job_status=status_params["reporting_status"]["status"],
                start_time=status_params["job_start_time"],
                end_time=status_params["job_end_time"],
                file_name=file_name,
                log=reporting_job_obj.logger,
                exception=None,
            )

            # Load status report to DDB
            logger.info("Updating job status in Dynamo DB: {}".format(status_params))
            try:
                reporting_job_obj.update_stage_status(status_params, status_load_key)
            except Exception as error:
                logger.error(
                    "Encountered error while attempting to update stage status: {0}".format(
                        error
                    )
                )
            finally:
                # Load log to S3
                utils.upload_file(
                    config.LOG_FILE,
                    utils.get_param_store_configs(key)["log_bucket"],
                    args,
                )


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "FILE_NAME"])
    file_name = args["FILE_NAME"]
    driver(file_name, args)
