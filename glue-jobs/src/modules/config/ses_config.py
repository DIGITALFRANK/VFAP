"""
This module contains the relevant data for configuring the use of SES
through AWS Glue via an SMTP client in Python
"""
from modules.utils.utils_core import utils

key = utils.get_parameter_store_key()
env_params = utils.get_param_store_configs(key)
whouse_details = utils.get_secret(env_params["secret_manager_key"])

smtp_username = whouse_details["smtp_username"]
smtp_password = whouse_details["smtp_password"]

smtp_host = env_params["smtp_host"]
smtp_port = int(env_params["smtp port"])
job_status_email_recipient_address_list = env_params["job_status_email_recipient_address_list"].split(",")
job_status_email_sender_address = env_params["job_status_email_sender_address"]
job_status_email_sender_name = env_params["job_status_email_sender_name"]

environment = env_params["env_name"]

reporting_email_sender_address = env_params["reporting_email_sender_address"]
reporting_email_recipient_address_list = env_params["reporting_email_recipient_address_list"].split(",")
reporting_email_sender_name = env_params["reporting_email_sender_name"]
