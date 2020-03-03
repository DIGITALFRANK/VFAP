from modules.transforms.run_full_file_checklist import run_full_file_checklist
import sys
from awsglue.utils import getResolvedOptions
from modules.utils.utils_core import utils
import logging
from modules.config import config
from modules.constants import constant


def driver(args):
    """This driver function is created to trigger the ETL job for any given file.The
    file_name is received through an event

    Arguments:
        file_name {String} -- Name of the file that needs to be processed.
        args {dict} --        Dictionary of Glue job parameters
    """
    
    try:
        print("Starting run_full_file_checklist...")
        run_full_obj=run_full_file_checklist()
        transformed_tables_dict=run_full_obj.transform()
        transformed_df_to_redshift_table_status=True
        for target_table, transformed_df in transformed_tables_dict.items():
            if transformed_df_to_redshift_table_status:
                print(
                    "Inside datadict loop writing transformed_df to : {}".format(
                        target_table
                    )
                )
                transformed_df_to_redshift_table_status = run_full_obj.write_df_to_redshift_table(
                    df=transformed_df,
                    redshift_table=target_table,
                    load_mode="overwrite"
                    #load_mode=load_mode,
                )
                print(
                    "Response from writing to redshift is {}".format(
                        transformed_df_to_redshift_table_status
                    )
                )

            else:
                transformed_df_to_redshift_table_status = False
                print("Failed to Load Transformed Data Dict To Redshift")
        
        
    except Exception as error:
        print("Error Occurred Main {}".format(error))
        
    finally:
        print("Job exited with status : ")
        key = utils.get_parameter_store_key()
        params = utils.get_param_store_configs(key)
        utils.upload_file(config.LOG_FILE, params["log_bucket"], args)
        


if __name__ == "__main__":
    # *TODO: Pass file name through event

    # to do check for filename is valid or not ,if not raise filenamenotvalid exceptions
    args = getResolvedOptions(sys.argv, ["JOB_NAME","FILE_NAME"])
    driver(args)
    
