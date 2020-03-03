from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from pyspark.sql.functions import lit
from modules.utils.utils_core import utils
from pyspark.sql.types import *
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback

class run_full_file_checklist(Core_Job):
    def __init__(self):
        """Constructor for run_full_file_checklist

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """

        super(run_full_file_checklist, self).__init__('F_TNF_CLASS_20191104151203.csv')
        self.logger.info("inside run_full_file_checklist constructor")

    def transform(self):
        self.logger.info(" Applying run_full_file_checklist ")
        try:
            
            logger = self.logger
            glueContext = self.glueContext
            spark= self.spark
            transformed_df_dict={}
            logger.info("Connecting to Athena and get data from it..")
            df_file_status = glueContext.create_dynamic_frame.from_catalog(database="sampledb",table_name="anchal_ddb_glue_output",transformation_ctx=" dynFrame1").toDF()
            df_file_broker = glueContext.create_dynamic_frame.from_catalog(database="cc_sample_db",table_name="akash_test_etl_file_broker",transformation_ctx=" dynFrame2").toDF()
            print("Read both the tables from Athena")
            print("df_file_broker_count : {}".format(df_file_broker.count()))
            df_file_broker.show()
            df_file_status.show()
            df_file_status.createOrReplaceTempView("df_file_status_table")
            df_file_broker.createOrReplaceTempView("df_file_broker_table")

            df_redshift = utils.run_file_checklist(self.whouse_details,logger,self.spark)
            df_redshift.createOrReplaceTempView("df_redshift_table")
            
            logger.info("Before executing the query")
            
            df_broker_status=spark.sql("""select 
            file_broker.feed_name as input_config_file_name, 
            file_status.file_name as status_file_name, 
            file_status.load_date as status_load_date
            from (select * from df_file_broker_table where upper(data_source) = 'CRM') file_broker 
            left join 
            (SELECT 
            file_name,
            split(refined_to_transformed.update_dttm,' ')[1] as load_date,
            regexp_replace(file_name,'[0-9]','')  as file_name_wo_date
            FROM df_file_status_table 
            where 
                upper(refined_to_transformed.status) = 'COMPLETED'  
            and refined_to_transformed.update_dttm between date_format((current_date - interval '4' day),'%Y-%m-%d') 
            and date_format((current_date - interval '0' day),'%Y-%m-%d')
            order by file_name,load_date) file_status
            on file_broker.feed_name = file_status.file_name_wo_date
            """)
            logger.info("After executing the query")
            df_broker_status.createOrReplaceTempView("df_broker_status_table")

            df_crm_file_summary = spark.sql("""select 
                    df_broker_status.input_config_file_name,
                    df_broker_status.status_file_name,
                    df_broker_status.status_load_date,
                    df_redshift_daily_data.file_name as redshift_file_name,
                    df_redshift_daily_data.Brand as Brand,
                    df_redshift_daily_data.brand_count as brand_count,
                    df_redshift_daily_data.load_date as redshift_load_date

                    from df_broker_status_table df_broker_status
                    left join 
                    df_redshift_table df_redshift_daily_data
                    on df_broker_status.input_config_file_name = df_redshift_daily_data.file_name
                    """)
            df_crm_file_summary.createOrReplaceTempView("df_crm_file_summary_table")
            df_crm_file_not_present_this_week = spark.sql("""select 
                    df_crm_file_summary.input_config_file_name as files_not_present_this_week
                    from df_crm_file_summary_table df_crm_file_summary
                    where status_file_name is null
                    """)
            logger.info("After executing the summary Query")
            df_crm_file_not_present_this_week.show()
            
            transformed_df_dict={
                'crm_file_summary':df_crm_file_summary,
                'crm_file_not_present_this_week':df_crm_file_not_present_this_week
            }
        except Exception as error:
            transformed_df_dict={}
            logger.error(
                "Error Occurred While processing run_full_file_checklist due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName=constant.RUN_FULL_FILE_CHECKLIST,
                                 exeptionType=constant.TRANSFORMATION_EXCEPTION,
                                 message="Error Occurred While processing run_full_file_checklist due to : {}".format(
                                     traceback.format_exc()))
            
        return transformed_df_dict
