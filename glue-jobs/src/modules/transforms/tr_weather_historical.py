from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
from pyspark.sql.types import IntegerType
from modules.utils.utils_core import utils


class tr_weather_historical(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_sr_weather

        Arguments:
            TransformCore {class} -- Base class within transform module
            file_name {String} -- input file name
        """
        super(tr_weather_historical, self).__init__(file_name)

    def transform(self, df):
        """This function is overriding the default transform method in the base class
        Arguments:
            df {spark.dataframe} -- source dataframe for input file

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """
        """
        """

        full_load_df = None
        transformed_df_dict = {}
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
            redshift_schema = self.whouse_details["dbSchema"]
            logger.info("Applying tr_weather_historical")
            target_table_stage = params["tgt_dstn_tbl_name"] + "_stage"
            # warehouse_df = self.redshift_table_to_dataframe(
            #     redshift_table=params["tgt_dstn_tbl_name"]
            # )
            # add sas_process_dt column having date value from file_name
            refined_df = df.withColumn("SAS_PROCESS_DT", F.lit(params["file_date"])).withColumn("sas_brand_id", lit(int(params["sas_brand_id"]))).withColumn("process_dtm", F.current_timestamp()).withColumn("file_name", F.lit(self.file_name))
                df.withColumn("fs_sk", lit(None).cast(IntegerType()))
                .withColumn("sas_brand_id", lit(int(params["sas_brand_id"])))
                .withColumn("SAS_PROCESS_DT", F.lit(params["file_date"]))
                .withColumn("process_dtm", F.current_timestamp())
                .withColumn("file_name", F.lit(self.file_name))
            )

            # renaming columns from I_TNF_WeatherTrends_vf_historical_data file as present at destination
            logger.info("Schema After Adding SAS_PROCESS_DT : ")
            refined_df.printSchema()

            # create temp view for refined data
            logger.info(
                "Writing refined df to redshift table!!! : {}".format(
                    target_table_stage
                )
            )
            refined_df.createOrReplaceTempView(config.TR_WEATHER_REFINED_TEMP_VIEW)

            # refined_df_to_redshift_table_status = self.write_df_to_redshift_table(
            #             df=refined_df,
            #             redshift_table=target_table_stage,
            #             load_mode="overwrite",
            #         )

            drp_stg_tbl_qry = "drop table if exists {0}".format(
                redshift_schema + "." + target_table_stage
            )
            utils.execute_query_in_redshift(
                drp_stg_tbl_qry, self.whouse_details, logger
            )
            create_stg_tabl_qry = "create table {0} as select * from {1} where 1=2".format(
                redshift_schema + "." + target_table_stage,
                redshift_schema + "." + params["tgt_dstn_tbl_name"],
            )
            utils.execute_query_in_redshift(
                create_stg_tabl_qry, self.whouse_details, logger
            )
            status = self.write_glue_df_to_redshift(
                df=refined_df, redshift_table=target_table_stage, load_mode="overwrite",
            )
            logger.info("Refined DF copied to redshift successfully...")

            update_location_qry1 = """update {0} 
                                    set location = regexp_replace(location,'[a-z,A-Z]','')""".format(
                redshift_schema + "." + target_table_stage
            )

            changed_records_qry = """create temp table changed_records as
                                        select a.* from {0} as a left outer join {1} as b
                                        on
                                        trim(b.location) = trim(a.location) and a.dt = b.dt
                                        where
                                        b.dt is null or (b.dt is not null and a.sas_process_dt >   b.sas_process_dt)""".format(
                redshift_schema + "." + target_table_stage,
                redshift_schema + "." + params["tgt_dstn_tbl_name"],
            )

            retained_records_qry = """create temp table retained_records  as
                                        select warehouse.* from  {0} as warehouse
                                        left outer join changed_records as change
                                        on
                                        trim(warehouse.location)=trim(change.location)
                                        and
                                        warehouse.dt = change.dt
                                        where
                                        change.dt is null""".format(
                redshift_schema + "." + params["tgt_dstn_tbl_name"]
            )

            truncate_tgt_tbl_qry = "truncate table {0}".format(
                redshift_schema + "." + params["tgt_dstn_tbl_name"]
            )
            insert_into_tgt_tbl_qry = """insert into {0} 
                                         (select * from changed_records union 
                                          select * from retained_records)""".format(
                redshift_schema + "." + params["tgt_dstn_tbl_name"]
            )

            update_location_qry = """update {0} 
                                    set location = regexp_replace(location,'[a-z,A-Z]','')""".format(
                redshift_schema + "." + params["tgt_dstn_tbl_name"]
            )

            execute_above_queries_list = [
                update_location_qry1,
                changed_records_qry,
                retained_records_qry,
                truncate_tgt_tbl_qry,
                insert_into_tgt_tbl_qry,
                update_location_qry
            ]
            utils.execute_multiple_queries_in_redshift(
                execute_above_queries_list, self.whouse_details, logger
            )

            logger.info("Executed All queries successfully!!!")

            full_load_df = refined_df

            transformed_df_dict = {target_table_stage: full_load_df}

        except Exception as error:
            full_load_df = None
            transformed_df_dict={}
            logger.error(
                " : {}".format(
                    error
                ),exc_info=True
            )
        return transformed_df_dict
