from pyspark.sql import functions as F
import boto3
from modules.core.core_job import Core_Job
import modules.config.config as config
from modules.utils.utils_core import utils
from string import digits
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
from awsglue.dynamicframe import DynamicFrame
class dq_cust_style_product_missing_cnt(Core_Job):
    def __init__(self, file_name, dq_item):
        """Constructor for dq_cust_style_product_missing_cnt

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            params {dict} -- dynamodb item python dictionary params related to file from file_broker table
            module
        """
        self.dq_item = dq_item
        super(dq_cust_style_product_missing_cnt, self).__init__(file_name)

        

    def get_list_of_files_from_bucket(self,file_bucket, prfx):

        logger = self.logger
        # Get List of files from the Refined - current folder and respective subfolders.
        file_list_dict_name = {}
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(file_bucket)
        for s3_file in bucket.objects.filter(Prefix=prfx):
            key_trim = s3_file.key.split("/")[-1]
            # removing the datetime value from the filename and creating key value pair.
            remove_digits = str.maketrans("", "", digits)
            refined_bucket_files = key_trim.translate(remove_digits).lower()
            file_list_dict_name[refined_bucket_files.split(".")[0]] = key_trim
            # logger.info("refined_files : {}".format(file_list_dict_name))
        #del file_list_dict_name[""]
        logger.info(
            "refined_files after deleting empty key: {}".format(file_list_dict_name)
        )
        return file_list_dict_name

    def dq(self, df):
        """This function is overriding the default transform method in the base class
        Arguments:
            df {spark.dataframe} -- source dataframe for input file

        Returns:
            [spark.dataframe] -- Returns spark dataframe after applying all the
             transformations
        """

        dq_status = False
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
            dq_item = self.dq_item
            file_name = self.file_name
            logger.info("Applying dq_cust_style_product_missing_cnt")
           
           # Added on 07-02-2020
            redshift_url = "jdbc:{}://{}:{}/{}".format(
            self.whouse_details["engine"],
            self.whouse_details["host"],
            self.whouse_details["port"],
            self.whouse_details["dbCatalog"],
            )

            username = self.whouse_details["username"]
            password = self.whouse_details["password"]
            temp_bucket = "s3://{}/{}/".format(self.env_params["refined_bucket"], "temp")
            redshift_schema = self.whouse_details["dbSchema"]
            

            # database = "vfuap"
            # redshift_conn = "reshift-conn"
            redshift_conn = self.env_params["glue_rs_conn"]
            database = self.whouse_details["dbCatalog"]

            
            # Added till here.

            threshold = params["dq_params"][dq_item]["threshold"]
            qry_file_present = params["dq_params"][dq_item]["qry_file_present"]
            qry_file_not_present = params["dq_params"][dq_item]["qry_file_not_present"]
            whouse_tbl = params["dq_params"][dq_item]["whouse_tbl"]
            current_folder_path = params["dq_params"][dq_item][
                "current_folder_path"
            ]  # "Refinded/dq_check/"
            ref_file_folder_path = params["dq_params"][dq_item]["ref_file_folder_path"]
            sas_brand_id = params["sas_brand_id"]
            tgt_dstn_tbl_name = params["tgt_dstn_tbl_name"]

            # Remove the date from the file name and Identify TNF/VANS from the file_name
            file_name_trim = (
                file_name.split(".")[0]
                .translate(str.maketrans("", "", digits))
                .split("_")[1]
            )
            logger.info("Source File : {}".format(file_name_trim))

            # Register a temporary table for the source dataframe.
            df.createOrReplaceTempView("source_tbl")

            # create pattern to identify today's reference file for correct source(TNF/VANS)
            to_find = ("f_" + file_name_trim + "_" + str(whouse_tbl) + '_').lower()
            logger.info("Todays reference file to find: {}".format(to_find))

            # Get list of files from current folder in the Refined bucket.
            logger.info("Get List of Files from Current Folder -- Refined Bucket")
            current_folder_file_dict = self.get_list_of_files_from_bucket(file_bucket = self.env_params["refined_bucket"],
                prfx=current_folder_path
            )
            logger.info("Get List of Files from Refined Folder -- Transformed Bucket")
            ref_folder_file_dict = self.get_list_of_files_from_bucket(file_bucket = self.env_params["transformed_bucket"],
                prfx=ref_file_folder_path
            )
            logger.info("Todays file to find :{}".format(to_find.lower()))
            whouse_schema = self.whouse_details["dbSchema"]+'.'
            source_table = file_name.split(".")[0].translate(str.maketrans("", "", digits)) + 'dq'
            dq_status_table = self.whouse_details["dbSchema"]+'.'+source_table+"_dq_status"
            del_qry= "drop table if exists {};"
            drop_final_status_table_query = del_qry.format(dq_status_table)

            #Create empty source table in Redshift:
            if whouse_tbl == "PRODUCT_XREF":
                whouse_empty_tbl = "PRODUCTXREF"
            else:
                whouse_empty_tbl = whouse_tbl

            dynamic_df_source = DynamicFrame.fromDF(df, self.glueContext, "dynamic_df_source")

            empty_src_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+source_table,whouse_schema+tgt_dstn_tbl_name)
            utils.execute_query_in_redshift(empty_src_tbl_qry,self.whouse_details,logger)

            empty_src_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+source_table+self.params["brand"]+'_stage',whouse_schema+tgt_dstn_tbl_name)
            utils.execute_query_in_redshift(empty_src_tbl_qry,self.whouse_details,logger)



            if to_find in current_folder_file_dict.keys():
                logger.info("Processing the file from Current Folder..")
                # Get the actual file_name to read from the refined -current folder.
                file_to_read = current_folder_file_dict[to_find]
                logger.info("Today's reference file to read: {}".format(file_to_read))

                ref_file_params = utils.get_file_config_params(file_to_read, self.logger)
                structype_schema = utils.convert_dynamodb_json_schema_to_struct_type_schema(ref_file_params["schema"], self.logger)
                # Read the reference file, if the file is present in the current folder.
                today_ref_df = super().read_from_s3(
                    bucket=self.env_params["refined_bucket"],
                    path=current_folder_path + file_to_read,
                    structype_schema=structype_schema
                )
                
               

                logger.info("printing sample records for todays ref file: {}".format(today_ref_df.show(10)))
                # register a temporary table for the today_ref_df
                #today_ref_df.createOrReplaceTempView("today_ref_tbl")
                logger.info(
                    "Today's reference file {} has created successfully".format(
                        file_to_read
                    )
                )
                # Regarding Warehouse tables....
                # CUST -- If reference file arrives today or not, we have to read this whouse table.
                # PRODXREF -- Read this whouse table only when the reference file doesn't arrive today.
                # STYLE -- Read this whouse table only when the reference file doesn't arrive today.
                final_qry = None
                
                logger.info("Whouse table : {}".format(whouse_tbl))
                              
                ref_table = to_find + 'dq'
                logger.info("writing today's ref table to redshift : {}".format(ref_table))
                
                empty_ref_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+ref_table,whouse_schema+whouse_empty_tbl)
                utils.execute_query_in_redshift(empty_ref_tbl_qry,self.whouse_details,logger)

                empty_ref_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+ref_table+self.params["brand"]+'_stage',whouse_schema+whouse_empty_tbl)
                utils.execute_query_in_redshift(empty_ref_tbl_qry,self.whouse_details,logger)

                dynamic_df1 = DynamicFrame.fromDF(today_ref_df, self.glueContext, "dynamic_df1")

                connection_options1 = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": redshift_schema+'.'+ref_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    
                }

                # self.glueContext.write_dynamic_frame.from_options(
                #     frame=dynamic_df,
                #     connection_type = "redshift",                    
                #     connection_options=connection_options,
                #     transformation_ctx="datasink1",
                # )

                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df1,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options1,
                    transformation_ctx="datasink1",
                )

                # transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                #         df=today_ref_df,
                #         redshift_table=ref_table,
                #         # load_mode="append",
                #         load_mode="overwrite",
                #     )
                
                logger.info("writing today's ref table to redshift completed..")

                connection_options2 = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": redshift_schema+'.'+source_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    
                }
                
                
                
                logger.info("writing source table to redshift :{}".format(source_table))
                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df_source,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options2,
                    transformation_ctx="datasink1",
                )


                # transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                #         df=df,
                #         redshift_table=source_table,
                #         # load_mode="append",
                #         load_mode="overwrite",
                #     )
                logger.info("writing source table to redshift completed..")

                if whouse_tbl == "CUST":
                    logger.info("Process Started for CUST")
                    
                    final_qry = "create table {} as ".format(dq_status_table) + qry_file_present.format(
                        source_tbl=whouse_schema+source_table,
                        whouse_ref_tbl=whouse_schema+whouse_tbl,
                        sas_brand_id=sas_brand_id,
                        today_ref_tbl=whouse_schema+ref_table,
                    )
                else:
                    logger.info("Process Started for Others")
                    
                    final_qry = "create table {} as ".format(dq_status_table) + qry_file_present.format(
                        source_tbl=whouse_schema+source_table,
                        today_ref_tbl=whouse_schema+ref_table
                    )

                
                logger.info("Query to execute: {}".format(final_qry))
                drop_tbl_status = utils.execute_query_in_redshift(drop_final_status_table_query,self.whouse_details,logger)
                query_status = utils.execute_query_in_redshift(final_qry,self.whouse_details,logger)
                    
                logger.info(
                        "Query executed successfully!!"
                    )
                dq_status_df = self.redshift_table_to_dataframe(redshift_table=source_table+"_dq_status")
                sql_out = dq_status_df.first()["counts"]
                
                # Deleting the staging tables and dq tables we created.
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+ref_table),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+ref_table+self.params["brand"]+'_stage'),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table+self.params["brand"]+'_stage'),self.whouse_details,logger)
                
                # Execute the respective Query
                # sql_out = spark.sql(final_qry).first()["counts"]
                logger.info("DQ Count : {}".format(sql_out))
                logger.info("Threshold value : {}".format(threshold))
                
                if eval(threshold.format(sql_out=sql_out)):
                    dq_status = False
                    logger.info("Status of DQ - {}".format(dq_status))
                    logger.info("sql_out : {}".format(sql_out))
                else:
                    dq_status = True
                    logger.info("Status of DQ - {}".format(dq_status))

            elif to_find in ref_folder_file_dict.keys():
                logger.info("Processing the file from Refined Folder..")
                # Get the actual file_name to read from the refined -current folder.
                file_to_read = ref_folder_file_dict[to_find]
                # Read the reference file, if the file is present in the current folder.
                ref_file_params = utils.get_file_config_params(file_to_read, self.logger)
                structype_schema = utils.convert_dynamodb_json_schema_to_struct_type_schema(ref_file_params["schema"], self.logger)
                today_ref_df = super().read_from_s3(
                    bucket=self.env_params["transformed_bucket"],
                    path=ref_file_folder_path + file_to_read,
                    structype_schema = structype_schema
                )
                # register a temporary table for the today_ref_df
                #today_ref_df.registerTempTable("today_ref_tbl")
                # Regarding Warehouse tables....
                # CUST -- If reference file arrives today or not, we have to read this whouse table.
                # PRODXREF -- Read this whouse table only when the reference file doesn't arrive today.
                # STYLE -- Read this whouse table only when the reference file doesn't arrive today.
                final_qry = None
                
                ref_table = to_find + 'dq'
                logger.info("writing today's ref table to redshift : {}".format(ref_table))

                empty_ref_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+ref_table,whouse_schema+whouse_empty_tbl)
                utils.execute_query_in_redshift(empty_ref_tbl_qry,self.whouse_details,logger)

                empty_ref_tbl_qry = "create table {} (LIKE {} including defaults)".format(whouse_schema+ref_table+self.params["brand"]+'_stage',whouse_schema+whouse_empty_tbl)
                utils.execute_query_in_redshift(empty_ref_tbl_qry,self.whouse_details,logger)


                dynamic_df1 = DynamicFrame.fromDF(today_ref_df, self.glueContext, "dynamic_df1")

                connection_options1 = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": redshift_schema+'.'+ref_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    
                }

                # self.glueContext.write_dynamic_frame.from_options(
                #     frame=dynamic_df,
                #     connection_type = "redshift",                    
                #     connection_options=connection_options,
                #     transformation_ctx="datasink1",
                # )

                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df1,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options1,
                    transformation_ctx="datasink1",
                )

                # transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                #         df=today_ref_df,
                #         redshift_table=ref_table,
                #         # load_mode="append",
                #         load_mode="overwrite",
                #     )
                
                logger.info("writing today's ref table to redshift completed..")

                connection_options2 = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": redshift_schema+'.'+source_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    
                }
                
                
                
                logger.info("writing source table to redshift :{}".format(source_table))
                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df_source,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options2,
                    transformation_ctx="datasink1",
                )


                # transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                #         df=df,
                #         redshift_table=source_table,
                #         # load_mode="append",
                #         load_mode="overwrite",
                #     )
                logger.info("writing source table to redshift completed..")

                if whouse_tbl == "CUST":
                    logger.info("Process Started for CUST")
                    
                    final_qry = "create table {} as ".format(dq_status_table) + qry_file_present.format(
                        source_tbl=whouse_schema+source_table,
                        whouse_ref_tbl=whouse_schema+whouse_tbl,
                        sas_brand_id=sas_brand_id,
                        today_ref_tbl=whouse_schema+ref_table,
                    )
                else:
                    logger.info("Process Started for Others")
                    
                    final_qry = "create table {} as ".format(dq_status_table) + qry_file_present.format(
                        source_tbl=whouse_schema+source_table,
                        today_ref_tbl=whouse_schema+ref_table
                    )

                
                logger.info("Query to execute: {}".format(final_qry))
                drop_tbl_status = utils.execute_query_in_redshift(drop_final_status_table_query,self.whouse_details,logger)
                query_status = utils.execute_query_in_redshift(final_qry,self.whouse_details,logger)
                    
                logger.info(
                        "Query executed successfully!!"
                    )
                dq_status_df = self.redshift_table_to_dataframe(redshift_table=source_table+"_dq_status")
                sql_out = dq_status_df.first()["counts"]
                
                # Deleting the staging tables and dq tables we created.
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+ref_table),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+ref_table+self.params["brand"]+'_stage'),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table+self.params["brand"]+'_stage'),self.whouse_details,logger)
                
                # Execute the respective Query
                # sql_out = spark.sql(final_qry).first()["counts"]
                logger.info("DQ Count : {}".format(sql_out))
                logger.info("Threshold value : {}".format(threshold))
                
                if eval(threshold.format(sql_out=sql_out)):
                    dq_status = False
                    logger.info("Status of DQ - {}".format(dq_status))
                    logger.info("sql_out : {}".format(sql_out))
                else:
                    dq_status = True
                    logger.info("Status of DQ - {}".format(dq_status))

            else:
                logger.info(
                    "Todays reference file Not Present -- Reading from the whouse files"
                )

                connection_options2 = {
                    "url": redshift_url,
                    "user": username,
                    "password": password,
                    "dbtable": redshift_schema+'.'+source_table,
                    "redshiftTmpDir": temp_bucket,
                    "database": database,
                    
                }
                
                
                
                logger.info("writing source table to redshift :{}".format(source_table))
                self.glueContext.write_dynamic_frame.from_jdbc_conf(
                    frame=dynamic_df_source,
                    catalog_connection=redshift_conn,
                    connection_options=connection_options2,
                    transformation_ctx="datasink1",
                )

                
                # logger.info("writing source table to redshift :{}".format(source_table))
                # transformed_df_to_redshift_table_status = self.write_glue_df_to_redshift(
                #         df=df,
                #         redshift_table=source_table,
                #         # load_mode="append",
                #         load_mode="overwrite",
                #     )
                logger.info("writing source table to redshift completed..")
                
                if whouse_tbl == "PRODUCT_XREF":
                    whouse_ref_df = whouse_tbl.split("_")[0] + whouse_tbl.split("_")[1]
                    
                else:
                    whouse_ref_df = whouse_tbl
                 
                # Execute the respective Query
                
                final_qry = "create table {} as ".format(dq_status_table) + qry_file_not_present.format(
                    source_tbl=whouse_schema+source_table,
                    whouse_ref_tbl=whouse_schema+whouse_ref_df,
                    sas_brand_id=sas_brand_id,
                )
                logger.info("Query to execute : {}".format(final_qry))
                drop_tbl_status = utils.execute_query_in_redshift(drop_final_status_table_query,self.whouse_details,logger)
                query_status = utils.execute_query_in_redshift(final_qry,self.whouse_details,logger)
                    
                logger.info(
                        "Query executed successfully!!"
                    )               

                
                dq_status_df = self.redshift_table_to_dataframe(redshift_table=source_table+"_dq_status")
                sql_out = dq_status_df.first()["counts"]

                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table),self.whouse_details,logger)
                utils.execute_query_in_redshift(del_qry.format(whouse_schema+source_table+self.params["brand"]+'_stage'),self.whouse_details,logger)

                # Execute the respective Query
                # sql_out = spark.sql(final_qry).first()["counts"]
                logger.info("DQ Count : {}".format(sql_out))
                logger.info("Threshold value : {}".format(threshold))
                if eval(threshold.format(sql_out=sql_out)):
                    dq_status = False
                    logger.info("Status of DQ - {}".format(dq_status))
                    logger.info("sql_out : {}".format(sql_out))
                else:
                    dq_status = True
                    logger.info("Status of DQ - {}".format(dq_status))

                

        except Exception as error:
            dq_status = False
            logger.error(
                "Error Ocuured While processiong dq_cust_style_product_missing_cnt due to : {}".format(
                    error
                ),exc_info=True
            )
            raise CustomAppError(moduleName = constant.DQ_CUST_STYLE_PRODUCT_MISSING_CNT, exeptionType = constant.DQ_EXCEPTION, message = "Error Ocuured While processiong dq_cust_style_product_missing_cnt due to : {}".format(traceback.format_exc()))

        return dq_status
