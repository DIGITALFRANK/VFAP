import modules.config.config as config
from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from modules.utils.utils_core import utils
from modules.exceptions.AppUtilsException import AppUtilsError
from modules.constants import constant
import traceback


class MapUtils(Core_Job):
    def __init__(self, file_name):
        """Constructor for MaoUtils

        Arguments:
            WeeklyCore {class} -- Base class with common functionalities
            logger {logger object} -- Logger object to create logs using the logging
            module
            params{dict} -- etl_file_broker configurations
        """
        self.file_name = file_name
        super(MapUtils, self).__init__(file_name)

    # need to chnage parameters depending on source for redshift
    def read_from_source_register_view(self, table, view_name):
        """This function will read from source and create view using pyspark
        Arguments:
            bucket {String} -- source data bucket name
            path {String} -- path for obj to  be read
            view_name {String} -- temp view_name to be created

        Returns:
        """
        try:
            # df = self.read_from_s3(bucket=bucket, path=path)
            df = self.redshift_table_to_dataframe(redshift_table=table)
            # df.cache()
            df.createOrReplaceTempView(view_name)
            self.logger.info("Creating Temp View : {}".format(view_name))
        except Exception as error:
            self.logger.error(
                "Error Ocurred in read_from_source_register_view Due To : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.MAP_UTILS,
                exeptionType=constant.MAP_UTILS_EXCEPTION,
                message="Error Ocurred in read_from_source_register_view Due To : {}".format(
                    traceback.format_exc()
                ),
            )

    def custom_generic_map_customer_id(self, map_params, src_table, map_table):
        """This function will update the customer_id depending on source and map data with respective
        join condition from configuration parameters
        Arguments:
            map_params {dict} -- etl_file_broker config for specific map
            src_bucket {String} -- source data bucket name
            src_path {String} -- path for obj to  be read
            map_bucket {String} --map data bucket name
            map_path {String} -- path for obj to  be read

        Returns:
            [update_status {Boolean}] -- Returns Boolean status after mapping
        """
        update_status = False
        logger = self.logger
        try:
            spark = self.spark

            # read source and create view
            # self.read_from_source_register_view(bucket=src_bucket, path=src_path, view_name=config.MAP_SRC_TEMP_VIEW)
            #            self.read_from_source_register_view(
            #                table=src_table, view_name=config.MAP_SRC_TEMP_VIEW
            #            )

            # read map and create view
            # self.read_from_source_register_view(bucket=map_bucket, path=map_path, view_name=config.MAP_TEMP_VIEW)
            #            self.read_from_source_register_view(
            #                table=map_table, view_name=config.MAP_TEMP_VIEW
            #            )
            drop_query1 = config.MAP_DROP_QUERY1.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info("generic drop table query: {}".format(drop_query1))
            create_query = config.MAP_CREATE_QUERY.format(
                map_params["projectionExpression"],
                src_table,
                map_table,
                map_params["joinExpression"],
                self.whouse_details["dbSchema"],
            )
            logger.info(
                "generic customerId mapping query to create stage table: {}".format(
                    create_query
                )
            )
            update_query = config.MAP_UPDATE_QUERY.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info(
                "generic customerId mapping query to update stage table: {}".format(
                    update_query
                )
            )
            alter_query1 = config.MAP_ALTER_QUERY1.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info("generic alter query to drop column: {}".format(alter_query1))
            alter_query2 = config.MAP_ALTER_QUERY2.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info("generic alter query to rename column: {}".format(alter_query2))
            truncate_query = config.MAP_TRUNCATE_QUERY.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info(
                "generic query to truncate source table: {}".format(truncate_query)
            )
            append_query = config.MAP_APPEND_QUERY.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info(
                "generic query to append stage table records: {}".format(append_query)
            )
            drop_query2 = config.MAP_DROP_QUERY2.format(
                src_table, self.whouse_details["dbSchema"]
            )
            logger.info("generic drop stage table query: {}".format(drop_query2))
            query_status_drop1 = utils.execute_query_in_redshift(
                drop_query1, self.whouse_details, logger
            )
            query_status = utils.execute_query_in_redshift(
                create_query, self.whouse_details, logger
            )
            query_status_update = utils.execute_query_in_redshift(
                update_query, self.whouse_details, logger
            )
            query_status_alter1 = utils.execute_query_in_redshift(
                alter_query1, self.whouse_details, logger
            )
            query_status_alter2 = utils.execute_query_in_redshift(
                alter_query2, self.whouse_details, logger
            )
            query_status_truncate = utils.execute_query_in_redshift(
                truncate_query, self.whouse_details, logger
            )
            query_status_append = utils.execute_query_in_redshift(
                append_query, self.whouse_details, logger
            )
            query_status_drop2 = utils.execute_query_in_redshift(
                drop_query2, self.whouse_details, logger
            )
            #            mapped_df = (
            #                spark.sql(map_query)
            #                .select("customer_id_updated", F.expr("src_tbl.*"))
            #                .drop("customer_id")
            #                .withColumnRenamed("customer_id_updated", "customer_id")
            #            )

            #            logger.info("Mapped customer id results : {}".format(mapped_df.count()))
            #            mapped_df.show(10, truncate=False)
            # mapped_df.select("ORDERS_ID", "EMAIL_ADDRESS", "SAS_BRAND_ID", "CUSTOMER_ID").show(10, truncate=False)
            # super().write_to_tgt()
            #            logger.info("Writing Mapped results to Redshift ")
            # update_status = super().write_to_tgt(
            #     df=mapped_df,
            #     mode="overwrite",
            #     #bucket=self.params["transformed_bucket_name"],
            #     bucket=src_bucket,
            #     path=src_path,
            # )
            #            update_status = super().write_df_to_redshift_table(
            #                df=mapped_df, redshift_table=src_table, load_mode="overwrite"
            #            )
            update_status = query_status_append
        except Exception as error:
            logger.error(
                "Error Occurred In custom_generic_map_customer_id due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            update_status = False
            raise AppUtilsError(
                moduleName=constant.MAP_UTILS,
                exeptionType=constant.MAP_UTILS_EXCEPTION,
                message="Error Ocurred in read_from_source_register_view Due To : {}".format(
                    error.message
                ),
            )
        return update_status

    # CREATE CM_SESSION_XREF
    # need to integrate in seperate xref module and parameters need to be chnaged for redshift
    def create_cm_session_xref(self, src_bucket, src_table, map_table):
        """This function will create session_xref depending on source and map data
        Arguments:
            src_bucket {String} -- source data bucket name
            cm_registration_src_path {String} -- path for cm_registration  data
            map_bucket {String} --map data bucket name
            email_xref_map_path {String} -- path for email_xref data

        Returns:
            [cm_session_xref_status {Boolean}] -- Returns Boolean status after creating xref
        """
        cm_session_xref_status = False
        try:
            spark = self.spark
            params = self.params
            # read cm_registration source data
            self.read_from_source_register_view(
                table=src_table, view_name=config.MAP_CM_REGISTRATION_VIEW,
            )

            # read email_xref data
            self.read_from_source_register_view(
                table=map_table, view_name=config.MAP_EMAIL_XREF_VIEW,
            )

            cm_session_xref_query = config.CM_SESSION_XREF_QUERY
            cm_session_xref_df = spark.sql(cm_session_xref_query)

            print("cm_session_xref : ")
            # cm_session_xref_df.show(10, truncate=False)

            # writing cm_session_xref to target ,need to chnage as per target
            # cm_session_xref_status = self.write_to_tgt(
            #     df=cm_session_xref_df,
            #     bucket=params["transformed_bucket_name"],
            #     path="Map_Custom/cm_session_xref",
            #     mode="overwrite",
            # )
            cm_session_xref_status = self.write_df_to_redshift_table(
                df=cm_session_xref_df, redshift_table=src_table, load_mode="overwrite"
            )

        except Exception as error:
            self.logger.error(
                "Error Ocurred while processing create_cm_session_xref due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            cm_session_xref_status = False
            raise AppUtilsError(
                moduleName=error.moduleName,
                exeptionType=error.exeptionType,
                message="Error Ocurred while processing create_cm_session_xref due to : {}".format(
                    error.message
                ),
            )
        return cm_session_xref_status

    # need to integrate in seperate xref module and parameters need to be chnaged for redshift
    def create_loyalty_xref(self, src_bucket, cust_attribute_src_path):
        """This function will create loyalty_xref
        Arguments:
            src_bucket {String} -- source data bucket name
            cust_attribute_src_path {String} -- path for cust_attribute  data

        Returns:
            [loyalty_xref_status {Boolean}] -- Returns Boolean status after creating xref
        """
        loyalty_xref_status = False
        logger = self.logger
        try:
            spark = self.spark
            params = self.params

            # read cust_attribute data
            self.read_from_source_register_view(
                bucket=src_bucket,
                path=cust_attribute_src_path,
                view_name=config.CUST_ATTRIBUTE_VIEW,
            )

            loyalty_xref_query = config.LOYALTY_XREF_QUERY

            loyalty_xref_df = spark.sql(loyalty_xref_query)

            logger.info("loyalty_xref : ")
            # loyalty_xref_df.show(10, truncate=False)

            # writing loyalty_xref to target ,need to chnage as per target
            loyalty_xref_status = self.write_to_tgt(
                df=loyalty_xref_df,
                bucket=params["transformed_bucket_name"],
                path="Map_Custom/loyalty_xref/",
                mode="append",
            )

        except Exception as error:
            loyalty_xref_status = False
            logger.error(
                "Error Ocurred while processing create_loyalty_xref due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=error.moduleName,
                exeptionType=error.exeptionType,
                message="Error Ocurred while processing create_loyalty_xref due to : {}".format(
                    error.message
                ),
            )

        return loyalty_xref_status

    def create_tnf_responsys_xref(self, src_table, tgt_table):
        """This function will create tnf_responsys_xref which will be used in further mappings
        Arguments:
            src_bucket {String} -- source data bucket name
            tnf_email_sent_view_src_path {String} -- path for cust_attribute  data

        Returns:
            [tnf_responsys_xref_status {Boolean}] -- Returns Boolean status after creating xref
        """
        tnf_responsys_xref_status = False
        logger = self.logger
        try:
            spark = self.spark
            params = self.params

            # read tnf_email_sent_view
            # need to identify source/defination of  tnf_email_sent_view which is combination of tnf_email_sent and tnf_email_sent_hist
            # self.read_from_source_register_view(
            #     table=src_table, view_name=config.TNF_EMAIL_SENT_VIEW,
            # )
            self.whouse_details["dbSchema"]
            tnf_responsys_xref_query = config.TNF_RESPONSYS_XREF_QUERY.format(
                self.whouse_details["dbSchema"], config.TNF_EMAIL_SENT_VIEW
            )
            logger.info("responsys_xref_query : {}".format(tnf_responsys_xref_query))
            tnf_responsys_xref_drop_query = config.TNF_RESPONSYS_XREF_DROP_QUERY.format(
                self.whouse_details["dbSchema"]
            )
            utils.execute_query_in_redshift(
                tnf_responsys_xref_drop_query, self.whouse_details, logger
            )
            utils.execute_query_in_redshift(
                tnf_responsys_xref_query, self.whouse_details, logger
            )
            # tnf_responsys_xref_df = spark.sql(tnf_responsys_xref_query)

            logger.info("tnf_responsys_xref created successfully!!")
            # tnf_responsys_xref_df.show(10, truncate=False)

            # writing tnf_responsys_xref_df to target ,need to chnage as per target
            # tnf_responsys_xref_status = self.write_to_tgt(
            #     df=tnf_responsys_xref_df,
            #     bucket=params["transformed_bucket_name"],
            #     path="Map_Custom/tnf_responsys_xref/",
            #     mode="append",
            # )
            # tnf_responsys_xref_status = self.write_df_to_redshift_table(
            #     df=tnf_responsys_xref_df,
            #     redshift_table=tgt_table,
            #     load_mode="overwrite",
            # )

        except Exception as error:
            tnf_responsys_xref_status = False
            logger.error(
                "Error Ocurred while processing create_tnf_responsys_xref_df due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise AppUtilsError(
                moduleName=constant.MAP_UTILS,
                exeptionType=constant.MAP_UTILS_EXCEPTION,
                message="Error Ocurred while processing create_tnf_responsys_xref_df due to : {}".format(
                    error.message
                ),
            )
        return tnf_responsys_xref_status

    def create_webs_xref(self, src_table, tgt_table):
        """This function will create webs_xref which will be used in further mappings
        Arguments:
            src_bucket {String} -- source data bucket name
            cust_alt_key_src_path {String} -- path for cust_alt_key  data

        Returns:
            [webs_xref_status {Boolean}] -- Returns Boolean status after creating xref
        """
        webs_xref_status = False
        logger = self.logger
        try:
            spark = self.spark
            params = self.params

            self.read_from_source_register_view(
                table=src_table, view_name=config.MAP_ADOBE_TEMP_VIEW,
            )

            webs_xref_query = config.WEBS_XREF_QUERY
            logger.info("webs_xref_query : {}".format(webs_xref_query))

            webs_xref_df = spark.sql(webs_xref_query)

            logger.info("webs_xref : ")
            # need to remove used for testsing
            # webs_xref_df.show(10, truncate=False)
            final_df = webs_xref_df.withColumn("process_dtm", F.current_timestamp())
            webs_xref_status = self.write_glue_df_to_redshift(  # self.write_df_to_redshift_table(
                df=final_df, redshift_table=tgt_table, load_mode=params["write_mode"]
            )

        except Exception as error:
            logger.error(
                "Error Ocurred while processing create_webs_xref due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            webs_xref_status = False
            raise AppUtilsError(
                moduleName=constant.MAP_UTILS,
                exeptionType=constant.MAP_UTILS_EXCEPTION,
                message="Error Ocurred while processing create_webs_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return webs_xref_status

    def create_tnf_adobe_cart_and_prodview_xref_final(
        self, src_table, cart_xref_tgt_table, prodview_xref_tgt_table
    ):
        """This function will create create_tnf_adobe_cart_xref_final which will be used in further mappings
            in visitor_id and Customer_id mapping tables
        Arguments:
            src_bucket {String} -- source data bucket name
            tnf_adobe_productview_abandon_src_path {String} -- path for tnf_adobe_productview_abandon  data

        Returns:
            [tnf_adobe_cart_xref_final_status {Boolean}] -- Returns Boolean status after creating xref
        """
        tnf_adobe_cart_and_prodview_xref_status = False
        logger = self.logger
        try:
            spark = self.spark
            params = self.params

            self.read_from_source_register_view(
                table=src_table,
                view_name=config.MAP_TNF_ADOBE_PRODUCTVIEW_ABANDON_TEMP_VIEW,
            )

            tnf_adobe_cart_and_prodview_xref_final_query = (
                config.MAP_TNF_ADOBE_CART_AND_PRODVIEW_XREF_FINAL_QUERY
            )
            logger.info(
                "tnf_adobe_cart_xref_final_query : {}".format(
                    tnf_adobe_cart_and_prodview_xref_final_query
                )
            )

            tnf_adobe_cart_and_prodview_xref_final_df = spark.sql(
                tnf_adobe_cart_and_prodview_xref_final_query
            )

            logger.info("tnf_adobe_cart_xref_final : ")
            # tnf_adobe_cart_and_prodview_xref_final_df.show(10, truncate=False)
            cart_xref_final_df = tnf_adobe_cart_and_prodview_xref_final_df.withColumn(
                "process_dtm", F.current_timestamp()
            )
            # writing tnf_adobe_cart_and_prodview_xref_final_df to target -> tnf_adobe_cart_xref_final,need to chnage as per target
            tnf_adobe_cart_xref_final_status = self.write_glue_df_to_redshift(
                df=cart_xref_final_df,
                redshift_table=cart_xref_tgt_table,
                load_mode="overwrite",
            )

            tnf_adobe_prodview_xref_final_status = self.write_glue_df_to_redshift(
                df=cart_xref_final_df,
                redshift_table=prodview_xref_tgt_table,
                load_mode="overwrite",
            )

            tnf_adobe_cart_and_prodview_xref_status = (
                tnf_adobe_cart_xref_final_status
                and tnf_adobe_prodview_xref_final_status
            )

        except Exception as error:
            logger.error(
                "Error Ocurred while processing tnf_adobe_cart_xref_final_df due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            tnf_adobe_cart_and_prodview_xref_status = False
            raise AppUtilsError(
                moduleName=constant.MAP_UTILS,
                exeptionType=constant.MAP_UTILS_EXCEPTION,
                message="Error Ocurred while processing tnf_adobe_cart_xref_final_df due to : {}".format(
                    error.message
                ),
            )
        return tnf_adobe_cart_and_prodview_xref_status
