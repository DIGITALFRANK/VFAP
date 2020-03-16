from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from pyspark.sql import functions as f
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback
from modules.utils.utils_core import utils


class map_adobe(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_adobe, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_adobe")
        try:
            params = self.params
            logger = self.logger
            redshift_schema = self.whouse_details["dbSchema"]

            # instantiate map class
            map_utils = MapUtils(file_name=self.file_name)

            # create webs_xref table from cust_alt_key
            # get reference table for customer_id and webs_customer_id
            webs_xref_status = map_utils.create_webs_xref(
                src_table=params["map_params"]["source_target_params"][
                    "tbl_cust_alt_key"
                ],
                tgt_table=params["map_params"]["source_target_params"][
                    "xref_tbl_webs_xref"
                ],
            )
            if webs_xref_status == False:
                raise CustomAppError(
                    moduleName=constant.MAP_ADOBE,
                    exeptionType=constant.MAP_EXCEPTION,
                    message="webs_xref not loaded",
                )
            # assert webs_xref_status, "webs_xref not loaded"

            # TNF_ADOBE_CART_ABANDON
            # adobe_2_cust_id  -- source -> TNF_ADOBE_CART_ABANDON, map ->webs_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_adobe"][
                    "adobe_2_cust_id_for_webs_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "cust_updt_tbl_tnf_adobe_cart_abandon"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_webs_xref"
                ],
            )

            # TNF_ADOBE_PRODUCTVIEW_ABANDON
            # adobe_2_cust_id  -- source -> TNF_ADOBE_CART_ABANDON, map ->webs_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_adobe"][
                    "adobe_2_cust_id_for_webs_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "cust_updt_tbl_tnf_adobe_productview_abandon"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_webs_xref"
                ],
            )

            # Derive SAS_product_id from products TNF_ADOBE_CART_ABANDON
            # updating sas_product_id
            # reading source data -> TNF_ADOBE_CART_ABANDON need to change as per 
            
            update_sas_product_adobe_cart_qry = "update "+redshift_schema+".tnf_adobe_cart_abandon set sas_product_id = REGEXP_REPLACE(substring(trim(products),1,20),'(NF00|NF0A|00|0A|NF){0,}([[:alnum:]]*)(-C1|-G1){0,}', '$2',1) where coalesce(sas_product_id,'-1') <> coalesce(REGEXP_REPLACE(substring(trim(products),1,20),'(NF00|NF0A|00|0A|NF){0,}([[:alnum:]]*)(-C1|-G1){0,}', '$2',1),'-2')"
            
            utils.execute_query_in_redshift(
                update_sas_product_adobe_cart_qry, self.whouse_details, logger
            )
            logger.info("sas_product_id variable updated successfully for the table : TNF_ADOBE_CART_ABANDON")

            
            update_sas_product_adobe_productview_qry = "update "+redshift_schema+".tnf_adobe_productview_abandon set sas_product_id = REGEXP_REPLACE(substring(trim(products),1,20),'(NF00|NF0A|00|0A|NF){0,}([[:alnum:]]*)(-C1|-G1){0,}', '$2',1) where coalesce(sas_product_id,'-1') <> coalesce(REGEXP_REPLACE(substring(trim(products),1,20),'(NF00|NF0A|00|0A|NF){0,}([[:alnum:]]*)(-C1|-G1){0,}', '$2',1),'-2')"
            
            utils.execute_query_in_redshift(
                update_sas_product_adobe_productview_qry, self.whouse_details, logger
            )
            logger.info("sas_product_id variable updated successfully for the table : TNF_ADOBE_PRODUCTVIEW_ABANDON")
            
            create_cart_prodview_xref_final_qry=["drop table {0}.tnf_adobe_cart_xref_final".format(redshift_schema),"""create table {0}.tnf_adobe_cart_xref_final  as  select VISITOR_ID, max(CUSTOMER_ID) as CUSTOMER_ID ,SAS_brand_id from {0}.TNF_ADOBE_PRODUCTVIEW_ABANDON tnf where customer_id is not null group by visitor_id,SAS_brand_id""".format(redshift_schema),"drop table {0}.tnf_adobe_prodview_xref_final".format(redshift_schema),"""create table {0}.tnf_adobe_prodview_xref_final as  
select VISITOR_ID, max(CUSTOMER_ID) as CUSTOMER_ID ,SAS_brand_id from {0}.TNF_ADOBE_PRODUCTVIEW_ABANDON tnf where customer_id is not null group by visitor_id,SAS_brand_id""".format(redshift_schema)]
            
            utils.execute_multiple_queries_in_redshift(
                        create_cart_prodview_xref_final_qry, self.whouse_details, logger
                    )
            
            logger.info("Create Cart_xref & Prodview_xref successfully!!!")
            


            
            # tnf_adobe_cart_abandon_df = self.redshift_table_to_dataframe(
            #     redshift_table=params["map_params"]["source_target_params"][
            #         "cust_updt_tbl_tnf_adobe_cart_abandon"
            #     ]
            # )
            # tnf_adobe_cart_abandon_df = tnf_adobe_cart_abandon_df.withColumn(
            #     "sas_product_id",
            #     f.split(tnf_adobe_cart_abandon_df["products"], "-")
            #     .getItem(0)
            #     .substr(-4, 4),
            # )
            # logger.info("tnf_adobe_cart_abandon_df after sas_product_id")
            # tnf_adobe_cart_abandon_df.show(truncate=False)

            # load data updated with sas_product_id to target ->tnf_adobe_cart_abandon
            # tnf_adobe_cart_abandon_status = self.write_glue_df_to_redshift(
            #     df=tnf_adobe_cart_abandon_df,
            #     redshift_table=params["map_params"]["source_target_params"][
            #         "cust_updt_tbl_tnf_adobe_cart_abandon"
            #     ],
            #     load_mode=self.params["write_mode"],
            # )

            # Derive SAS_product_id from Products PRODUCTVIEW_ABANDON
            # updating sas_product_id
            # reading source data -> TNF_ADOBE_PRODUCTVIEW_ABANDON need to change as per redshift
            # tnf_adobe_productview_abandon_df = self.redshift_table_to_dataframe(
            #     redshift_table=params["map_params"]["source_target_params"][
            #         "cust_updt_tbl_tnf_adobe_productview_abandon"
            #     ]
            # )
            # logger.info("tnf_adobe_productview_abandon_df after sas_product_id")
            # # tnf_adobe_productview_abandon_df.show(truncate=False)
            # tnf_adobe_productview_abandon_df = tnf_adobe_productview_abandon_df.withColumn(
            #     "sas_product_id",
            #     f.split(tnf_adobe_productview_abandon_df["products"], "-")
            #     .getItem(0)
            #     .substr(-4, 4),
            # )

            # load data updated with sas_product_id to target ->tnf_adobe_productview_abandon
            # tnf_adobe_productview_abandon_status = self.write_glue_df_to_redshift(
            #     df=tnf_adobe_productview_abandon_df,
            #     redshift_table=params["map_params"]["source_target_params"][
            #         "cust_updt_tbl_tnf_adobe_productview_abandon"
            #     ],
            #     load_mode=self.params["write_mode"],
            # )

            # create xref for visitor_id and Customer_id mapping tables
            # creating tnf_adobe_cart_xref_final xref and tnf_adobe_prodview_xref_final
            # cart_and_prodview_xref_status = map_utils.create_tnf_adobe_cart_and_prodview_xref_final(
            #     src_table=params["map_params"]["source_target_params"][
            #         "cust_updt_tbl_tnf_adobe_productview_abandon"
            #     ],
            #     cart_xref_tgt_table=params["map_params"]["source_target_params"][
            #         "xref_tbl_tnf_adobe_cart_xref_final"
            #     ],
            #     prodview_xref_tgt_table=params["map_params"]["source_target_params"][
            #         "xref_tbl_tnf_adobe_prodview_xref_final"
            #     ],
            # )

            # assert (cart_and_prodview_xref_status), "Unable To Load tnf_adobe_cart_xref_final and tnf_adobe_prodview_xref_final"
            # if cart_and_prodview_xref_status == False:
            #     raise CustomAppError(
            #         moduleName=constant.MAP_ADOBE,
            #         exeptionType=constant.MAP_EXCEPTION,
            #         message="Unable To Load tnf_adobe_cart_xref_final and tnf_adobe_prodview_xref_final",
            #     )

            # Update the customer_id for same visitor_id
            # adobe_2_cust_id  -- source -> TNF_ADOBE_CART_ABANDON, map ->tnf_adobe_cart_xref_final
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_adobe"][
                    "adobe_2_cust_id_for_visitor_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "cust_updt_tbl_tnf_adobe_productview_abandon"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_tnf_adobe_cart_xref_final"
                ],
            )

            # TNF_ADOBE_PRODUCTVIEW_ABANDON
            # adobe_2_cust_id  -- source -> TNF_ADOBE_CART_ABANDON, map ->tnf_adobe_prodview_xref_final
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_adobe"][
                    "adobe_2_cust_id_for_visitor_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "cust_updt_tbl_tnf_adobe_productview_abandon"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_tnf_adobe_prodview_xref_final"
                ],
            )

            map_status = True
        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_adobe due to : {}".format(error),
                exc_info=True,
            )
            raise CustomAppError(
                moduleName=constant.MAP_ADOBE,
                exeptionType=constant.MAP_EXCEPTION,
                message="Error Ocuured While processiong map_adobe due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return map_status
