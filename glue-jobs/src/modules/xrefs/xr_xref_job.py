from pyspark.sql import functions as F
from pyspark.sql import Row
from modules.config import xref_global_config
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from modules.config import config
from modules.core.core_job import Core_Job
from modules.exceptions.CustomAppException import CustomAppError
from modules.constants import constant
import traceback
from modules.utils.utils_core import utils
from modules.xrefs.schemas import schema_style_xref_clean, style_xref_schema_1


class Xref_Job(Core_Job):
    """This is a class for the weekly Xref Job that runs on a weekly basis.
    Methods of this class represents every equivalent xref jobs in SAS.

    Arguments:
        Core_Job {class} -- Base class for the module
    """

    global hop
    global original_cust_no

    def __init__(self, file_name):
        """Default Constructor for Xref_Job class
        """
        super(Xref_Job, self).__init__(file_name)
        self.file_name = file_name
        # sc = SparkContext.getOrCreate()
        # glueContext = GlueContext(sc)
        # # *TODO - Get Key for AWS Param store from config_store.ini for every env
        # self.key = utils.get_parameter_store_key()
        # print("Found parameter store key as : {}".format(self.key))
        # self.env_params = utils.get_param_store_configs(self.key)
        # logger = get_logger()
        # self.logger = logger
        # self.spark = glueContext.spark_session

    @staticmethod
    def clean_tnf_style_xref(row):
        try:
            sas_product_category = row.tnf___clm_description_for_sas
            clm_description_for_sas = row.tnf___clm_description_for_sas
            sas_style_description = row.style_description.strip()
            product_gender = None
            product_age_group = None

            for key in xref_global_config.clean_tnf_xref_basic.keys():
                if key.startswith("TNF___CLM_DESCRIPTION_FOR_SAS_startswith_"):
                    if (
                        row.tnf___clm_description_for_sas[0:2]
                        == xref_global_config.clean_tnf_xref_basic[key]["condition"]
                    ):
                        product_gender = xref_global_config.clean_tnf_xref_basic[key][
                            "result"
                        ]["gender"]
                        product_age_group = xref_global_config.clean_tnf_xref_basic[
                            key
                        ]["result"]["age_group"]
                        sas_product_category = sas_product_category[
                            xref_global_config.clean_tnf_xref_basic[key]["result"][
                                "product_category_index"
                            ] :
                        ]
                    else:
                        pass
                if key.startswith("TNF___CLM_DESCRIPTION_FOR_SAS_indexof_"):
                    try:
                        if (
                            sas_product_category.index(
                                xref_global_config.clean_tnf_xref_basic[key][
                                    "condition"
                                ]
                            )
                            >= 0
                        ):
                            sas_product_category = sas_product_category.replace(
                                xref_global_config.clean_tnf_xref_basic[key]["result"][
                                    "product_category_replace"
                                ][0],
                                xref_global_config.clean_tnf_xref_basic[key]["result"][
                                    "product_category_replace"
                                ][1],
                            )
                            product_gender = xref_global_config.clean_tnf_xref_basic[
                                key
                            ]["result"]["gender"]
                            product_age_group = xref_global_config.clean_tnf_xref_basic[
                                key
                            ]["result"]["age_group"]
                        else:
                            pass
                    except Exception as error:
                        print("Error encountered while running xref {}".format(error))
                if key.startswith("STYLE_DESCRIPTION_"):
                    position = row.style_description.find(
                        xref_global_config.clean_tnf_xref_basic[key]["substring"]
                    )
                    if (
                        row.style_description.find(
                            " "
                            + xref_global_config.clean_tnf_xref_basic[key]["substring"]
                        )
                        < 0
                        and position >= 0
                    ):
                        if xref_global_config.clean_tnf_xref_basic[key]["concatenate"][
                            "end_part"
                        ]:
                            sas_style_description = (
                                sas_style_description[0:position]
                                + " "
                                + xref_global_config.clean_tnf_xref_basic[key][
                                    "substring"
                                ]
                                + sas_style_description[
                                    position
                                    + 1
                                    + len(
                                        xref_global_config.clean_tnf_xref_basic[key][
                                            "substring"
                                        ]
                                    ) :
                                ]
                            )
                        else:
                            sas_style_description = (
                                sas_style_description[0:position]
                                + " "
                                + xref_global_config.clean_tnf_xref_basic[key][
                                    "substring"
                                ]
                            )
                if key.startswith("JKT"):
                    position = sas_style_description.find(
                        xref_global_config.clean_tnf_xref_basic[key]["substring"]
                    )
                    if position >= 0:
                        sas_style_description = (
                            sas_style_description[0:position]
                            + xref_global_config.clean_tnf_xref_basic[key][
                                "replace_with"
                            ]
                            + sas_style_description[
                                position
                                + len(
                                    xref_global_config.clean_tnf_xref_basic[key][
                                        "substring"
                                    ]
                                ) :
                            ]
                        )
                if key.startswith("SWEATS"):
                    pos1 = sas_style_description.find(
                        xref_global_config.clean_tnf_xref_basic[key]["substring1"]
                    )
                    pos2 = sas_style_description.find(
                        xref_global_config.clean_tnf_xref_basic[key]["substring2"]
                    )
                    if pos1 >= 0 and pos2 < 0:
                        sas_style_description = (
                            sas_style_description[0:pos1]
                            + xref_global_config.clean_tnf_xref_basic[key][
                                "replace_with"
                            ]
                        )
                if key.startswith("TRIJACKET"):
                    position = sas_style_description.find(
                        xref_global_config.clean_tnf_xref_basic[key]["substring"]
                    )
                    if position >= 0:
                        sas_style_description = (
                            sas_style_description[0:position]
                            + " "
                            + xref_global_config.clean_tnf_xref_basic[key][
                                "replace_with"
                            ]
                            + sas_style_description[
                                position
                                + 1
                                + len(
                                    xref_global_config.clean_tnf_xref_basic[key][
                                        "substring"
                                    ]
                                ) :
                            ]
                        )

                if key.startswith("RTO"):
                    for search_term in xref_global_config.clean_tnf_xref_basic[key][
                        "substring"
                    ]:
                        position = sas_style_description.find(search_term)
                        if position >= 0:
                            sas_style_description = (
                                sas_style_description[0:position]
                                + " "
                                + xref_global_config.clean_tnf_xref_basic[key][
                                    "replace_with"
                                ]
                                + sas_style_description[
                                    position
                                    + 1
                                    + len(
                                        xref_global_config.clean_tnf_xref_basic[key][
                                            "substring"
                                        ]
                                    ) :
                                ]
                            )

                if key.startswith("LIST"):
                    if key in ["LIST_JACKET", "LIST_TRICLIMATE","LIST_THERMOBALL","LIST_TODDLER","LIST_INSULATED"]:
                        for search_term in xref_global_config.clean_tnf_xref_basic[key][
                            "substring"
                        ]:
                            position = sas_style_description.find(search_term)
                            if position >= 0:
                                sas_style_description = sas_style_description.replace(
                                    search_term,
                                    xref_global_config.clean_tnf_xref_basic[key][
                                        "replace_with"
                                    ],
                                )
                                break
                    else:
                        for search_term in xref_global_config.clean_tnf_xref_basic[key][
                            "substring"
                        ]:
                            position = sas_style_description.find(search_term)
                            if position >= 0:
                                sas_style_description = sas_style_description.replace(
                                    search_term,
                                    xref_global_config.clean_tnf_xref_basic[key][
                                        "replace_with"
                                    ],
                                )

                if key.startswith("GLOVES"):
                    for search_term in xref_global_config.clean_tnf_xref_basic[key][
                        "substring"
                    ]:
                        pos_style = sas_style_description.find(search_term)
                        pos_sas_product = row.tnf___clm_description_for_sas.find(
                            search_term
                        )
                        pos_sas_clm_desc = row.tnf___clm_description_for_sas.find(
                            search_term
                        )
                        if pos_style >= 0:
                            sas_style_description = sas_style_description.replace(
                                search_term,
                                xref_global_config.clean_tnf_xref_basic[key][
                                    "replace_with"
                                ],
                            )
                        if pos_sas_product >= 0:
                            sas_product_category = row.tnf___clm_description_for_sas.replace(
                                search_term,
                                xref_global_config.clean_tnf_xref_basic[key][
                                    "replace_with"
                                ],
                            )
                        if pos_sas_clm_desc >= 0:
                            sas_style_description = row.tnf___clm_description_for_sas.replace(
                                search_term,
                                xref_global_config.clean_tnf_xref_basic[key][
                                    "replace_with"
                                ],
                            )
                            clm_description_for_sas = row.tnf___clm_description_for_sas.replace(
                                search_term,
                                xref_global_config.clean_tnf_xref_basic[key][
                                    "replace_with"
                                ],
                            )

            return (
                # "department_code":
                row.department_code,
                # "department_description":
                row.department_description,
                # "class_code":
                row.class_code,
                # "class_description":
                row.class_description,
                # "style_id":
                row.style_id,
                # "vendor_style":
                row.vendor_style,
                # "style_description":
                row.style_description,
                # "vendor_cd_4":
                row.vendor_cd_4,
                # "vendor_cd_6":
                row.vendor_cd_6,
                # "cnodeid":
                row.cnodeid,
                # "style_description_short":
                row.style_description_short,
                # "style_description_long":
                row.style_description_long,
                # "product_line":
                row.product_line,
                # "sbu":
                row.sbu,
                # "series":
                row.series,
                # "newconsumersegmentterritory":
                row.newconsumersegmentterritory,
                # "newendusesport":
                row.newendusesport,
                # "newproductcollectionfamily":
                row.newproductcollectionfamily,
                # "tnf___clm_description_for_sas":
                clm_description_for_sas,
                # "tnf_sas_product_category":
                sas_product_category,
                # "product_gender":
                product_gender,
                # "product_age_group":
                product_age_group,
                # "sas_style_description":
                sas_style_description,
            )
        except Exception as error:
            print("Error encountered while running xref {}".format(error))
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running clean_tnf_style_xref : {}".format(
                    traceback.format_exc()
                ),
            )

    # Method to call from redshift
    @staticmethod
    def tnf_get_vendor_style(df, sel, spark):
        try:
            # sqlContext.registerDataFrameAsTable(df=df,tableName="temp")
            df.createOrReplaceTempView("temp")
            query = "SELECT DISTINCT " + sel + " FROM temp"
            list_vendor_styles = (
                spark.sql(query).select(sel).rdd.map(lambda x: x[sel]).collect()
            )
        except Exception as error:
            print("Error encountered while running xref {}".format(error))
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running tnf_get_vendor_style {}".format(
                    traceback.format_exc()
                ),
            )
        return list_vendor_styles

    @staticmethod
    def tnf_update_style_xref(
        df,
        THERMOBALL_STYLE,
        SUMMIT_STYLE_IDS_NEW,
        FARNORTHERN_STYLE,
        SNOWSPORTS_STYLE,
        RAINWEAR_STYLE,
        TNF_FOOTWEAR_STYLE_ID,
        spark,
    ):
        try:
            new_df = (
                df.withColumn(
                    "TB_FLAG",
                    F.when(
                        df["VENDOR_STYLE"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                THERMOBALL_STYLE, "VENDOR_STYLE", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "SM_FLAG",
                    F.when(
                        df["VENDOR_STYLE"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                SUMMIT_STYLE_IDS_NEW, "VENDOR_STYLE", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "FN_FLAG",
                    F.when(
                        df["VENDOR_STYLE"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                FARNORTHERN_STYLE, "VENDOR_STYLE", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "SS_FLAG",
                    F.when(
                        df["VENDOR_STYLE"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                SNOWSPORTS_STYLE, "VENDOR_STYLE", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "RW_FLAG",
                    F.when(
                        df["VENDOR_STYLE"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                RAINWEAR_STYLE, "VENDOR_STYLE", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "FW_FLAG",
                    F.when(
                        df["VENDOR_CD_4"].isin(
                            Xref_Job.tnf_get_vendor_style(
                                TNF_FOOTWEAR_STYLE_ID, "VENDOR_CD_4", spark
                            )
                        ),
                        1,
                    ).otherwise(0),
                )
                .withColumn(
                    "bcpck_flag", F.when(df.department_code == 220, 1).otherwise(0)
                )
            )
            new_df = new_df.withColumn(
                "tnf_sas_product_category",
                F.when(
                    new_df.tnf_sas_product_category == "SLEEPING_BAG_CLM_SPRT",
                    "SLEEP_BAG_CLM_SPRT",
                )
                .when(new_df.tnf_sas_product_category == "SNW_SPRT", "SNW_SPT")
                .when(
                    new_df.tnf_sas_product_category == "GLVSCLM_SPRT", "GLOVES_CLM_SPRT"
                )
                .when(
                    new_df.tnf_sas_product_category == "GLOVES_CLIMBING_SPRT",
                    "GLOVES_CLM_SPRT",
                )
                .when(new_df.tnf_sas_product_category == "GLVOES_CUL", "GLOVES_CUL")
                .otherwise(new_df.tnf_sas_product_category),
            )
            return new_df
        except Exception as error:
            print("Error encountered while running xref {}".format(error))
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running tnf_update_style_xref {}".format(
                    traceback.format_exc()
                ),
            )

    @staticmethod
    def clean_tnf_xref_final(row):
        try:
            gender_age = (
                row.product_gender.strip() + "_" + row.product_age_group.strip()
            )
            sas_product_category = row.tnf_sas_product_category
            newendusesport = row.newendusesport
            vendor_cd_4 = row.vendor_cd_4
            # NEWENDUSESPORT = None
            TRGT_MA_FLAG = 0
            TRGT_MS_FLAG = 0
            TRGT_ML_FLAG = 0
            TRGT_UE_FLAG = 0

            for key in xref_global_config.clean_tnf_xref_final.keys():
                if key.startswith("VENDER_CD"):
                    if (
                        row.vendor_cd_4
                        in xref_global_config.clean_tnf_xref_final[key]["substring"]
                    ):
                        sas_product_category = xref_global_config.clean_tnf_xref_final[
                            key
                        ]["result"]
                if key.endswith("product_category"):
                    for item in xref_global_config.clean_tnf_xref_final[key][
                        "replacements"
                    ]:
                        replace_with = xref_global_config.clean_tnf_xref_final[key][
                            "replacements"
                        ][item]
                        sas_product_category = sas_product_category.rstrip().replace(
                            item, replace_with
                        )

                if key.endswith("new_end_use_sport"):
                    for item in xref_global_config.clean_tnf_xref_final[key][
                        "replacements"
                    ]:
                        replace_with = xref_global_config.clean_tnf_xref_final[key][
                            "replacements"
                        ][item]
                        newendusesport = newendusesport.rstrip().replace(
                            item, replace_with
                        )
                elif key.startswith("condition"):
                    if (
                        row[xref_global_config.clean_tnf_xref_final[key]["substring"]]
                        .strip()
                        .upper()
                        == xref_global_config.clean_tnf_xref_final[key]["equality"][0]
                    ):
                        TRGT_MA_FLAG = 1
                    elif (
                        row[xref_global_config.clean_tnf_xref_final[key]["substring"]]
                        .strip()
                        .upper()
                        == xref_global_config.clean_tnf_xref_final[key]["equality"][1]
                    ):
                        TRGT_MS_FLAG = 1
                    elif (
                        row[xref_global_config.clean_tnf_xref_final[key]["substring"]]
                        .strip()
                        .upper()
                        in xref_global_config.clean_tnf_xref_final[key]["equality"][2]
                    ):
                        TRGT_ML_FLAG = 1
                    elif (
                        row[xref_global_config.clean_tnf_xref_final[key]["substring"]]
                        .strip()
                        .upper()
                        == xref_global_config.clean_tnf_xref_final[key]["equality"][3]
                    ):
                        TRGT_UE_FLAG = 1

            return (
                # "department_code":
                row.department_code,
                # "department_description":
                row.department_description,
                # "class_code":
                row.class_code,
                # "class_description":
                row.class_description,
                # "style_id":
                row.style_id,
                # "vendor_style":
                row.vendor_style,
                # "style_description":
                row.style_description,
                # "vendor_cd_4":
                vendor_cd_4,
                # "vendor_cd_6":
                row.vendor_cd_6,
                # "cnodeid":
                row.cnodeid,
                # "style_description_short":
                row.style_description_short,
                # "style_description_long":
                row.style_description_long,
                # "product_line":
                row.product_line,
                # "sbu":
                row.sbu,
                # "series":
                row.series,
                # "newconsumersegmentterritory":
                row.newconsumersegmentterritory,
                # "newendusesport":
                newendusesport,
                # "newproductcollectionfamily":
                row.newproductcollectionfamily,
                # "tnf___clm_description_for_sas":
                row.tnf___clm_description_for_sas,
                # "tnf_sas_product_category":
                sas_product_category.replace(" ", "_"),
                # "product_gender":
                row.product_gender,
                # "product_age_group":
                row.product_age_group,
                # "sas_style_description":
                row.sas_style_description,
                # "TRGT_MA_FLAG":
                TRGT_MA_FLAG,
                # "TRGT_MS_FLAG":
                TRGT_MS_FLAG,
                # "TRGT_ML_FLAG":
                TRGT_ML_FLAG,
                # "TRGT_UE_FLAG":
                TRGT_UE_FLAG,
                # "gender_age":
                gender_age,
            )
        except Exception as error:
            print("Error encountered while running xref {}".format(error))
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running clean_tnf_xref_final {}".format(
                    traceback.format_exc()
                ),
            )

    def xr_build_style_xref_clean_tnf(self):
        spark = self.spark
        # env_params = self.env_params
        logger = self.logger
        params = self.params
        print("Parameters from base job is {}".format(params))
        try:
            # sqlContext = SQLContext(self.sc)
            # response=DynamoUtils.get_dndb_item("feed_name", "tnf_style_xref_clean", env_params["config_table"], sort_key_attr=None, sort_key_value=None, logger=logger)
            # print("DDB response is {}".format(response))
            response = self.params

            # (
            #     TNF_STYLE_XREF,
            #     THERMOBALL_STYLE,
            #     FARNORTHERN_STYLE,
            #     SNOWSPORTS_STYLE,
            #     RAINWEAR_STYLE,
            #     SUMMIT_STYLE_IDS_NEW,
            #     TNF_FOOTWEAR_STYLE_ID,
            # ) = get_redshift_tables(spark)
            TNF_STYLE_XREF = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_style_xref"]
            )
            TNF_STYLE_XREF = TNF_STYLE_XREF.drop("LONG_NAME", "SHORT_NAME")
            logger.info("showig sample records..")
            TNF_STYLE_XREF.show(truncate=False)
            THERMOBALL_STYLE = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_thermoball_style"]
            )
            FARNORTHERN_STYLE = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_farnorthern_style"]
            )
            SNOWSPORTS_STYLE = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_snowsports_style"]
            )
            RAINWEAR_STYLE = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_rainwear_style"]
            )
            SUMMIT_STYLE_IDS_NEW = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"][
                    "xref_input_summit_style_ids_new"
                ]
            )
            TNF_FOOTWEAR_STYLE_ID = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"][
                    "xref_input_tnf_footwear_style_id"
                ]
            )
            rdd1 = []
            for x in TNF_STYLE_XREF.collect():
                rdd1.append(Xref_Job.clean_tnf_style_xref(x))
            # rdd1 = TNF_STYLE_XREF.rdd.map(lambda x: Xref_Job.clean_tnf_style_xref(x))
            style_xref_clean = spark.createDataFrame(rdd1, schema=style_xref_schema_1)
            # logger.info("style_xref_clean is {}".format(style_xref_clean.show()))
            # Replace Null value with 'B'
            style_xref_clean = style_xref_clean.na.fill(
                {"product_gender": "B", "product_age_group": "B"}
            )
            # rdd2 = style_xref_clean.rdd.map(
            #     lambda x: Row(**Xref_Job.clean_tnf_xref_final(x))
            # )
            rdd2 = []
            for x in style_xref_clean.collect():
                rdd2.append(Xref_Job.clean_tnf_xref_final(x))

            style_xref_clean = spark.createDataFrame(
                rdd2, schema=schema_style_xref_clean
            )
            df = Xref_Job.tnf_update_style_xref(
                style_xref_clean,
                THERMOBALL_STYLE,
                SUMMIT_STYLE_IDS_NEW,
                FARNORTHERN_STYLE,
                SNOWSPORTS_STYLE,
                RAINWEAR_STYLE,
                TNF_FOOTWEAR_STYLE_ID,
                spark,
            )
            # *TODO-Write to S3
            # write_status = self.write_df_to_redshift_table(
            #     df=df,
            #     redshift_table=response["xref_params"]["xref_output"],
            #     load_mode=response["xref_params"]["write_mode"],
            # )
            new_df = df.withColumn("process_dtm", F.current_timestamp())
            logger.info("new_df sample records : {}".format(new_df.show(20)))
            write_status = self.write_glue_df_to_redshift(
                df=new_df,
                redshift_table=response["xref_params"]["xref_output"],
                load_mode=response["xref_params"]["write_mode"],
            )
        except Exception as error:
            write_status = False
            logger.error(
                "Error encountered while running xref {}".format(error), exc_info=1
            )
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running clean_tnf_xref_final {}".format(
                    traceback.format_exc()
                ),
            )

        return write_status

    # need to change parameters depending on source for redshift
    def read_from_source_register_view(self, table, view_name):
        """This function will read from source and create view using pyspark
        Arguments:
            bucket {String} -- source data bucket name
            path {String} -- path for obj to  be read
            view_name {String} -- temp view_name to be created

        Returns:
        """
        df = None
        logger = self.logger
        try:
            params = {}
            params["raw_source_file_delimiter"] = ","
            self.params = params
            # df = self.read_from_s3(bucket=bucket, path=path)
            df = self.redshift_table_to_dataframe(redshift_table=table)
            # df.cache()
            df.createOrReplaceTempView(view_name)
            logger.info("Creating Temp View : {}".format(view_name))
        except Exception as error:
            df = None
            print(
                "Error Ocurred in read_from_source_register_view Due To : {}".format(
                    error
                )
            )
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocurred in read_from_source_register_view Due To : {}".format(
                    traceback.format_exc()
                ),
            )

        return df

    def xr_create_cm_session_xref(self):
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
        logger = self.logger
        try:
            # response=DynamoUtils.get_dndb_item("feed_name", "cm_session_xref", env_params["config_table"], sort_key_attr=None, sort_key_value=None, logger=logger)
            # print("DDB response is {}".format(response))
            response = self.params
            spark = self.spark
            # read cm_registration source data
            cm_reg_view = self.read_from_source_register_view(
                table=response["xref_params"]["xref_input_cm_registration"],
                view_name=config.MAP_CM_REGISTRATION_VIEW,
            )
            print("CoreMetrics : ")
            # read email_xref data
            email_view = self.read_from_source_register_view(
                table=response["xref_params"]["xref_input_email"],
                view_name=config.MAP_EMAIL_XREF_VIEW,
            )
            cm_reg_view.createOrReplaceTempView(config.MAP_CM_REGISTRATION_VIEW)
            email_view.createOrReplaceTempView(config.MAP_EMAIL_XREF_VIEW)

            cm_session_xref_query = config.CM_SESSION_XREF_QUERY
            cm_session_xref_df = spark.sql(cm_session_xref_query)
            # print(cm_session_xref_df.show())

            # print("cm_session_xref : ", cm_reg_view.show())
            # print("email_xref : ", email_view.show())
            # cm_session_xref_df.show(10, truncate=False)

            # writing cm_session_xref to target ,need to chnage as per target
            # cm_session_xref_status = self.write_to_tgt(
            #     df=cm_session_xref_df,
            #     bucket="vf-datalake-dev-transformed",
            #     path="Map_Custom/cm_session_xref",
            #     mode="overwrite",
            # )
            # cm_session_xref_status = self.write_df_to_redshift_table(
            #     df=cm_session_xref_df,
            #     redshift_table=response["xref_params"]["xref_output"],
            #     load_mode=response["xref_params"]["write_mode"],
            # )
            final_df = cm_session_xref_df.withColumn(
                "process_dtm", F.current_timestamp()
            )
            cm_session_xref_status = self.write_glue_df_to_redshift(
                df=final_df,
                redshift_table=response["xref_params"]["xref_output"],
                load_mode=response["xref_params"]["write_mode"],
            )

        except Exception as error:
            logger.error(
                "Error Ocurred while processing \
                    create_cm_session_xref due to : {}".format(
                    error
                ),
                exc_info=1,
            )
            cm_session_xref_status = False
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocurred while processing create_cm_session_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return cm_session_xref_status

    def xr_create_loyalty_xref(self):
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
            # read cust_attribute data
            # response=DynamoUtils.get_dndb_item("feed_name", "cm_session_xref", env_params["config_table"], sort_key_attr=None, sort_key_value=None, logger=logger)
            # print("DDB response is {}".format(response))
            response = self.params
            self.read_from_source_register_view(
                table=response["xref_params"]["xref_input_cust_attribute"],
                view_name=config.CUST_ATTRIBUTE_VIEW,
            )

            loyalty_xref_query = config.LOYALTY_XREF_QUERY

            loyalty_xref_df = spark.sql(loyalty_xref_query)

            logger.info("loyalty_xref : ")
            loyalty_xref_df.show(10, truncate=False)

            # writing loyalty_xref to target ,need to chnage as per target
            # loyalty_xref_status = self.write_to_tgt(
            #     df=loyalty_xref_df,
            #     bucket="vf-datalake-dev-transformed",
            #     path="Map_Custom/loyalty_xref/",
            #     mode="overwrite",
            # )
            final_df = loyalty_xref_df.withColumn("process_dtm", F.current_timestamp())
            loyalty_xref_status = self.write_glue_df_to_redshift(
                df=final_df,
                redshift_table=response["xref_params"]["xref_output"],
                load_mode=response["xref_params"]["write_mode"],
            )

        except Exception as error:
            loyalty_xref_status = False
            logger.error(
                "Error Ocurred while processing create_loyalty_xref due to : {}".format(
                    error, exc_info=1
                )
            )
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocurred while processing create_loyalty_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return loyalty_xref_status

    def xr_email_xref_old(self):
        full_load_df = None
        spark = self.spark
        logger = self.logger
        logger.info("Applying tr_email_xref")
        try:
            response = self.params
            df = self.redshift_table_to_dataframe(
                redshift_table=response["xref_params"]["xref_input_cust"]
            )
            refined_df = df
            refined_df.createOrReplaceTempView("cust_vw")
            refined_df.printSchema()
            full_load_df = spark.sql(
                """ SELECT 
                        trim(lower(uniq.email_address)) as email_address,
                        mstr.customer_id,
                        uniq.sas_brand_id
                        FROM cust_vw mstr
                        INNER JOIN 
                        (SELECT 
                        email_address, 
                        sas_brand_id 
                        FROM cust_vw 
                        WHERE email_address IS NOT NULL 
                        GROUP BY email_address , sas_brand_id  
                        HAVING COUNT(DISTINCT customer_id) = 1
                        ) uniq 
                        ON mstr.email_address = uniq.email_address
                        AND mstr.sas_brand_id = uniq.sas_brand_id
                        """
            )
            full_load_df.show(20)
            # logger.info(
            #     "count of records in email_xref table is {}".format(
            #         full_load_df.count()
            #     )
            # )
            # status = self.write_to_tgt(
            #     df=full_load_df,
            #     bucket="vf-datalake-dev-transformed",
            #     path="Map_Custom/email_xref/",
            #     mode="append",
            # )
            logger.debug("*************Number of records to write**********")
            logger.debug(full_load_df.count())
            final_df = full_load_df.withColumn("process_dtm", F.current_timestamp())
            status = self.write_glue_df_to_redshift(
                df=final_df,
                redshift_table=response["xref_params"]["xref_output"],
                load_mode=response["xref_params"]["write_mode"],
            )
            # status = self.write_df_to_redshift_table(
            #     df=full_load_df,
            #     redshift_table=response["xref_params"]["xref_output"],
            #     load_mode=response["xref_params"]["write_mode"],
            # )
        except Exception as error:
            full_load_df = None
            logger.error(
                "Error Ocuured While processiong email_xref due to : {}".format(error),
                exc_info=True,
            )
            status = False
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocuured While processiong email_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return status

    def xr_email_xref(self):
        full_load_df = None
        spark = self.spark
        logger = self.logger
        logger.info("Applying tr_email_xref")
        try:
            response = self.params
            email_xref_qry = "Truncate table {0}.email_xref".format(
                self.whouse_details["dbSchema"]
            )

            email_xref_create_qry = """Insert into {0}.email_xref  SELECT 
                        trim(lower(uniq.email_address)) as email_address,
                        mstr.customer_id,
                        uniq.sas_brand_id,
                        current_timestamp as process_dtm
                        FROM {0}.cust mstr
                        INNER JOIN 
                        (SELECT 
                        email_address, 
                        sas_brand_id 
                        FROM {0}.cust 
                        WHERE email_address IS NOT NULL 
                        GROUP BY email_address , sas_brand_id  
                        HAVING COUNT(DISTINCT customer_id) = 1
                        ) uniq 
                        ON mstr.email_address = uniq.email_address
                        AND mstr.sas_brand_id = uniq.sas_brand_id
                        """.format(
                self.whouse_details["dbSchema"]
            )

            utils.execute_query_in_redshift(email_xref_qry, self.whouse_details, logger)
            logger.info("Email_Xref created successfully!!!")

            utils.execute_query_in_redshift(
                email_xref_create_qry, self.whouse_details, logger
            )
            logger.info("create_email_Xref created successfully!!!")

            status = True

        except Exception as error:
            full_load_df = None
            logger.error(
                "Error Ocuured While processiong email_xref due to : {}".format(error),
                exc_info=True,
            )
            status = False
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocuured While processiong email_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return status

    def xr_build_style_xref_clean_vans(self):
        """
        Builds the tr_clean_vans_xref table for VANS

        :param df:
        :return:
        """
        print(" Applying tr_clean_vans_xref ")
        spark = self.spark
        logger = self.logger
        dbschema = self.whouse_details["dbSchema"]
        # env_params = self.env_params
        response = self.params
        # response=DynamoUtils.get_dndb_item("feed_name", "vans_clean_xref", env_params["config_table"], sort_key_attr=None, sort_key_value=None, logger=logger)
        # print("DDB response is {}".format(response))
        full_load_df = None
        try:
            print("enter into try block")

            drop_clean_stage_tables=["drop table if exists {0}.prodxref_clean_1".format(dbschema),"drop table if exists {0}.prodxref_clean_2".format(dbschema),
            "drop table if exists {0}.mte".format(dbschema)]
            utils.execute_multiple_queries_in_redshift(drop_clean_stage_tables, self.whouse_details, logger)

            prodxref_clean_query1 = """create table {0}.prodxref_clean_1 as
            SELECT *,
            CASE 
            WHEN class_code IN (1108,1120,1008,1020,1107,
            4403,4000,4003,4100,4201,4203,4300,4400,4401,4402,4500,4504,4002,4004, 4200,4501,4502,4503, 4600,4602,5002,5000,5001,5003,5004,5005,5101) THEN 'VANS_AS_SKATE_U'
            WHEN class_code = 1208 THEN 'VANS_AS_SKATE_W'
            WHEN class_code IN (1007,1012) THEN 'VANS_AS_SKATE_M'
            WHEN class_code IN (1308,1320) THEN 'VANS_AS_SKATE_Y'
            WHEN class_code IN (1030,1130,1230,3600,3605,3610,3615,3620,3625,3630,3635,1109,1112) THEN 'VANS_AS_SNOW'
            WHEN class_code IN (1006,1209,1106,1306) THEN 'VANS_AS_SURF'
            WHEN class_code IN (1009,1010,1013,1014,1015) THEN 'VANS_FL_FT_M'
            WHEN class_code IN (1110,1113,1114,1115) THEN 'VANS_FL_FT_U'
            WHEN class_code IN (1206,1207,1210,1214) THEN 'VANS_FL_FT_W'
            WHEN class_code IN (1310,1410,1406,1408,1506,1507,1509,1510,1512,1606,1608,1610,1415) THEN 'VANS_FL_FT_Y'
            WHEN class_code IN (2100,2104,2105,2106,2107,2108,2301,2302,2304,2306,2350,2901,2902,3700,3705,3710,3715,3720,3725,3730,3735,3740,3745,3750,3755,3760,3765,3770,3775,3799,9920) AND
            charindex('3700000010911',style_aka) = 0 AND
            charindex('3705000011018',style_aka) = 0 AND
            charindex('3710000011510',style_aka) = 0 THEN 'VANS_FL_AP_M' 
            WHEN class_code IN (2102,3800,3805,3810,3815,3820,3825,3830,3835,3840,3845,3850,3855,3860,3865,3868,3870,3875,3880) THEN 'VANS_FL_AP_W'
            WHEN class_code IN (2110,3900,3905,3910,3915,3920,3925,3930,3935,3940,3945,3955,3960,3965,3970) THEN 'VANS_FL_AP_YB'
            WHEN class_code IN (2005,2006,2007,2207,2208,2210,2212,2409,2410,2495,2509,3788,2930,2508,2411,2408,2404,2213,2209,2510,2865) THEN 'VANS_FL_AC_M'
            WHEN class_code IN (2950,2951,2952,2953,2956,2957,2958,2009,2954,2855,2870) THEN 'VANS_FL_AC_W'
            WHEN class_code IN (2706,2805,2806,2205,2875) THEN 'VANS_FL_AC_U'
            WHEN class_code = 1011 THEN 'VANS_BS_FT_M'
            WHEN class_code = 1111 THEN 'VANS_BS_FT_U'
            WHEN class_code IN (1211,1212,1213) THEN 'VANS_BS_FT_W'
            WHEN class_code IN (1311,1411,1409,1511,1520,1306,1307,1309,1312) THEN 'VANS_BS_FT_Y'
            WHEN charindex('3700000010911',style_aka) > 0  or
            charindex('3705000011018',style_aka) > 0 or
            charindex('3710000011510',style_aka) > 0 THEN 'VANS_BS_AP_M'
            WHEN class_code IN (2505,2506) THEN 'VANS_BS_AC_M'
            WHEN class_code = 2507 THEN 'VANS_BS_AC_YB'
            WHEN class_code = 2955 THEN 'VANS_BS_AC_W'
            WHEN class_code IN (2512,2511,2513) THEN 'VANS_BS_AC_Y'
            WHEN class_code = 2705 THEN 'VANS_BS_AC_U'
            WHEN class_code IN (1005,3099) THEN 'VANS_MO_FT_M'
            WHEN class_code IN (1205,3299) THEN 'VANS_MO_FT_W'
            WHEN class_code IN (1105,3199) THEN 'VANS_MO_FT_U'
            WHEN class_code IN (1305,1405,1505,1508,3399,3499,3599,3699) THEN 'VANS_MO_FT_Y'
            WHEN class_code IN (9970,9971,2407,9960,9990,9950,2860,2850,9910,1315,1515,1408,1309,1107,1109,1007,9900,3009,3640,3645,3650) THEN 'VANS_OT'
            WHEN class_code = 9980 THEN 'VANS_OT_EXCLUDE'
            WHEN class_code IN (6000,6001,6002,6003,6005,7000,7100,7110,7200,7210,7500,7510,7520,8201,8208,8495,8202) THEN 'VANS_OT_NOTUSED'
            WHEN style_id = 1 or  class_code=1215 THEN 'VANS_UNSPECIFIED'
            else ''
            end as VANS_PRODCAT
            FROM {0}.style
            WHERE sas_brand_id = 7""".format(dbschema)
            utils.execute_query_in_redshift(prodxref_clean_query1, self.whouse_details, logger)


            prodxref_clean_query2 = """create table {0}.prodxref_clean_2 as 
            SELECT DISTINCT a.class_code as class_class_code,
            b.department_code as class_department_code, 
            b.class_description as class_class_description, 
            a.*
            FROM {0}.prodxref_clean_1 a 
            LEFT JOIN ( SELECT * FROM {0}.class WHERE sas_brand_id = 7) as b
            ON a.class_code  = b.class_code
            order by a.style_id""".format(dbschema)
            utils.execute_query_in_redshift(prodxref_clean_query2, self.whouse_details, logger)

            mte_query = """create table {0}.mte as 
            SELECT  DISTINCT style_id, 
            1 as MTE_IND 
            FROM {0}.vans_mte_style_id""".format(dbschema)
            utils.execute_query_in_redshift(mte_query, self.whouse_details, logger)

            prodxref_clean_3_temp1_drop = "drop table if exists {0}.prodxref_clean_3_temp1".format(dbschema)
            prodxref_clean_3_temp1_create = """create table {0}.prodxref_clean_3_temp1 as
            SELECT 
            class_class_code as class_code,
            class_department_code as department_code,
            class_class_description as class_description,
            a.style_id,
            b.mte_ind,
            vendor_code,
            subclass_code,
            vendor_style, 
            style_aka,
            style_description,
            retail,
            cost,
            sas_brand_id,
            VANS_PRODCAT,
            CASE WHEN TRIM(LOWER(style_description)) = 'class level style' 
                THEN UPPER(TRIM(class_class_description)) ELSE  UPPER(TRIM(style_description)) 
            END AS SAS_STYLE_DESCRIPTION,
            'B'::varchar(256) as product_gender,
            'B'::varchar(256) as product_age_group,
            0 as TRGT_Peanuts_FLAG,
            0 as TRGT_Peanuts_like_FLAG,
            0 as Peanuts_ind,
            0 as Peanuts_like_ind,
            0 as TRGT_UltRngLS_FLAG,
            0 as TRGT_UltRngPro_FLAG,
            0 as TRGT_bTS_FLAG,
            null::varchar(256) as VANS_SAS_PRODUCT_CATEGORY,
            null::varchar(256) as product_family,
            null::varchar(256) as product_type,
            null::varchar(256) as gen_age,
            null::varchar(256) as family_type,
            null::integer as skate_ind,
            null::integer as surf_ind,
            null::integer as snwb_ind,
            null::integer as nonsegment_ind
            FROM 
            {0}.prodxref_clean_2 a 
            Left JOIN {0}.mte b 
            ON  
            a.style_id = b.style_id""".format(dbschema)

            prodxref_clean_3_temp_query = [prodxref_clean_3_temp1_drop,prodxref_clean_3_temp1_create]
            utils.execute_multiple_queries_in_redshift(prodxref_clean_3_temp_query, self.whouse_details, logger)

            prodxref_clean_3_temp_update1 = """update {0}.prodxref_clean_3_temp1
            set VANS_SAS_PRODUCT_CATEGORY = 'MENS CORE CLASSIC'
            where class_code = 1011""".format(dbschema)
    
            prodxref_clean_3_temp_update2 = """update {0}.prodxref_clean_3_temp1
            set VANS_SAS_PRODUCT_CATEGORY = 'MENS BRAND AFFINITY'
            where      charindex('3700000010911',style_aka) > 0
            		or charindex('3705000011018',style_aka) > 0 
            		or charindex('3710000011510',style_aka) > 0
            		or charindex('3700000012937',style_aka) > 0
            		or charindex('3700000017008',style_aka) > 0
            		or charindex('3700000017500',style_aka) > 0
            		or charindex('3700000017510',style_aka) > 0
            		or charindex('3700000018012',style_aka) > 0
            		or charindex('3700000019006',style_aka) > 0
            		or charindex('3700005000911',style_aka) > 0
            		or charindex('3705000010002',style_aka) > 0
            		or charindex('3705000010063',style_aka) > 0
            		or charindex('3735000010016',style_aka) > 0
            		or charindex('3735000018017',style_aka) > 0
            		or charindex('3735000019756',style_aka) > 0
            		or charindex('3740000010004',style_aka)  > 0
            		or charindex('3700000019029',style_aka)  > 0
            		or charindex('3700000019031',style_aka) > 0
            		or charindex('3705000017524',style_aka) > 0
            		or charindex('3735000010020',style_aka) > 0
            		or charindex('3700000010268',style_aka) > 0
            		or charindex('3715000010012',style_aka)  > 0
            		or charindex('3710000010008',style_aka)  > 0""".format(dbschema)
    
            prodxref_clean_3_temp_update3 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'ACTION SPORTS'
            where substring(VANS_PRODCAT, 6,2) = 'AS'""".format(dbschema)
    
            prodxref_clean_3_temp_update4 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'BASIC'
            where substring(VANS_PRODCAT, 6,2) = 'BS'""".format(dbschema)
    
            prodxref_clean_3_temp_update5 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'FASHION'
            where substring(VANS_PRODCAT, 6,2) = 'FL'""".format(dbschema)
    
            prodxref_clean_3_temp_update6 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'MARKDOWN OUTLET'
            where substring(VANS_PRODCAT, 6,2) = 'MO'""".format(dbschema)
    
            prodxref_clean_3_temp_update7 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'OTHER'
            where substring(VANS_PRODCAT, 6,2) = 'OT'""".format(dbschema)
    
            prodxref_clean_3_temp_update8 = """update {0}.prodxref_clean_3_temp1
            set product_type = 'APPAREL'
            where substring(VANS_PRODCAT, 9,2) = 'AP'""".format(dbschema)
    
            prodxref_clean_3_temp_update9 = """update {0}.prodxref_clean_3_temp1
            set product_type = 'FOOT WEAR'
            where substring(VANS_PRODCAT, 9,2) = 'FT'""".format(dbschema)
    
            prodxref_clean_3_temp_update10 = """update {0}.prodxref_clean_3_temp1
            set product_type = 'ACCESSORIES'
            where substring(VANS_PRODCAT, 9,2) = 'AC'""".format(dbschema)
    
            prodxref_clean_3_temp_update11 = """update {0}.prodxref_clean_3_temp1
            set product_gender = CASE WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'M' THEN  'M'
                                  WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'W' THEN 'F'
                                  WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'YB' THEN 'M'
                                  WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'YG' THEN 'F'
                                  WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'U' THEN 'U'
                                  WHEN CHARINDEX('WOMEN',SAS_STYLE_DESCRIPTION) > 0 THEN 'F'
                                  WHEN CHARINDEX('MEN',SAS_STYLE_DESCRIPTION) > 0 THEN 'M'
                                  WHEN CHARINDEX('BOY',SAS_STYLE_DESCRIPTION) > 0 THEN  'M'
                                  WHEN CHARINDEX('GIRL',SAS_STYLE_DESCRIPTION) > 0 THEN 'F'
                                  ELSE product_gender end,
                                  
                product_age_group = CASE WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'M' THEN  product_age_group
                                         WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'W' THEN product_age_group
                                         WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'YB' THEN product_age_group
                                         WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'YG' THEN product_age_group
                                         WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) = 'U' THEN product_age_group
                                         WHEN CHARINDEX('WOMEN',SAS_STYLE_DESCRIPTION) > 0 THEN 'ADULT'
                                         WHEN CHARINDEX('MEN',SAS_STYLE_DESCRIPTION) > 0 THEN 'ADULT'
                                         WHEN CHARINDEX('BOY',SAS_STYLE_DESCRIPTION) > 0 THEN 'KID'
                                         WHEN CHARINDEX('GIRL',SAS_STYLE_DESCRIPTION) > 0 THEN  'KID'
                                         WHEN CHARINDEX('TODDLER',SAS_STYLE_DESCRIPTION) > 0 THEN 'KID'
                                         WHEN CHARINDEX('INFANT',SAS_STYLE_DESCRIPTION) > 0 THEN 'KID'
                                         WHEN CHARINDEX('KID',SAS_STYLE_DESCRIPTION) > 0 THEN 'KID'
                                         ELSE product_age_group end""".format(dbschema)
    
            prodxref_clean_3_temp_update12 = """update {0}.prodxref_clean_3_temp1
            set product_gender = CASE WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) in ('M') THEN  'M'
                                      WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) in ('W') THEN  'F'
                                      Else product_gender
                                  End,
                product_age_group = CASE WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) in ('M') THEN  'ADULT'
                                         WHEN TRIM(SPLIT_PART(VANS_PRODCAT, '_',4)) in ('W') THEN  'ADULT'
                                         Else product_age_group
                                    End""".format(dbschema)
    
            prodxref_clean_3_temp_update13 = """update {0}.prodxref_clean_3_temp1
            set product_gender = CASE WHEN CHARINDEX('YB',SPLIT_PART(VANS_PRODCAT, '_',4)) > 0  THEN  'M'
                                      WHEN CHARINDEX('YG',SPLIT_PART(VANS_PRODCAT, '_',4)) > 0  THEN  'F'
                                      Else product_gender
                                  End,
                product_age_group = CASE WHEN CHARINDEX('YB',SPLIT_PART(VANS_PRODCAT, '_',4)) > 0  THEN  'KID'
                                         WHEN CHARINDEX('YG',SPLIT_PART(VANS_PRODCAT, '_',4)) > 0  THEN  'KID'
                                         WHEN CHARINDEX('Y',SPLIT_PART(VANS_PRODCAT, '_',4)) > 0  THEN  'KID'
                                      Else product_age_group
                                  End""".format(dbschema)
    
            prodxref_clean_3_temp_update14 = """update {0}.prodxref_clean_3_temp1
            set product_gender = 'U'
            where TRIM(SPLIT_PART(VANS_PRODCAT, '_',4))  in ('U')""".format(dbschema)
    
            prodxref_clean_3_temp_update15 = """update {0}.prodxref_clean_3_temp1
            set VANS_SAS_PRODUCT_CATEGORY = 'WOMEN FASHION'
            where class_code IN (1206, 1207, 1210, 1214, 2950, 2951, 2952, 2953, 2956, 2957, 2958) 
               or department_code = 380 
               or (product_family = 'FL' AND product_gender = 'F')""".format(dbschema)
    
            prodxref_clean_3_temp_update16 = """update {0}.prodxref_clean_3_temp1
            set VANS_SAS_PRODUCT_CATEGORY = 'KIDS PRODUCTS'
            where class_code IN (1308,1320,1310,1410,1311,1411,1409,1511,1520,2512,2511,2513,1305,1405,1505,1508,3399,3499,3599) 
               or product_age_group = 'KID'""".format(dbschema)
    
            prodxref_clean_3_temp_update17 = """update {0}.prodxref_clean_3_temp1
            set product_family = 'NA',
                product_gender= 'B',
                product_type= 'NA',
                product_age_group= 'B'
            where CHARINDEX('UNSPECIFIED',VANS_PRODCAT) > 0 
               or CHARINDEX('EXCLUDE',VANS_PRODCAT) > 0 
               or CHARINDEX('NOTUSED',VANS_PRODCAT) > 0""".format(dbschema)
    
            prodxref_clean_3_temp_update18 = """update {0}.prodxref_clean_3_temp1
            set gen_age = product_gender||'_'||product_age_group""".format(dbschema)
    
            prodxref_clean_3_temp_update19 = """update {0}.prodxref_clean_3_temp1
            set family_type = CASE WHEN regexp_count(VANS_PRODCAT,'_') >= 2 
                              THEN SPLIT_PART(VANS_PRODCAT,'_',2)||'_'||SPLIT_PART(VANS_PRODCAT,'_',3)
                              WHEN regexp_count(VANS_PRODCAT,'_') = 1  THEN SPLIT_PART(VANS_PRODCAT,'_',2)
                              else family_type end""".format(dbschema)
                             
            prodxref_clean_3_temp_update20 = """update {0}.prodxref_clean_3_temp1
            set mte_ind =  0 
            where mte_ind is null""".format(dbschema)
    
            prodxref_clean_3_temp_update21 = """update {0}.prodxref_clean_3_temp1
            set SKATE_IND = case when sas_style_description like ('% SKATE%') or vans_prodcat like '%SKATE%' then 1 else 0 END,
                SURF_IND = case when sas_style_description like ('% SURF%') or vans_prodcat like '%SURF%' then 1 else 0 END,
                SNWB_IND = case when sas_style_description like ('%SNOWBOARD%') or
                                     class_description like ('%SNOWBOARD%') or
                                     vans_prodcat like '%SNOW%' then 1 else 0 END,
                nonsegment_ind = case when mte_ind =1 or (VANS_SAS_PRODUCT_CATEGORY IN ('MENS CORE CLASSIC', 'KIDS PRODUCTS', 'WOMEN FASHION', 'MEN BRAND AFFINITY')) 
                                 then 0 else 1 end""".format(dbschema)
    
            prodxref_clean_3_temp_update22 = """update {0}.prodxref_clean_3_temp1
            set TRGT_Peanuts_FLAG = 1,
            		Peanuts_ind=1 
            where exists (
            select 1 
            from {0}.vans_peanuts_style B
            where prodxref_clean_3_temp1.style_id = B.style_id
            )""".format(dbschema)
    
            prodxref_clean_3_temp_update23 = """update {0}.prodxref_clean_3_temp1 
            set TRGT_Peanuts_like_FLAG = 1,
                Peanuts_like_ind=1 
            where exists (
            select 1 
            from {0}.vans_prod_seg_past_recom B
            where prodxref_clean_3_temp1.class_code = B.class
            and prodxref_clean_3_temp1.vendor_code = B.vendor::integer
            and b.tab = 'PEANUTS'
            and ((substring(prodxref_clean_3_temp1.style_aka,len(TRIM(prodxref_clean_3_temp1.style_aka))-3, 4) = B.style)
            or (upper(b.style) = 'ALL'))
            )""".format(dbschema)
    
            prodxref_clean_3_temp_update24 = """update {0}.prodxref_clean_3_temp1 
            set TRGT_UltRngLS_FLAG = 1 
            where exists (
            select 1 
            from {0}.vans_prod_seg_past_recom B
            where prodxref_clean_3_temp1.class_code = B.class
            and prodxref_clean_3_temp1.vendor_code = B.vendor::integer
            and b.tab = 'ULTRARANGE_LIFESTYLE'
            and ((substring(prodxref_clean_3_temp1.style_aka,len(trim(prodxref_clean_3_temp1.style_aka))-3, 4) = B.style)
            or (upper(b.style) = 'ALL'))
            )""".format(dbschema)
    
            prodxref_clean_3_temp_update25 = """update {0}.prodxref_clean_3_temp1
            set TRGT_UltRngPro_FLAG = 1 
            where exists (
            select 1 
            from {0}.vans_prod_seg_past_recom B
            where prodxref_clean_3_temp1.class_code = B.class
            and prodxref_clean_3_temp1.vendor_code = B.vendor::integer
            and b.tab = 'ULTRARANGE_PRO'
            and ((substring(prodxref_clean_3_temp1.style_aka,len(trim(prodxref_clean_3_temp1.style_aka))-3, 4) = B.style)
            or (upper(b.style) = 'ALL'))
            )""".format(dbschema)
    
            prodxref_clean_3_temp_update26 = """update {0}.prodxref_clean_3_temp1 
            set TRGT_bTS_FLAG = 1 
            where exists (
            select 1 
            from {0}.vans_prod_seg_past_recom B
            where prodxref_clean_3_temp1.class_code = B.class
            and prodxref_clean_3_temp1.vendor_code = B.vendor::integer
            and b.tab = 'BTS'
            and ((substring(prodxref_clean_3_temp1.style_aka,len(trim(prodxref_clean_3_temp1.style_aka))-3, 4) = B.style)
            or (upper(b.style) = 'ALL'))
            )""".format(dbschema)
    
            prodxref_clean_3_temp_update=[prodxref_clean_3_temp_update1,prodxref_clean_3_temp_update2,prodxref_clean_3_temp_update3,prodxref_clean_3_temp_update4,
            prodxref_clean_3_temp_update5,prodxref_clean_3_temp_update6,prodxref_clean_3_temp_update7,prodxref_clean_3_temp_update8,prodxref_clean_3_temp_update9,
            prodxref_clean_3_temp_update10,prodxref_clean_3_temp_update11,prodxref_clean_3_temp_update12,prodxref_clean_3_temp_update13,prodxref_clean_3_temp_update14,
            prodxref_clean_3_temp_update15,prodxref_clean_3_temp_update16,prodxref_clean_3_temp_update17,prodxref_clean_3_temp_update18,prodxref_clean_3_temp_update19,
            prodxref_clean_3_temp_update20,prodxref_clean_3_temp_update21,prodxref_clean_3_temp_update22,prodxref_clean_3_temp_update23,prodxref_clean_3_temp_update24,
            prodxref_clean_3_temp_update25,prodxref_clean_3_temp_update26]
            
            utils.execute_multiple_queries_in_redshift(prodxref_clean_3_temp_update, self.whouse_details, logger)
            
            prodxref_tmp_drop = "drop table if exists {0}.prodxref_tmp".format(dbschema)
            prodxref_tmp_create = """create table {0}.prodxref_tmp as
            select a.*, b.style_id
            from {0}.ur_sku a 
            left join 
            (select * from {0}.productxref where sas_brand_id = 7) b
            on a.IP_UPC = b.product_code""".format(dbschema)
            prodxref_tmp_query = [prodxref_tmp_drop,prodxref_tmp_create]
            utils.execute_multiple_queries_in_redshift(prodxref_tmp_query, self.whouse_details, logger)
            
            vans_style_xref_clean_truncate = "Truncate table {0}.vans_style_xref_clean".format(dbschema)
            utils.execute_query_in_redshift(vans_style_xref_clean_truncate, self.whouse_details, logger)
            
            vans_style_xref_clean_insert = """Insert into {0}.vans_style_xref_clean
            select 
            department_code ,
            class_code ,
            class_description ,
            sas_brand_id ,
            style_id ,
            vendor_code ,
            subclass_code ,
            vendor_style ,
            style_aka ,
            style_description ,
            retail ,
            cost ,
            vans_prodcat ,
            mte_ind ,
            vans_sas_product_category ,
            product_family ,
            product_type ,
            product_gender ,
            product_age_group ,
            sas_style_description ,
            gen_age ,
            family_type,
            skate_ind ,
            surf_ind ,
            snwb_ind ,
            nonsegment_ind ,
            trgt_peanuts_flag ,
            trgt_peanuts_like_flag ,
            peanuts_ind ,
            peanuts_like_ind ,
            trgt_ultrngls_flag ,
            trgt_ultrngpro_flag ,
            trgt_bts_flag , 
            case 
            when style_id in (select distinct style_id from {0}.prodxref_tmp where style_id is not null ) then 1 
            else 0 
            end 
            as TRGT_NEW_ULTRARANGE_FLAG,
            current_timestamp as process_dtm
            from {0}.prodxref_clean_3_temp1""".format(dbschema)
            
            status = utils.execute_query_in_redshift(vans_style_xref_clean_insert, self.whouse_details, logger)

            utils.execute_multiple_queries_in_redshift(drop_clean_stage_tables, self.whouse_details, logger)
            utils.execute_query_in_redshift(prodxref_clean_3_temp1_drop, self.whouse_details, logger)
            utils.execute_query_in_redshift(prodxref_tmp_drop, self.whouse_details, logger)


        except Exception as error:
            status = False
            logger.error(
                "Error Ocuured While processiong tr_clean_vans_xref due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error Ocuured While processiong tr_clean_vans_xref due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return status

    def xr_build_cust_xref_final(self):
        spark = self.spark
        logger = self.logger
        response = self.params

        try:
            cust_xref_stored_procedure_qry = config.cust_xref_stored_procedure.format(
                self.whouse_details["dbSchema"]
            )
            logger.info(
                " cust_xref_stored_procedure_qry : {}".format(
                    cust_xref_stored_procedure_qry
                )
            )
            utils.execute_query_in_redshift(
                cust_xref_stored_procedure_qry, self.whouse_details, logger
            )

            utils.execute_query_in_redshift(
                "call {}.stp_find_new_customer({});".format(
                    self.whouse_details["dbSchema"],
                    xref_global_config.CUST_SAS_BRAND_ID_4,
                ),
                self.whouse_details,
                logger,
            )
            utils.execute_query_in_redshift(
                "call {}.stp_find_new_customer({});".format(
                    self.whouse_details["dbSchema"],
                    xref_global_config.CUST_SAS_BRAND_ID_7,
                ),
                self.whouse_details,
                logger,
            )
            status = True

        except Exception as error:
            logger.error(
                "Error Occurred in Xref Job due To {}".format(error), exc_info=1
            )
            status = False
            raise CustomAppError(
                moduleName=constant.XR_XREF_JOB,
                exeptionType=constant.XREF_EXCEPTION,
                message="Error encountered while running xr_build_cust_xref_final : {}".format(
                    traceback.format_exc()
                ),
            )
        return status
