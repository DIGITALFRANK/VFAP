from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.types import *


class tr_clean_vans_xref(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_clean_vans_xref

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """
        print("inside tr_clean_vans_xref constructor")
        super(tr_clean_vans_xref, self).__init__(file_name)

        self.whouse_class_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "class" + "/",
        )
        self.whouse_sku_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "sku" + "/",
        )
        self.whouse_us_item_master_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "vans_item_master" + "/",
        )
        self.whouse_prod_xref_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "product_xref" + "/",
        )
        self.whouse_vans_mte_style_id_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "vans_mte_style_id" + "/",
        )
        self.whouse_vans_peanuts_style_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"] + "/" + "vans_peanuts_style" + "/",
        )
        self.whouse_vans_prod_seg_past_recom_df = self.read_from_s3(
            bucket=self.params["REFINED_BUCKET_NAME"],
            path=self.params["REFINED_SOURCE_PATH"]
            + "/"
            + "vans_prod_seg_past_recom"
            + "/",
        )

    def transform(self, df):
        """
        Builds the tr_clean_vans_xref table for VANS

        :param df:
        :return:
        """
        print(" Applying tr_clean_vans_xref ")
        spark = self.spark
        params = self.params
        logger = self.logger
        full_load_df = None
        try:
            print("enter into try block")

            whouse_style_df = df
            whouse_class_df = self.whouse_class_df
            whouse_sku_df = self.whouse_sku_df
            whouse_us_item_master_df = self.whouse_us_item_master_df
            whouse_prod_xref_df = self.whouse_prod_xref_df
            whouse_vans_mte_style_id_df = self.whouse_vans_mte_style_id_df
            whouse_vans_peanuts_style_df = self.whouse_vans_peanuts_style_df
            whouse_whouse_vans_prod_seg_past_recom_df = (
                self.whouse_vans_prod_seg_past_recom_df
            )

            whouse_whouse_vans_prod_seg_past_recom_df.printSchema()
            whouse_class_df.printSchema()
            whouse_sku_df.printSchema()
            whouse_prod_xref_df.printSchema()
            whouse_us_item_master_df.printSchema()
            whouse_style_df.printSchema()
            whouse_vans_mte_style_id_df.printSchema()
            whouse_vans_peanuts_style_df.printSchema()

            whouse_whouse_vans_prod_seg_past_recom_df.createOrReplaceTempView(
                "whouse_vans_prod_seg_past_recom"
            )
            whouse_class_df.createOrReplaceTempView("whouse_class")
            whouse_sku_df.createOrReplaceTempView("whouse_sku")
            whouse_us_item_master_df.createOrReplaceTempView("whouse_us_item_master")
            whouse_prod_xref_df.createOrReplaceTempView("whouse_prod_xref")
            whouse_style_df.createOrReplaceTempView("whouse_style")
            whouse_vans_mte_style_id_df.createOrReplaceTempView(
                "whouse_vans_mte_style_id"
            )
            whouse_vans_peanuts_style_df.createOrReplaceTempView(
                "whouse_vans_peanuts_style"
            )

            prodxref_clean_1_df = spark.sql(
                """
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
                		instr(style_aka,'3700000010911') = 0 AND
                		instr(style_aka,'3705000011018') = 0 AND
                		instr(style_aka,'3710000011510') = 0 THEN 'VANS_FL_AP_M' 
                		WHEN class_code IN (2102,3800,3805,3810,3815,3820,3825,3830,3835,3840,3845,3850,3855,3860,3865,3868,3870,3875,3880) THEN 'VANS_FL_AP_W'
                		WHEN class_code IN (2110,3900,3905,3910,3915,3920,3925,3930,3935,3940,3945,3955,3960,3965,3970) THEN 'VANS_FL_AP_YB'
                		WHEN class_code IN (2005,2006,2007,2207,2208,2210,2212,2409,2410,2495,2509,3788,2930,2508,2411,2408,2404,2213,2209,2510,2865) THEN 'VANS_FL_AC_M'
                		WHEN class_code IN (2950,2951,2952,2953,2956,2957,2958,2009,2954,2855,2870) THEN 'VANS_FL_AC_W'
                		WHEN class_code IN (2706,2805,2806,2205,2875) THEN 'VANS_FL_AC_U'
                		WHEN class_code = 1011 THEN 'VANS_BS_FT_M'
                		WHEN class_code = 1111 THEN 'VANS_BS_FT_U'
                		WHEN class_code IN (1211,1212,1213) THEN 'VANS_BS_FT_W'
                		WHEN class_code IN (1311,1411,1409,1511,1520,1306,1307,1309,1312) THEN 'VANS_BS_FT_Y'
                		WHEN instr(style_aka,'3700000010911') > 0  or
                		instr(style_aka,'3705000011018') > 0 or
                		instr(style_aka,'3710000011510') > 0 THEN 'VANS_BS_AP_M'
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
                	FROM whouse_style
                	WHERE sas_brand_id = 7 """
            )

            prodxref_clean_1_df.createOrReplaceTempView("prodxref_clean_1")

            prodxref_clean_2_df = spark.sql(
                """
                				SELECT DISTINCT a.class_code as class_class_code,
                				                b.department_code as class_department_code, 
                								b.class_description as class_class_description, 
                								a.*
                				FROM prodxref_clean_1 a 
                				LEFT JOIN ( SELECT * FROM whouse_class WHERE sas_brand_id = 7) as b
                					ON a.class_code  = b.class_code
                				order by a.style_id """
            ).drop("class_code")

            prodxref_clean_2_df.createOrReplaceTempView("prodxref_clean_2")

            mte_df = spark.sql(
                """
                				SELECT  DISTINCT style_id, 
                					1 as MTE_IND 
                				FROM whouse_vans_mte_style_id """
            )
            mte_df.createOrReplaceTempView("mte")

            prodxref_clean_df_tmp_1 = spark.sql(
                """ 
                        SELECT class_department_code,
                            class_class_code,
                            class_class_description, 
                            vendor_code,
                            a.style_id,
                            CASE WHEN LOWER(TRIM(style_description)) = "class level style" 
                                THEN UPPER(TRIM(class_class_description)) ELSE  UPPER(TRIM(style_description)) 
                            END AS SAS_STYLE_DESCRIPTION, 
                            style_aka,
                            CASE WHEN SUBSTR(VANS_PRODCAT, 6,2) = "AS" THEN "ACTION SPORTS"
                                WHEN SUBSTR(VANS_PRODCAT, 6,2) = "BS" THEN "BASIC"
                                WHEN SUBSTR(VANS_PRODCAT, 6,2) = "FL" THEN "FASHION"
                                WHEN SUBSTR(VANS_PRODCAT, 6,2) = "MO" THEN "MARKDOWN OUTLET"
                                WHEN SUBSTR(VANS_PRODCAT, 6,2) = "OT" THEN "OTHER"
                                WHEN INSTR(VANS_PRODCAT, 'UNSPECIFIED') > 0 or INSTR(VANS_PRODCAT, 'EXCLUDE') > 0 or INSTR(VANS_PRODCAT, 'NOTUSED') > 0 THEN 'NA'
                                ELSE ""
                            END AS product_family,
                            CASE WHEN SUBSTR(VANS_PRODCAT, 9,2) = "AP" THEN "APPAREL"
                                WHEN SUBSTR(VANS_PRODCAT, 9,2) = "FT"  THEN "FOOT WEAR"
                                WHEN SUBSTR(VANS_PRODCAT, 9,2) = "AC"  THEN "ACCESSORIES"
                                WHEN INSTR(VANS_PRODCAT, 'UNSPECIFIED') > 0 or INSTR(VANS_PRODCAT, 'EXCLUDE') > 0 or INSTR(VANS_PRODCAT, 'NOTUSED') > 0 THEN 'NA'
                                ELSE ""
                            END AS product_type,	
                            if(MTE_IND is null,0,MTE_IND) as MTE_IND,	
                            VANS_PRODCAT
                        FROM prodxref_clean_2 a JOIN mte b ON  a.style_id = b.style_id """
            )

            prodxref_clean_df_tmp_1.createOrReplaceTempView("prodxref_clean_tmp_1")

            prodxref_clean_df_tmp_2 = spark.sql(
                """ 
                        SELECT * , 
                			CASE WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "M" THEN  "M"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "W" THEN "F"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "YB" THEN "M"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "YG" THEN "F"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "U" THEN "U"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "WOMEN") > 0 THEN "F"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "MEN") > 0 THEN "M"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "BOY") > 0 THEN  "M"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "GIRL") > 0 THEN "F"
                				WHEN INSTR(VANS_PRODCAT, 'UNSPECIFIED') > 0 or INSTR(VANS_PRODCAT, 'EXCLUDE') > 0 or INSTR(VANS_PRODCAT, 'NOTUSED') > 0 THEN 'B'
                				ELSE 'B'
                			END AS product_gender,
                			CASE WHEN INSTR(SAS_STYLE_DESCRIPTION, "WOMEN") > 0 THEN "ADULT"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "MEN") > 0 THEN "ADULT"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "BOY") > 0 THEN "KID"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "GIRL") > 0 THEN  "KID"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "TODDLER") > 0 THEN "KID"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "INFANT") > 0 THEN "KID"
                				WHEN INSTR(SAS_STYLE_DESCRIPTION, "KID") > 0 THEN "KID"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "M" THEN "ADULT"
                				WHEN TRIM(SPLIT(VANS_PRODCAT, "_")[3]) = "W" THEN "ADULT"
                				WHEN INSTR(SPLIT(VANS_PRODCAT, "_")[3], "YB") > 0 THEN "KID"
                				WHEN INSTR(SPLIT(VANS_PRODCAT, "_")[3], "YG") > 0 THEN "KID"
                				WHEN INSTR(SPLIT(VANS_PRODCAT, "_")[3], "Y") > 0 THEN "KID"
                				WHEN INSTR(VANS_PRODCAT, 'UNSPECIFIED') > 0 or INSTR(VANS_PRODCAT, 'EXCLUDE') > 0 or INSTR(VANS_PRODCAT, 'NOTUSED') > 0 THEN 'B'
                				ELSE 'B'
                			END AS product_age_group,

                			CASE WHEN (char_length(VANS_PRODCAT)) - (char_length(replace(VANS_PRODCAT, "_", ""))) >= 2 
                				THEN CONCAT(SPLIT(VANS_PRODCAT,'_')[1],'_',SPLIT(VANS_PRODCAT,'_')[2])
                				WHEN (char_length(VANS_PRODCAT)) - (char_length(replace(VANS_PRODCAT, "_", ""))) = 1 
                				THEN SPLIT(VANS_PRODCAT,'_')[1]
                				ELSE ""
                			END AS family_type,

                			CASE WHEN (INSTR(SAS_STYLE_DESCRIPTION,"SKATE")> 0 OR 
                					INSTR(VANS_PRODCAT,"SKATE") > 0) THEN  1 ELSE 0
                			END AS SKATE_IND,

                			CASE WHEN  (INSTR(SAS_STYLE_DESCRIPTION,"SURF") >0  OR INSTR(VANS_PRODCAT,"SURF") >0 ) THEN 1 ELSE 0
                			END AS SURF_IND,

                			CASE WHEN (INSTR(SAS_STYLE_DESCRIPTION,"SNOWBOARD")> 0 OR
                					INSTR(CLASS_CLASS_DESCRIPTION,"SNOWBOARD") > 0 OR INSTR(VANS_PRODCAT,"SNOW") > 0) 
                				THEN 1
                				else 0
                			END AS SNWB_IND
                		FROM prodxref_clean_tmp_1 """
            )

            prodxref_clean_df_tmp_2.createOrReplaceTempView("prodxref_clean_tmp_2")

            prodxref_clean_df_tmp_3 = spark.sql(
                """
                		SELECT *, 
                			CONCAT(product_gender,"_",product_age_group) as gen_age,			
                			CASE WHEN class_class_code = 1011 THEN "MENS CORE CLASSIC"
                				WHEN instr(style_aka,'3700000010911') > 0
                					or instr(style_aka,'3705000011018') > 0
                					or instr(style_aka,'3710000011510') > 0
                					or instr(style_aka,'3700000012937') > 0
                					or instr(style_aka,'3700000017008') > 0
                					or instr(style_aka,'3700000017500') > 0
                					or instr(style_aka,'3700000017510') > 0
                					or instr(style_aka,'3700000018012') > 0
                					or instr(style_aka,'3700000019006') > 0
                					or instr(style_aka,'3700005000911') > 0
                					or instr(style_aka,'3705000010002') > 0
                					or instr(style_aka,'3705000010063') > 0
                					or instr(style_aka,'3735000010016') > 0
                					or instr(style_aka,'3735000018017') > 0
                					or instr(style_aka,'3735000019756') > 0
                					or instr(style_aka,'3740000010004') > 0
                					or instr(style_aka,'3700000019029') > 0
                					or instr(style_aka,'3700000019031') > 0
                					or instr(style_aka,'3705000017524') > 0
                					or instr(style_aka,'3735000010020') > 0
                					or instr(style_aka,'3700000010268') > 0
                					or instr(style_aka,'3715000010012') > 0
                					or instr(style_aka,'3710000010008') > 0 THEN "MENS BRAND AFFINITY" 
                				WHEN class_class_code IN (1206, 1207, 1210, 1214, 2950, 2951, 2952, 2953, 2956, 2957, 2958) 
                					or class_department_code = 380 
                					or (product_family = "FL" AND product_gender = "F") THEN "WOMEN FASHION"
                				WHEN class_class_code IN (1308,1320,1310,1410,1311,1411,1409,1511,1520,2512,2511,2513,1305,1405,1505,1508,3399,3499,3599) 
                				or product_age_group = "KID" THEN "KIDS PRODUCTS"
                				ELSE "" 
                				END AS VANS_SAS_PRODUCT_CATEGORY
                		FROM prodxref_clean_tmp_2	
                			"""
            )
            prodxref_clean_df_tmp_3.createOrReplaceTempView("prodxref_clean_tmp_3")

            prodxref_clean_df_3 = (
                spark.sql(
                    """
                				SELECT *,
                					CASE WHEN  ((MTE_IND = 1) OR (VANS_SAS_PRODUCT_CATEGORY IN ('MENS CORE CLASSIC', 'KIDS PRODUCTS', 'WOMEN FASHION', 'MEN BRAND AFFINITY'))) THEN 0 ELSE 1
                					END AS nonsegment_ind,
                					0 as TRGT_Peanuts_FLAG,
                					0 as TRGT_Peanuts_like_FLAG,
                					0 as Peanuts_ind,
                					0 as Peanuts_like_ind,
                					0 as TRGT_UltRngLS_FLAG,
                					0 as TRGT_UltRngPro_FLAG,
                					0 as TRGT_bTS_FLAG
                				FROM prodxref_clean_tmp_3 
                				"""
                )
                .withColumnRenamed("class_department_code", "department_code")
                .withColumnRenamed("class_class_code", "class_code")
                .withColumnRenamed("class_class_description", "class_description")
            )

            prodxref_clean_df_3.createOrReplaceTempView("prodxref_clean_3")

            Peanuts_ind_df = (
                spark.sql(
                    """ 
                					SELECT  
                						A.*,
                						CASE WHEN default is null then 0 else default 
                						end as TRGT_Peanuts_FLAG_tmp,
                						CASE WHEN default is null then 0 else default 
                						end as Peanuts_ind_tmp
                					FROM 
                						prodxref_clean_3 A LEFT JOIN 
                						(SELECT A.style_id as style_style_id,1 as default 
                						FROM 
                						prodxref_clean_3 A join whouse_vans_peanuts_style B
                						ON A.style_id = B.style_id 
                						) sub 
                						ON A.style_id = sub.style_style_id """
                )
                .drop("TRGT_Peanuts_FLAG", "Peanuts_ind")
                .withColumnRenamed("TRGT_Peanuts_FLAG_tmp", "TRGT_Peanuts_FLAG")
                .withColumnRenamed("Peanuts_ind_tmp", "Peanuts_ind")
            )

            Peanuts_ind_df.createOrReplaceTempView("Peanuts_ind_vw")

            Peanuts_like_FLAG_df = (
                spark.sql(
                    """ 
                            SELECT 
                                A.*,
                                CASE WHEN default is null then 0 else default 
                                end as TRGT_Peanuts_like_FLAG_tmp,
                                CASE WHEN default is null then 0 else default 
                                end as Peanuts_like_ind_tmp
                                FROM 	Peanuts_ind_vw A LEFT JOIN 		
                                (SELECT CAST(B.class as int) as class_class,
                                        CAST(B.vendor as int) as vendor_vendor,
                                        style as style_style ,
                                        1 as default
                            FROM Peanuts_ind_vw A JOIN whouse_vans_prod_seg_past_recom B
                            ON A.class_code = CAST(B.class as int)
                            AND A.vendor_code = CAST(B.vendor as int)
                            AND B.tab = "PEANUTS"
                            AND ((substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = B.style)
                            or (UPPER(B.style) = 'ALL'))
                                ) sub	
                                ON A.class_code = sub.class_class
                                AND A.vendor_code = sub.vendor_vendor 
                                AND substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = sub.style_style
                                """
                )
                .drop("TRGT_Peanuts_like_FLAG", "Peanuts_like_ind")
                .withColumnRenamed(
                    "TRGT_Peanuts_like_FLAG_tmp", "TRGT_Peanuts_like_FLAG"
                )
                .withColumnRenamed("Peanuts_like_ind_tmp", "Peanuts_like_ind")
            )

            Peanuts_like_FLAG_df.createOrReplaceTempView("Peanuts_like_FLAG_vw")

            trgt_ultrngls_flag_df = (
                spark.sql(
                    """ 
                            SELECT 
                                A.*,
                                CASE WHEN default is null then 0 else default 
                                end as trgt_ultrngls_flag_tmp
                                FROM 	Peanuts_like_FLAG_vw A LEFT JOIN 		
                                (SELECT CAST(B.class as int) as class_class,
                                        CAST(B.vendor as int) as vendor_vendor,
                                        style,
                                        1 as default
                            FROM Peanuts_like_FLAG_vw A JOIN whouse_vans_prod_seg_past_recom B
                            ON A.class_code = CAST(B.class as int)
                            AND A.vendor_code = CAST(B.vendor as int)
                            AND B.tab = "ULTRARANGE_LIFESTYLE"
                            AND ((substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = B.style)
                                or (UPPER(B.style) = 'ALL'))
                                ) sub	
                                ON A.class_code = sub.class_class
                                AND A.vendor_code = sub.vendor_vendor 
                                AND substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = sub.style
                                """
                )
                .drop("trgt_ultrngls_flag")
                .withColumnRenamed("trgt_ultrngls_flag_tmp", "trgt_ultrngls_flag")
            )

            trgt_ultrngls_flag_df.createOrReplaceTempView("trgt_ultrngls_flag_vw")

            trgt_ultrngpro_flag_df = (
                spark.sql(
                    """ 
                                SELECT 
                                    A.*,
                                    CASE WHEN default is null then 0 else default 
                                    end as trgt_ultrngpro_flag_tmp
                                    FROM 	trgt_ultrngls_flag_vw A LEFT JOIN 		
                                    (SELECT CAST(B.class as int) as class_class,
                                            CAST(B.vendor as int) as vendor_vendor,
                                            style,
                                            1 as default
                                FROM trgt_ultrngls_flag_vw A JOIN whouse_vans_prod_seg_past_recom B
                                ON A.class_code = CAST(B.class as int)
                                AND A.vendor_code = CAST(B.vendor as int)
                                AND B.tab = "ULTRARANGE_PRO"
                                AND ((substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = B.style)
                                    or (UPPER(B.style) = 'ALL'))
                                    ) sub	
                                    ON A.class_code = sub.class_class
                                    AND A.vendor_code = sub.vendor_vendor 
                                    AND substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = sub.style
                                    """
                )
                .drop("trgt_ultrngpro_flag")
                .withColumnRenamed("trgt_ultrngpro_flag_tmp", "trgt_ultrngpro_flag")
            )

            trgt_ultrngpro_flag_df.createOrReplaceTempView("trgt_ultrngpro_flag_vw")

            trgt_bts_flag_df = (
                spark.sql(
                    """ 
                					SELECT 
                						A.*,
                						CASE WHEN default is null then 0 else default 
                						end as trgt_bts_flag_tmp
                						FROM 	trgt_ultrngpro_flag_vw A LEFT JOIN 		
                						(SELECT CAST(B.class as int) as class_class,
                								CAST(B.vendor as int) as vendor_vendor,
                								style,
                								1 as default
                						FROM trgt_ultrngpro_flag_vw A JOIN whouse_vans_prod_seg_past_recom B
                						ON A.class_code = CAST(B.class as int)
                						AND A.vendor_code = CAST(B.vendor as int)
                						AND B.tab = "BTS"
                						AND ((substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = B.style)
                						or (UPPER(B.style) = 'ALL'))) sub	
                						ON A.class_code = sub.class_class
                						AND A.vendor_code = sub.vendor_vendor 
                						AND substr(A.style_aka,CHAR_LENGTH(TRIM(A.style_aka))-3, 4) = sub.style
                						"""
                )
                .drop("trgt_bts_flag")
                .withColumnRenamed("trgt_bts_flag_tmp", "trgt_bts_flag")
            )

            trgt_bts_flag_df.createOrReplaceTempView("trgt_bts_flag_vw")

            tmp_df = spark.sql(
                """	
                					SELECT a.*, 
                							b.style_id
                					FROM whouse_sku a 
                					LEFT JOIN ( SELECT  * 
                								FROM  whouse_prod_xref 
                								WHERE sas_brand_id = 7 
                								) b
                					ON  a.IP_UPC = b.product_code """
            )
            tmp_df.createOrReplaceTempView("tmp")

            full_load_df = spark.sql(
                """ 
                                SELECT a.*, 
                                    CASE 
                                        WHEN b.style is not null THEN 1 else 0 
                                    END AS TRGT_NEW_ULTRARANGE_FLAG
                                FROM trgt_bts_flag_vw a 
                                LEFT JOIN ( SELECT DISTINCT style_id as style FROM tmp WHERE style_id IS NOT NULL
                                          ) b on a.style_id = b.style """
            )
            full_load_df.createOrReplaceTempView("vans_style_xref_clean")
            print("count of records in full_load_df is {}".format(full_load_df.count()))
            full_load_df.show()
            # COMBINE CRM style with VANS_US_ITEM MASTER via productxref to see how much is missing IN respective datasets
            #
            # SELECT COUNT(DISTINCT product_code) as num_product_missing_crm
            # FROM whouse_prod_xref
            # WHERE sas_brand_id = 7
            # AND product_code NOT IN(SELECT CAST(upc as int) as product_code FROM whouse_us_item_master)
            #
            # SELECT COUNT(DISTINCT upc) as num_prod_missing_US_item_mstr
            # FROM whouse_us_item_master
            # WHERE CAST(upc as int) NOT IN( SELECT product_code FROM whouse_prod_xref WHERE sas_brand_id = 7)
            #
            # SELECT COUNT(DISTINCT style_id) as num_style_missing_crm
            # FROM whouse_style
            # WHERE sas_brand_id = 7
            #    AND style_id NOT IN(SELECT style_id FROM whouse_prod_xref WHERE sas_brand_id = 7 )
            #
            # SELECT COUNT(DISTINCT style_id) as num_style_missing_prodxref
            # FROM whouse_prod_xref
            # WHERE sas_brand_id = 7
            # AND style_id NOT IN(SELECT style_id FROM whouse_style WHERE sas_brand_id = 7 )

        except Exception as error:
            full_load_df = None
            logger.info(
                "Error Ocuured While processiong tr_clean_vans_xref due to : {}".format(
                    error
                )
            )
        return full_load_df
