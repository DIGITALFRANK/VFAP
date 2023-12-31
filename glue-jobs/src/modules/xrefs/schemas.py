from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema_style_xref_clean = StructType(
    [
        StructField("department_code", IntegerType(), True),
        StructField("department_description", StringType(), True),
        StructField("class_code", IntegerType(), True),
        StructField("class_description", StringType(), True),
        StructField("style_id", IntegerType(), True),
        StructField("vendor_style", StringType(), True),
        StructField("style_description", StringType(), True),
        StructField("vendor_cd_4", StringType(), True),
        StructField("vendor_cd_6", StringType(), True),
        StructField("cnodeid", IntegerType(), True),
        StructField("style_description_short", StringType(), True),
        StructField("style_description_long", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("sbu", StringType(), True),
        StructField("series", StringType(), True),
        StructField("newconsumersegmentterritory", StringType(), True),
        StructField("newendusesport", StringType(), True),
        StructField("newproductcollectionfamily", StringType(), True),
        StructField("tnf___clm_description_for_sas", StringType(), True),
        StructField("tnf_sas_product_category", StringType(), True),
        StructField("product_gender", StringType(), True),
        StructField("product_age_group", StringType(), True),
        StructField("sas_style_description", StringType(), True),
        StructField("TRGT_MA_FLAG", IntegerType(), True),
        StructField("TRGT_MS_FLAG", IntegerType(), True),
        StructField("TRGT_ML_FLAG", IntegerType(), True),
        StructField("TRGT_UE_FLAG", IntegerType(), True),
        StructField("gender_age", StringType(), True),
    ]
)

style_xref_schema_1 = StructType(
    [
        StructField("department_code", IntegerType(), True),
        StructField("department_description", StringType(), True),
        StructField("class_code", IntegerType(), True),
        StructField("class_description", StringType(), True),
        StructField("style_id", IntegerType(), True),
        StructField("vendor_style", StringType(), True),
        StructField("style_description", StringType(), True),
        StructField("vendor_cd_4", StringType(), True),
        StructField("vendor_cd_6", StringType(), True),
        StructField("cnodeid", IntegerType(), True),
        StructField("style_description_short", StringType(), True),
        StructField("style_description_long", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("sbu", StringType(), True),
        StructField("series", StringType(), True),
        StructField("newconsumersegmentterritory", StringType(), True),
        StructField("newendusesport", StringType(), True),
        StructField("newproductcollectionfamily", StringType(), True),
        StructField("tnf___clm_description_for_sas", StringType(), True),
        StructField("tnf_sas_product_category", StringType(), True),
        StructField("product_gender", StringType(), True),
        StructField("product_age_group", StringType(), True),
        StructField("sas_style_description", StringType(), True),
    ]
)
