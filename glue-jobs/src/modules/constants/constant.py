# ___________________Xref names______________________
session_xref = "cm_session_xref"
tnf_style_xref = "tnf_style_xref_clean"
email_xref = "email_xref"
loyalty_xref = "loyalty_xref"
customer_xref = "cust_xref"
vans_clean_xref = "vans_clean_xref"

# _________________DDB etl status creation message________________

ETL_STATUS_CREATION_SUCCESS = True
ETL_STATUS_CREATION_FAILED = False

# ___________________job status messages_________________________
skipped = "skipped"
success = "success"
failure = "failed"
starting = "starting"
daily_job_skip = "Skipping daily jobs as its a weekend"
weekly_job_skip = "Ignoring weekly jobs since week has not ended"
job_exception = "null"
error_null = "null"
# ___________________module names__________________
CORE_JOB = "core_job"
TR_WEARHER_HISTORICAL = "tr_weather_historical"
CORE_JOB_TRANSFORM = "core_job_transform"
DQ_MISSING_CNT = "dq_missing_cnt"
DQ_CUST_STYLE_PRODUCT_MISSING_CNT = "dq_cust_style_product_missing_cnt"
DQ_MISSING_PERCENT_CNT = "dq_missing_percent_cnt"
TR_APPEND_BRAND_ID = "tr_append_brand_id"
TR_BASIC_APPEND = "tr_basic_append"
TR_ETL_RPT_MISSING_DATES = "tr_etl_rpt_missing_dates"
TR_VANS_WCS_WISHLIST = "tr_vans_wcs_wishlist"
TR_WCS_RETURN = "tr_wcs_return"
TR_WEATHER_FORECAST = "tr_weather_forecast"
RUN_FULL_FILE_CHECKLIST = "run_full_file_checklist"
MODULE_UNIDENTIFIED = "MODULE_UNIDENTIFIED"
CORE_UTILS = "utils_core"

# map_modules
MAP_ADOBE = "map_adobe"
MAP_COREMETRICS = "map_coremetrics"
MAP_CUSTOM_SHOES = "map_custom_shoes"
MAP_EXPERIAN_GOLD = "map_experian_gold"
MAP_RESPONSYS_EMAIL = "map_responsys_email"
MAP_WCS = "map_wcs"
MAP_ZETA_EMAIL = "map_zeta_email"
MAP_ZETA_PREFERENCE = "map_zeta_preference"
MAP_UTILS = "MapUtils"

# xref_modules
XR_XREF_JOB = "xr_xref_job"

# ___________________exception types_____________________________
TRANSFORMATION_EXCEPTION = "TransformationException"
DQ_EXCEPTION = "DQException"
DQ_ROW_COUNT_EXCEPTION = "DQRowCountException"
IO_S3_READ_OBJECT_EXCEPTION = "S3ReadObjectException"
IO_S3_WRITE_OBJECT_EXCEPTION = "S3WriteObjectException"
IO_DYNAMODB_READ_EXCEPTION = "DynamoDbReadException"
IO_DYNAMODB_UPDATE_EXCEPTION = "DynamoDbUpdateException"
IO_DYNAMODB_PUT_EXCEPTION = "DynamoDbPutException"
IO_READ_REDSHIFT_EXCEPTION = "ReadRedshiftException"
IO_WRITE_REDSHIFT_EXCEPTION = "WriteRedshiftException"
CORE_UTILS_EXCEPTION = "CoreUtilsException"
DYNAMODB_UTILS = "DynamoUtils"
CORE_JOB_MODULE = "CoreJob"

MAP_EXCEPTION = "MapException"
MAP_UTILS_EXCEPTION = "MapUtilsException"

XREF_EXCEPTION = "XrefException"
# __________________exception messages___________________________
TRANSFORMATION_PRECONDITION_STAGE_STATUS_EXCEPTION_MESSAGE = (
    "Not Procedding For Transformation Stage As DQ_CHECK Stage Failed"
)
TRANSFORMED_DF_DATA_DICT_EMPTY_MESSAGE = "Failed To Generate Transformed DF ,Found Transformed Dataframe Data Dict Not Loaded"
TRANSFORMED_DF_TO_REDSHIFT_TABLE_WRITE_FAILED_MESSAGE = (
    "Failed to Load Transformed Dataframe To Redshift"
)
TR_STAGE_FAILED_MESSAGE = "Transformation Stage Failed"
DQ_CHECK_STAGE_STATUS_FAILED_MESSAGE = "DQ Stage Failed Due To One Of DQ Check Failed"
S3_READ_FAILED_MESSAGE = "Failed To Read S3 Object "
S3_WRITE_FAILED_MESSAGE = "Failed To Read S3 Object "
GET_STATUS_FAILED_MESSAGE = "Error ocurred in get_status "
UPDATE_STAGE_STATUS_FAILED_MESSAGE = "Error Occured  update_stage_status "
REDSHIFT_TBL_TO_DF_FAILED_MESSAGE = "Unable to read Redshift table "
DF_TO_REDSHIFT_WRITE_FAILED = "Unable To Load Dataframe To Redshift "
DQ_ROW_COUNT_FAILURE_MESSAGE = "Number of rows in feed does not equal stored row count"
# ____________________generic constants_____________________________
EMPTY_STRING = ""
INITIAL_ERROR_INFO = "null"

# ____________________feed specific timestamp formats_______________________

ADOBE_FEED_NAME_INDICATOR = "ADOBE"
RESPONSYS_FEED_NAME_INDICATOR = "Responsys"
ADOBE_TIMESTAMP_FORMAT = "MMMMM dd, YYYY"
RESPONSYS_TIMESTAMP_FORMAT = "dd-MMM-yyyy HH:mm:ss"
