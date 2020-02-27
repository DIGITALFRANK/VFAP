# -----------------------------------------AWS CONFIGURATIONS------------
REGION = "us-east-1"


# ___________________job status messages_________________________
skipped = "skipped"
success = "success"
failure = "failed"
starting = "starting"
daily_job_skip = "Skipping daily jobs as its a weekend"
weekly_job_skip = "Ignoring weekly jobs since week has not ended"
job_exception="null"
error_null="null"


# --------------------------------DYNAMODB_CONFIGURATIONS------------------------------------
FILE_STATUS_TABLE = "vf-dev-etl-file-status"
FILE_STATUS_PRIMARY_KEY_ATTRIBUTE = "file_name"
FILE_CONFIG_PARTITION_KEY = 'data_source'
FILE_CONFIG_SORT_KEY_ATTRIBUTE = 'feed_name'
STATUS_PARTITION_KEY_ATTRIBUTE = 'file_name'
FILE_CONFIG_ATTRIBUTES_TO_BE_FETCHED = ['brand', 'brand_adj','tgt_dstn_folder_name',
                                        'tr_class_to_call', 'data_source',
                                        'dq_class_to_call', 'dq_params',
                                        'feed_name', 'file_frequency',
                                        'hasHeaders', 'isActive', 'isRequired',
                                        'raw_source_file_delimiter',
                                        'raw_source_filename',
                                        'raw_source_filetype',
                                        'raw_source_folder_name',
                                        'rf_dstn_file_delimiter',
                                        'rf_dstn_filename',
                                        'rf_dstn_filetype',
                                        'rf_dstn_folder_name',
                                        'rf_source_dir', 'region',
                                        'schema', 'tgt_destination_filename',
                                        'tgt_dstn_filetype',
                                        'tgt_dstn_folder_name',
                                        'tr_class_to_call',
                                        'tr_params', 'tgt_dstn_tbl_name']


# --------------------------------FORESEE_CONFIGURATIONS----------------------
STORE_COLUMNS_FOR_CONDITION1 = ["Engagement - Effort", "Engagement - Friendliness", "Engagement - Promptness",
                                "Environment - Appeal",
                                "Environment - Cleanliness", "Environment - Layout", "FPI", "In-Channel Purchase39",
                                "Other-Channel Purchase40", "Presentation - Accessibility", "Presentation - Labels",
                                "Assoc: Offer to Help",
                                "Assoc: Readily Available", "Assoc: Recommend Items", "Mobl Site Purch Freq",
                                "Non Accomplish Purchase_Shipping Offer",
                                "Store Purch Freq", "Task Accomplishment", "Channel Preference", "Checkout",
                                "Discourage", "Presentation - Organized",
                                "Frequency", "Gender", "Assoc: Approached", "Assoc: Checkout Ask",
                                "Assoc: Checkout Thank", "Assoc: Greeted",
                                "Assoc: Inform Promos", "Assoc: Need Assistance", "Assoc: No Assistance Reason",
                                "Satisfaction - Ideal",
                                "Satisfaction - Overall", "Service - Assistance", "Service - Availability",
                                "Service - Knowledge", "Accomplish Purchase",
                                "Price - Competitiveness", "Price - Quality", "Products - Variety",
                                "Purchase Next Time50", "Recommend Company51",
                                "Return52", "Satisfaction - Expectations", "Price - Value", "Products - Fit",
                                "Products - Interest"
                                ]
MOBILE_COLUMNS_FOR_CONDITION1 = ["Look and Feel - Appeal", "Look and Feel - Readability", "Merchandise - Appeal",
                                 "Merchandise - Variety",
                                 "Navigation - Options", "Navigation - Organized", "Product Browsing - Narrow",
                                 "Product Browsing - Sort",
                                 "Sat - Ideal", "Sat - Overall", "Site Performance - Errors",
                                 "Site Performance - Loading"
                                 ]
BROWSER_COLUMNS_FOR_CONDITION1 = ["Brand Preference11", "Look and Feel - Readability", "Look and Feel - Appeal",
                                  "Look and Feel - Balance",
                                  "Merchandise - Appeal", "Merchandise - Availability", "Merchandise - Variety",
                                  "Navigation - Layout", "Navigation - Options",
                                  "Navigation - Organized", "Price - Competitiveness", "Price - Fairness",
                                  "Product Browsing - Features", "Product Browsing - Narrow",
                                  "Product Browsing - Sort", "Product Images - Details", "Product Images - Realistic",
                                  "Product Images - Views", "Purchase Offline12",
                                  "Purchase Online13", "Satisfaction - Expectations", "Satisfaction - Ideal",
                                  "Satisfaction - Overall", "Site Performance - Consistency",
                                  "Site Performance - Errors", "Site Performance - Loading"
                                  ]
STRORE_COLUMNS_FOR_CONDITION2 = ["Visit Reason", "Wait Time to Purch", "Web Purch Freq", "Why Not Purchase"]

MOBILE_COLUMNS_FOR_CONDITION2 = ["Age", "Gender"]

BROWSER_COLUMNS_FOR_CONDITION2 = ["Age", "Gender"]

BROWSER_COLUMNS_FOR_CONDITION3 = ["SV - Rank 1", "SV - Rank 2", "SV - Rank 3"]

STORE_COLUMNS_FOR_MASTER_DATAFRAME = ["Engagement - Effort", "Satifaction-Overall", "Purchase_Products",
                                      "Purchase_from_Store", "Engagement - Promptness", "Environment - Appeal"]
MOBILE_COLUMNS_FOR_MASTER_DATAFRAME = ["Look and Feel - Appeal", "Satifaction-Overall", "Purchase_Products",
                                       "Purchase_from_Store", "Merchandise - Variety"]
BROWSER_COLUMNS_FOR_MASTER_DATAFRAME = ["Price - Competitiveness", "Satifaction-Overall", "Purchase_Products",
                                        "Purchase_from_Store", "Site Performance - Consistency",
                                        "Merchandise - Variety"]

# -----------------------------------------------------------------------------------------------------------

LOG_DIR = "glue_process/"
LOG_FILE = "./tmp/process.log"


#_________________DDB etl status creation message________________

ETL_STATUS_CREATION_SUCCESS = True
ETL_STATUS_CREATION_FAILED = False

# ---------------------------STAGE_STATUS_CONFIGURATIONS----------------------------------------------------
STAGE_COMPLETED_STATUS = "completed"
STAGE_NOT_STARTED_STATUS = "started"
STAGE_FAILED_STATUS = "failed"
STAGE_NOT_APPLICABLE_STATUS = "not_applicable"
STAGE_ERROR_NULL = "Null"
STAGE_REFINED_TO_TRANSFORM_INITIAL_STATUS_PARAMS = {
    "error_info": STAGE_ERROR_NULL,
    "status": STAGE_NOT_STARTED_STATUS,
    "update_dttm": "Null",
}