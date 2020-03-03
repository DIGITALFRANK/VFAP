# -----------------xr_build_style_xref_clean_tnf CONFIGURATIONS----------------
clean_tnf_xref_basic = {
    "TNF___CLM_DESCRIPTION_FOR_SAS_startswith_M": {
        "condition": "M ",
        "result": {"gender": "M", "age_group": "ADULT", "product_category_index": 3},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_startswith_W": {
        "condition": "W ",
        "result": {"gender": "F", "age_group": "ADULT", "product_category_index": 3},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_startswith_B": {
        "condition": "B ",
        "result": {"gender": "M", "age_group": "KID", "product_category_index": 3},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_startswith_G": {
        "condition": "G ",
        "result": {"gender": "F", "age_group": "KID", "product_category_index": 3},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_YOUTH_BOY": {
        "condition": "YOUTH BOY",
        "result": {
            "gender": "M",
            "age_group": "KID",
            "product_category_replace": ["YOUTH BOY", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_YOUTH_GIRL": {
        "condition": "YOUTH GIRL",
        "result": {
            "gender": "F",
            "age_group": "KID",
            "product_category_replace": ["YOUTH GIRL", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_YOUTH": {
        "condition": "YOUTH",
        "result": {"age_group": "KID", "product_category_replace": ["YOUTH", "KID"]},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_TODDLER_BOY": {
        "condition": "TODDLER BOY",
        "result": {
            "gender": "M",
            "age_group": "KID",
            "product_category_replace": ["TODDLER BOY", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_TODDLER_GIRL": {
        "condition": "TODDLER GIRL",
        "result": {
            "gender": "F",
            "age_group": "KID",
            "product_category_replace": ["TODDLER GIRL", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_TODDLER": {
        "condition": "TODDLER",
        "result": {"age_group": "KID", "product_category_replace": ["TODDLER", "KID"]},
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_INFANT_BOY": {
        "condition": "INFANT BOY",
        "result": {
            "gender": "M",
            "age_group": "KID",
            "product_category_replace": ["INFANT BOY", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_INFANT_GIRL": {
        "condition": "INFANT GIRL",
        "result": {
            "gender": "F",
            "age_group": "KID",
            "product_category_replace": ["INFANT GIRL", "KID"],
        },
    },
    "TNF___CLM_DESCRIPTION_FOR_SAS_indexof_INFANT": {
        "condition": "INFANT",
        "result": {"age_group": "KID", "product_category_replace": ["INFANT", "KID"]},
    },
    "STYLE_DESCRIPTION_find_JACKET": {
        "substring": "JACKET",
        "concatenate": {"initial_part": True, "end_part": True},
    },
    "STYLE_DESCRIPTION_find_PANT": {
        "substring": "PANT",
        "concatenate": {"initial_part": True, "end_part": True},
    },
    "STYLE_DESCRIPTION_find_SWEATER": {
        "substring": "SWEATER",
        "concatenate": {"initial_part": True, "end_part": False},
    },
    "JKT_SEARCH": {"substring": " JKT-", "replace_with": "JACKET-"},
    "SWEATS_SEARCH": {
        "substring1": " SWEAT",
        "substring2": "SWEATSHIRT",
        "replace_with": "SWEATER",
    },
    "TRIJACKET_SEARCH": {
        "substring": "TRIJACKET",
        "replace_with": "TRICLIMATE JACKET",
        "concatenate": {"initial_part": True, "end_part": True},
    },
    "RTO_SEARCH": {"substring": ["-RTO", " -RTO"], "replace_with": " RTO"},
    "LIST_TRICLIMATE": {
        "substring": [
            "TRI",
            "TRIC",
            "TRICL",
            "TRICLI",
            "TRICLIM",
            "TRICLIMA",
            "TRICLIMAT",
        ],
        "replace_with": "TRICLIMATE",
    },
    "LIST_HYBRID": {"substring": ["HYB", "HYBD", "HYBR"], "replace_with": "HYBRID"},
    "LIST_PULLOVER": {"substring": ["PLVR"], "replace_with": "PULLOVER"},
    "LIST_HOODIE": {"substring": ["HOOD", "HOODI"], "replace_with": "HOODIE"},
    "LIST_PARKA": {"substring": ["PKA", "PRKA"], "replace_with": "PARKA"},
    "LIST_INSULATED": {
        "substring": ["INS", "INS.", "INSU", "INSUL"],
        "replace_with": "INSULATED",
    },
    "LIST_ANORAK": {"substring": ["AN"], "replace_with": "ANORAK"},
    "LIST_JACKET": {
        "substring": ["J", "JK", "JAC", "JKT", "JCK", "JCKT", "JACK", "JACKE"],
        "replace_with": "JACKET",
    },
    "LIST_JACKETRTO": {"substring": ["JACKETRTO"], "replace_with": "JACKET RTO"},
    "LIST_THERMOBALL": {
        "substring": ["THERMOB", "THERMOBA", "THERMOBAL", "THMBALL"],
        "replace_with": "THERMOBALL",
    },
    "LIST_THERMAL": {"substring": ["THML", "THRML"], "replace_with": "THERMAL"},
    "LIST_PLASMA": {"substring": ["PLSMA"], "replace_with": "PLASMA"},
    "LIST_DOWN": {"substring": ["DWN"], "replace_with": "DOWN"},
    "LIST_SNOW": {"substring": ["SNW"], "replace_with": "SNOW"},
    "LIST_FLANNEL": {"substring": ["FLAN"], "replace_with": "FLANNEL"},
    "LIST_SHIRT": {"substring": ["SHRT"], "replace_with": "SHIRT"},
    "LIST_TODDLER": {"substring": ["TOD", "TODD"], "replace_with": "TODDLER"},
    "LIST_PANT": {"substring": ["PNT"], "replace_with": "PANT"},
    "LIST_TANK": {"substring": ["TNK"], "replace_with": "TANK"},
    "LIST_SWEATER": {"substring": ["SWTR"], "replace_with": "SWEATER"},
    "LIST_NECK": {"substring": ["NCK"], "replace_with": "NECK"},
    "LIST_VNECK": {"substring": ["VNCK"], "replace_with": "V-NECK"},
    "LIST_CARGO": {"substring": ["CRGO"], "replace_with": "CARGO"},
    "GLOVES_SEARCH": {"substring": ["GLVOES"], "replace_with": "GLOVES"},
    "GLOVESCLIMBING_SEARCH": {
        "substring": ["GLOVESCLIMBING"],
        "replace_with": "GLOVES CLIMBING",
    },
}

clean_tnf_xref_final = {
    "acting_column_product_category": {
        "column_name": "tnf_sas_product_category",
        "replacements": {
            "OUTERWEAR": "OW",
            "SPORTSWEAR": "SW",
            "PERFORMANCE": "PERF",
            "ACTIONSPORTS": "AS",
            "HIKING": "HIK",
            "MOUNTAIN CULTURE": "MTN_CUL",
            "MOUNTAINEERING/CLIMBING": "MTN_CLM",
            "RUNNING": "RUN",
            "SPT_": "",
            "SNOWSPORTS": "SNW_SPRT",
            "TRAINING": "TRN",
            "BOTTOMS": "BOT",
            "SPORTS": "SPRT",
            "CULTURE": "CUL",
            "CULTUR": "CUL",
            "BACKPACKS": "BCPK",
            "EXPLORATION": "EXP",
            "BASELAYERS": "BLAYER",
            "FREERIDE": "FREE",
            "FOOTWEAR": "FW",
            "GLOVESCLIMBING": "GLOVES_CLM",
            "GLOVES_CLIMBING": "GLOVES_CLM",
            "OUTSIDE": "OUT",
            "SNOW": "SNW",
            "VENDOR": "VEND",
            "INSULATED": "INS",
            "SLEEPING_BAG": "SLEEP_BAG",
            "CLIMBING": "CLM",
            "SOFTSHELL": "SS",
            "ACCESSORIES": "ACC",
            "TECH_HYDRATION": "TECH_HYD",
            "URBAN": "URB",
            "SMARTWOOL": "SWOOL",
            "N/A": "N_A",
            "_": "",
        },
    },
    "acting_column_new_end_use_sport": {
        "column_name": "newendusesport",
        "replacements": {
            "HIKING": "HIK",
            "MOUNTAIN CULTURE": "MTN_CUL",
            "ACTIONSPORTS": "AS",
            "HIKING": "HIK",
            "MOUNTAIN CULTURE": "MTN_CUL",
            "MOUNTAINEERING/CLIMBING": "MTN_CLM",
            "RUNNING": "RUN",
            "SNOWSPORTS": "SNW_SPRT",
            "TRAINING": "TRN",
            "URBAN EXPLORATION": "URB_EXP",
            "EXPLORATION": "EXP",
            "NOT USED": "NOTUSED",
        },
    },
    "VENDOR_CD_4": {"substring": ("AHDB", "DBG0"), "result": "FW_CUL"},
    "condition_consumer_segment": {
        "substring": "newconsumersegmentterritory",
        "equality": [
            "MOUNTAIN ATHLETICS",
            "MOUNTAIN SPORTS",
            ["MOUNTAIN CULTURE", "MOUNTAIN LIFESTYLE"],
            "URBAN EXPLORATION",
        ],
        "result": "TRGT_MA_FLAG=1",
        "isArray": False,
    },
}

# ------------------------------CUST XREF---------------------------------------
CUST_SAS_BRAND_ID_4 = 4
CUST_SAS_BRAND_ID_7 = 7

# -------------------------------------------------------------------------------
