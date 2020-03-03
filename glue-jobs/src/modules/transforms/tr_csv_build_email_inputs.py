from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
from datetime import datetime, timedelta
from pyspark.sql.types import *
from pyspark.sql.types import StructField, StructType, StringType


class tr_csv_build_email_inputs(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_csv_build_email_inputs

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """

        print("inside tr csv build email inputs constructor")
        super(tr_csv_build_email_inputs, self).__init__(file_name)
        self.whouse_tnf_email_launch_view_df = self.read_from_s3(
            bucket="vf-datalake-dev-sandbox", path="CLM/launch_view/"
        )
        self.whouse_tnf_email_sent_view_1df = self.read_from_s3(
            bucket="vf-datalake-dev-sandbox", path="CLM/sent_view/"
        )
        self.whouse_tnf_email_open_view_1df = self.read_from_s3(
            bucket="vf-datalake-dev-sandbox", path="CLM/open_view/"
        )
        self.whouse_tnf_email_click_view_1df = self.read_from_s3(
            bucket="vf-datalake-dev-sandbox", path="CLM/click_view/"
        )
        self.whouse_etl_parm = self.read_from_s3(
            bucket="vf-datalake-dev-sandbox", path="CLM/ETL_PARMS/"
        )

    def util_read_etl_parm_table(self):
        spark = self.spark
        print("enter into util_read_etl_parm_table")
        today = datetime.today()
        calculated_date = today - timedelta(days=(today.weekday() - 1))
        _brand_name_prefix = "VANS"
        ##Uncomment this line to run the CSV on a day that is out of the week from where is supposed to run, comment the line above
        # calculated_date = today - timedelta(days=(today.weekday()+6))
        whouse_etl_parm = self.whouse_etl_parm
        whouse_etl_parm.createOrReplaceTempView("whouse_etl_parm_view")
        df = spark.sql(
            """select 
                case when UPPER(TRIM(CHAR_VALUE))= "CURRENT" then date_format('{}',"yyyy-MM-dd")
                else date_format(CHAR_VALUE,"yyyy-MM-dd")
                end as cutoff_date
            from whouse_etl_parm_view
            where LOWER(KEY2)="cutoff_date"
            AND LOWER(KEY1) = LOWER('{}')
            AND CHAR_VALUE IS NOT NULL
            AND NUM_VALUE=1""".format(
                calculated_date, _brand_name_prefix
            )
        )
        print("end of util_read_etl_parm_table")
        df.createOrReplaceTempView("date_table")

        cutoff_date_df = spark.sql(
            """select date_format(cutoff_date,"ddMMMyyyy") as cutoff_date from date_table where cutoff_date BETWEEN date_format('2000-01-01',"yyyy-MM-dd") AND date_format(current_date(),"yyyy-MM-dd") AND cutoff_date IS NOT NULL"""
        )

        cut_off_list = cutoff_date_df.select("cutoff_date").collect()
        _cutoff_date = cut_off_list[0].cutoff_date

        return _cutoff_date

    def transform(self, df):
        """
        """
        print(" Applying tnf_build_email_inputs ")
        full_load_df = None

        try:
            print("enter into try block")
            spark = self.spark
            params = self.params
            logger = self.logger
            _cutoff_date = self.util_read_etl_parm_table()

            whouse_tnf_email_launch_view_df = self.whouse_tnf_email_launch_view_df
            whouse_tnf_email_sent_view_1df = self.whouse_tnf_email_sent_view_1df
            whouse_tnf_email_open_view_1df = self.whouse_tnf_email_open_view_1df
            whouse_tnf_email_click_view_1df = self.whouse_tnf_email_click_view_1df

            whouse_tnf_email_sent_view_df = (
                whouse_tnf_email_sent_view_1df.withColumn(
                    "event_captured_dt_con",
                    F.concat(
                        whouse_tnf_email_sent_view_1df["event_captured_dt"].substr(
                            0, 2
                        ),
                        F.lit("-"),
                        whouse_tnf_email_sent_view_1df["event_captured_dt"].substr(
                            3, 3
                        ),
                        F.lit("-"),
                        whouse_tnf_email_sent_view_1df["event_captured_dt"].substr(
                            6, 4
                        ),
                    ),
                )
                .drop("event_captured_dt")
                .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
            )

            whouse_tnf_email_open_view_df = (
                whouse_tnf_email_open_view_1df.withColumn(
                    "event_captured_dt_con",
                    F.concat(
                        whouse_tnf_email_open_view_1df["event_captured_dt"].substr(
                            0, 2
                        ),
                        F.lit("-"),
                        whouse_tnf_email_open_view_1df["event_captured_dt"].substr(
                            3, 3
                        ),
                        F.lit("-"),
                        whouse_tnf_email_open_view_1df["event_captured_dt"].substr(
                            6, 4
                        ),
                    ),
                )
                .drop("event_captured_dt")
                .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
            )

            whouse_tnf_email_click_view_df = (
                whouse_tnf_email_click_view_1df.withColumn(
                    "event_captured_dt_con",
                    F.concat(
                        whouse_tnf_email_click_view_1df["event_captured_dt"].substr(
                            0, 2
                        ),
                        F.lit("-"),
                        whouse_tnf_email_click_view_1df["event_captured_dt"].substr(
                            3, 3
                        ),
                        F.lit("-"),
                        whouse_tnf_email_click_view_1df["event_captured_dt"].substr(
                            6, 4
                        ),
                    ),
                )
                .drop("event_captured_dt")
                .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
            )

            print("df's defined")
            whouse_tnf_email_launch_view_df.createOrReplaceTempView(
                "whouse_tnf_email_launch_view"
            )
            whouse_tnf_email_sent_view_df.createOrReplaceTempView(
                "whouse_tnf_email_sent_view"
            )
            whouse_tnf_email_open_view_df.createOrReplaceTempView(
                "whouse_tnf_email_open_view"
            )
            whouse_tnf_email_click_view_df.createOrReplaceTempView(
                "whouse_tnf_email_click_view"
            )

            tmp_tnf_email_launch_clean_csv_df1 = (
                spark.sql(
                    """ 
                                    SELECT *,
                                        CASE WHEN UPPER(launch_type) in ('S', 'P', 'R') AND UPPER(launch_status)=='C'
                                            THEN UPPER(campaign_name) 
                                            ELSE campaign_name
                                       END AS campaign_name_tmp,
                                       CASE WHEN UPPER(launch_type) in ('S', 'P', 'R') AND UPPER(launch_status)=='C'
                                            THEN UPPER(subject) 
                                            ELSE subject
                                       END AS  subject_tmp
                                    FROM whouse_tnf_email_launch_view """
                )
                .drop("campaign_name", "subject")
                .withColumnRenamed("campaign_name_tmp", "CAMPAIGN_NAME")
                .withColumnRenamed("subject_tmp", "SUBJECT")
            )

            tmp_tnf_email_launch_clean_csv_df1.createOrReplaceTempView(
                "tmp_tnf_email_launch_clean_csv_1"
            )

            print(
                "count of records in tmp_tnf_email_launch_clean_csv_df1 {}".format(
                    tmp_tnf_email_launch_clean_csv_df1.count()
                )
            )

            tmp_tnf_email_launch_clean_csv_df11 = spark.sql(
                """
                                    SELECT * FROM tmp_tnf_email_launch_clean_csv_1
                                    where INSTR(campaign_name,"UNSUB") <= 0 AND
                                    INSTR(campaign_name,"SHIPPING") <= 0 AND
                                    INSTR(campaign_name,"SHIP_") <= 0 AND
                                    INSTR(campaign_name,"PARTIALSHIP") <= 0 AND
                                    INSTR(campaign_name,"PARTIAL_SHIP") <= 0 AND
                                    INSTR(campaign_name,"REVIEW") <= 0 AND
                                    INSTR(campaign_name,"RETURN") <= 0 AND
                                    INSTR(campaign_name,"EXCHANGE") <= 0 AND
                                    INSTR(campaign_name,"CANCEL") <= 0 AND
                                    INSTR(campaign_name,"CONFIRM") <= 0 AND
                                    INSTR(campaign_name,"PREFERENCECENTER") <= 0 AND
                                    INSTR(campaign_name,"TEST") <= 0 AND
                                    INSTR(campaign_name,"SHIP-ACCOM") <= 0 AND
                                    INSTR(campaign_name,"OUTOFSTOCK") <= 0 AND
                                    INSTR(campaign_name,"USSHIPTOSTORE") <= 0 AND
                                    INSTR(subject,"TEST") <= 0 AND
                                    INSTR(subject,"TRIGGERED") <= 0"""
            )

            tmp_tnf_email_launch_clean_csv_df11.createOrReplaceTempView(
                "tmp_tnf_email_launch_clean_csv_11"
            )

            tmp_tnf_email_launch_clean_csv_df2 = (
                spark.sql(
                    """ SELECT *,
                            CASE WHEN INSTR(campaign_name,"FALL") > 0 OR 
                                        INSTR(subject,"FALL") > 0 OR 
                                        INSTR(subject,"OCTOBER") > 0 OR
                                        INSTR(subject,"ROCK FEST") > 0 
                                    THEN "F"
                                WHEN INSTR(campaign_name,"SPRING") > 0 
                                    THEN "P"
                                WHEN INSTR(campaign_name,"WINTER SALE") > 0 OR
                                        INSTR(campaign_name,"FEB-") > 0 OR 
                                        INSTR(subject,"OMNI_SALE") > 0 
                                    THEN "WS"
                                WHEN INSTR(campaign_name,"WINTER") > 0 OR  
                                        INSTR(subject,"WINTER") > 0 OR 
                                        INSTR(campaign_name,"NEWYEAR") > 0 OR 
                                        INSTR(subject,"NEW YEAR") > 0 OR 
                                        INSTR(subject,"NEW_YEAR") > 0 
                                    THEN "W"
                                WHEN INSTR(campaign_name,"SS-") > 0 
                                    THEN "SS"
                                WHEN INSTR(campaign_name,"HOLIDAY") > 0 OR 
                                        INSTR(campaign_name,"GIFT") > 0 OR 
                                        INSTR(subject,"GIFT") > 0 OR 
                                        INSTR(campaign_name,"DECEMBER") > 0 OR 
                                        INSTR(campaign_name,"BLACKFRIDAY") > 0  OR 
                                        INSTR(subject,"UNDER_100") > 0 OR 
                                        INSTR(subject,"BLACK FRI") > 0  OR 
                                        INSTR(campaign_name,"BLACK-FRI") > 0 OR 
                                        INSTR(campaign_name,"CYBER_MONDAY") > 0 OR 
                                        INSTR(campaign_name,"CYBER-MONDAY") > 0 OR 
                                        INSTR(campaign_name,"CYBERMONDAY") > 0 OR 
                                        INSTR(subject,"CYBER MONDAY") > 0 OR 
                                        INSTR(subject,"CYBER_MONDAY") > 0 OR 
                                        INSTR(subject,"TAX FREE") > 0 
                                    THEN "H"
                                WHEN INSTR(campaign_name,"BTS") > 0 OR
                                        INSTR(campaign_name,"SUMMER") > 0 OR 
                                        INSTR(subject,"SUMMER") > 0 OR
                                        INSTR(subject,"FATHERSDAY") > 0 OR
                                        INSTR(campaign_name,"FATHERSDAY") > 0 
                                    THEN "B"
                            ELSE ""
                            END AS email_ssn,

                            CASE WHEN INSTR(campaign_name,"GO-VACA") > 0 OR 
                                        INSTR(campaign_name,"_NATL_PARKS") > 0 OR 
                                        INSTR(subject,"EXPLORE IN") > 0 OR 
                                        INSTR(campaign_name,"NATIONALPARK") > 0 OR 
                                        INSTR(campaign_name,"BEST-OF-THE-BAY") > 0 
                                    THEN  "TRAVEL"
                                WHEN INSTR(campaign_name,"RUN") > 0 OR  
                                        INSTR(campaign_name,"_ECS_") > 0 OR 
                                        INSTR(campaign_name,"GOLIATHON") > 0 OR 
                                        INSTR(subject,"MARATHON") > 0 OR 
                                        INSTR(subject,"RUN") > 0 OR 
                                        INSTR(campaign_name,"OE_FUND") > 0 OR 
                                        INSTR(subject,"ENDURAN") > 0 OR 
                                        INSTR(subject,"LACE UP FOR") > 0 
                                    THEN  "RUN"
                                WHEN INSTR(campaign_name,"TRAIN") > 0 OR 
                                        INSTR(subject,"GYM") > 0 OR 
                                        INSTR(subject,"EQUIPPED") > 0 OR 
                                        INSTR(campaign_name,"WORKOUT") > 0 OR 
                                        INSTR(subject,"CROSS FIT")  > 0 OR 
                                        INSTR(subject,"XFITMN") > 0 
                                    THEN  "TRN"
                                WHEN INSTR(campaign_name,"HIK") > 0 OR 
                                        INSTR(subject,"HIK") > 0 OR 
                                        INSTR(subject,"TRAIL") > 0 
                                    THEN  "HIK"
                                WHEN INSTR(campaign_name,"WATER") > 0 OR 
                                        INSTR(campaign_name,"GO-SF") > 0 
                                    THEN "SURF"
                                WHEN INSTR(campaign_name,"CLIMB") > 0 OR 
                                        INSTR(subject,"PREPARED FOR THE MOUNTAIN") > 0 OR 
                                        INSTR(campaign_name,"SUMMIT") > 0 OR 
                                        INSTR(campaign_name,"NEPAL")> 0 OR 
                                        INSTR(campaign_name,"MERU") > 0    OR 
                                        INSTR(campaign_name,"BANFF") > 0 OR 
                                        INSTR(campaign_name,"MTN-D-DOW") > 0 OR 
                                        INSTR(campaign_name,"ANGOLA") > 0 OR  
                                        INSTR(campaign_name,"ALPINE") > 0 OR 
                                        INSTR(subject,"ALPINE") > 0 OR 
                                        INSTR(subject,"CLIMB") > 0 OR 
                                        INSTR(subject,"CONRAD ANKER") > 0 OR 
                                        INSTR(subject,"ALEX HONNOLD") > 0 
                                    THEN  "MTNCLM"
                                WHEN INSTR(campaign_name,"HIPCAMP") > 0 OR 
                                        INSTR(campaign_name,"CAMPING") > 0 OR 
                                        INSTR(campaign_name,"BACKPACK") > 0 OR 
                                        INSTR(subject,"BACKPACK") > 0 OR 
                                        INSTR(subject,"TENT") > 0 OR 
                                        INSTR(subject,"HOMESTEAD") > 0 OR 
                                        INSTR(campaign_name,"HOMESTEAD") > 0 OR 
                                        INSTR(subject,"CAMP") > 0 
                                    THEN  "BCPKCAMP"
                                WHEN INSTR(campaign_name,"SKI") > 0 OR  
                                        INSTR(campaign_name,"ALL-MTN") > 0 OR 
                                        INSTR(subject,"MEET INGRID") > 0 OR 
                                        INSTR(subject,"DESLAURIERS") > 0 OR 
                                        INSTR(subject,"SKI") > 0 OR 
                                        INSTR(campaign_name,"SNOWSPORTS") > 0 OR 
                                        INSTR(SUBJECT,"SLOPE") > 0 OR
                                        INSTR(SUBJECT,"STEEP") > 0 
                                    THEN  "SKI"
                                WHEN INSTR(campaign_name,"SNOW") > 0 OR 
                                        INSTR(campaign_name,"SNOWSPORTS") > 0 OR 
                                        INSTR(subject,"KAITLYN FARRINGTON") > 0 
                                    THEN  "SNWB"
                                WHEN INSTR(campaign_name,"YOGA") > 0 OR 
                                        INSTR(subject,"YOGA") > 0 
                                    THEN  "YOGA"
                                WHEN INSTR(campaign_name,"BOXING") > 0 OR 
                                        INSTR(SUBJECT,"BOXING") > 0 
                                THEN  "BOXING"
                                WHEN INSTR(subject,"HUNT-SEA") > 0 OR 
                                        INSTR(campaign_name,"HUNT-SEA") > 0 
                                THEN  "WATER"
                            ELSE ""
                            END AS email_activity,

                            CASE WHEN INSTR(campaign_name,"-MEN") > 0 THEN "M"
                                WHEN INSTR(campaign_name,"-WOMEN") > 0 THEN "F"
                            ELSE ""
                            END AS email_gender,

                            CASE WHEN INSTR(campaign_name,"RETAIL") > 0 OR  
                                        INSTR(subject,"RETAIL") > 0 
                                    THEN  "RETAIL"
                                    WHEN INSTR(campaign_name,"ECOM") > 0 OR 
                                            INSTR(subject,"ECOM") > 0 OR  
                                            INSTR(campaign_name,"NEW_SITE") > 0 
                                        THEN "ECOM"					
                                    WHEN INSTR(campaign_name,"OUTLET") > 0 OR  
                                            INSTR(subject,"OUTLET") > 0  
                                        THEN "OUTLET"
                            ELSE ""
                            END AS email_channel,

                            CASE WHEN  INSTR(campaign_name,"EQUIPMENT") > 0 OR 
                                        INSTR(subject,"EQUIPPED") > 0 OR 
                                        INSTR(subject,"GEAR") > 0 
                                    THEN  "EQUIP"
                                WHEN INSTR(campaign_name,"JACKET") > 0 OR 
                                        INSTR(subject,"JACKET") > 0 OR 
                                        INSTR(campaign_name,"WATSON") > 0 
                                    THEN  "JKT"
                                WHEN INSTR(campaign_name,"BOOT") > 0 OR 
                                        INSTR(campaign_name,"XTRAFOAM") > 0 OR 
                                        INSTR(subject,"FOOTWEAR") > 0 
                                    THEN  "FW"
                                WHEN INSTR(campaign_name,"BACKPACK") > 0 OR 
                                        INSTR(campaign_name,"DAY-PACK") > 0 OR 
                                        INSTR(subject,"DAY-PACK") > 0 OR 
                                        INSTR(subject,"BACKPACK") > 0 
                                    THEN  "BCPK"
                                WHEN INSTR(campaign_name,"ASCENTIAL") > 0 THEN "ASCNTL"		
                                WHEN INSTR(campaign_name,"THERM") > 0 OR  
                                        INSTR(subject,"3 WAYS") > 0 OR  
                                        INSTR(subject,"COLD") > 0 OR 
                                        INSTR(campaign_name,"COLD") > 0 OR 
                                        INSTR(campaign_name,"WINTERJACKET") > 0 OR 
                                        INSTR(campaign_name,"DOWN_JACKET") > 0 OR 
                                        INSTR(campaign_name,"SUMMIT") > 0	OR 
                                        INSTR(campaign_name,"_FUSE_CHI_") > 0 OR 
                                        INSTR(campaign_name,"_FUSE_SEATTLE") > 0 OR 
                                        INSTR(campaign_name,"_FUSE_BOSTON_") > 0 OR 
                                        INSTR(campaign_name,"APEX-FLEX") > 0 OR 
                                        INSTR(SUBJECT,"FAR-NORTH") > 0 OR 
                                        INSTR(SUBJECT,"FAR NORTH") > 0 OR 
                                        INSTR(campaign_name,"FARNORTHERN") > 0 OR 
                                        INSTR(campaign_name,"INSULATED") > 0 OR 
                                        INSTR(campaign_name,"URBAN_INS") > 0 OR  
                                        INSTR(campaign_name,"ALPINE") > 0 OR 
                                        INSTR(campaign_name,"_SOFT_") > 0 OR 
                                        INSTR(campaign_name,"URBAN-INS") > 0 OR 
                                        INSTR(campaign_name,"CORE") > 0 OR 
                                        INSTR(campaign_name,"TBALL") > 0 OR 
                                        INSTR(subject,"TBALL") > 0 OR 
                                        INSTR(subject,"THERMOBALL") > 0 OR 
                                        INSTR(campaign_name,"ARCTIC") > 0 OR 
                                        INSTR(subject,"ARCTIC") > 0 OR  
                                        INSTR(subject,"NEW DIMENSION TO WARMTH") > 0 OR  
                                        INSTR(subject,"NEW DIMENSION OF WARMTH") > 0 
                                    THEN "INS"
                                WHEN INSTR(campaign_name,"FLEECE") > 0 OR 
                                        INSTR(campaign_name,"URBAN_EXP") > 0 OR 
                                        INSTR(campaign_name,"TRICLIM") > 0 OR 
                                        INSTR(campaign_name,"VILLAGEWEAR") > 0 OR 
                                        INSTR(campaign_name,"OSITO") > 0 OR 
                                        INSTR(campaign_name,"WARMTH") > 0 OR 
                                        INSTR(campaign_name,"FAVES") > 0 OR 
                                        INSTR(subject,"FLEECE PONCHO") > 0 OR 
                                        INSTR(subject,"LIGHTER JACKET") > 0 OR 
                                        INSTR(campaign_name,"DENALI") > 0 
                                    THEN  "MILDJKT"
                                WHEN INSTR(campaign_name,"_FUSEFORM_") > 0 OR 
                                        INSTR(campaign_name,"_VENTURE_") > 0 OR 
                                        (INSTR(campaign_name,"RAIN") > 0 AND INSTR(campaign_name,"TRAIN") <= 0) OR
                                        (INSTR(subject,"RAIN") > 0 AND INSTR(subject,"TRAIN") <= 0) 
                                    THEN "RAIN_WR"
                                WHEN INSTR(subject,"HAT") > 0 OR 
                                        INSTR(subject,"BEANIE") > 0 OR 
                                        INSTR(subject,"EAR GEAR") > 0 OR 
                                        INSTR(subject,"MITTEN") > 0 OR 
                                        INSTR(subject,"SCARF") > 0 OR 
                                        INSTR(subject,"VISOR") > 0 OR 
                                        INSTR(subject," CAP ") > 0  OR 
                                        INSTR(subject,"GLOVES") > 0   OR 
                                        INSTR(subject,"SOCKS") > 0 OR 
                                        (INSTR(subject,"PACK") > 0 AND INSTR(subject,"BACKPACK") <= 0 ) OR 
                                        INSTR(subject," BAG") > 0 OR 
                                        INSTR(subject,"BOTTLE") > 0 
                                    THEN "ACCSR"			
                            ELSE ""
                            END AS Product_category_tmp
                        FROM tmp_tnf_email_launch_clean_csv_11
                            """
                )
                .drop("Product_category")
                .withColumnRenamed("Product_category_tmp", "Product_category")
            )

            tmp_tnf_email_launch_clean_csv_df2.createOrReplaceTempView(
                "tmp_tnf_email_launch_clean_csv_2"
            )
            print(
                "count of records in tmp_tnf_email_launch_clean_csv_df2 {}".format(
                    tmp_tnf_email_launch_clean_csv_df2.count()
                )
            )

            tmp_tnf_email_launch_clean_csv_df3 = spark.sql(
                """ SELECT *,
                                    CASE WHEN INSTR(campaign_name,"OUTDOOR") > 0 OR 
                                        INSTR(subject,"OUTDOOR") > 0  OR 
                                        INSTR(campaign_name,"OUTERWEAR") > 0 OR  
                                        INSTR(subject,"EXPLORATION") > 0 OR  
                                        INSTR(subject,"GO OUTSIDE") > 0 OR  
                                        INSTR(subject,"GET OUTSIDE") > 0 OR 
                                        INSTR(subject,"OUTERWEAR") > 0 OR 
                                        INSTR(subject,"SEEFORYOURSELF") > 0 OR 
                                        INSTR(campaign_name,"SEEFORYOURSELF") > 0 
                                    THEN  "OUTDOOR"
                                WHEN INSTR(campaign_name,"ADVENTUR") > 0 OR 
                                        INSTR(subject,"ADVENTUR") > 0 OR 
                                        INSTR(subject,"SUPERHERO") > 0 OR 
                                        INSTR(subject,"SPEAKER") > 0 OR 
                                        INSTR(subject,"CROWN") > 0 OR 
                                        INSTR(campaign_name,"CROWN") > 0 OR 
                                        INSTR(subject,"ULTIMATE EXPLORATION")  > 0 OR 
                                        INSTR(campaign_name,"FILM")> 0 OR 
                                        INSTR(campaign_name,"VALLEY")> 0 OR 
                                        INSTR(subject,"FACE SPEAK") > 0 OR 
                                        INSTR(subject,"FILM") > 0 OR 
                                        INSTR(subject,"PROGRES") > 0 OR 
                                        INSTR(campaign_name,"-FLIP-") > 0  OR 
                                        INSTR(subject,"MADNESS") > 0 OR 
                                        INSTR(CAMPAIGN_NAME,"EXPLORE-FUND") > 0 OR 
                                        INSTR(subject,"EXPLORE-FUND") > 0 	OR 
                                        INSTR(subject,"EXPLORE FUND") > 0 	OR 
                                        TRIM(email_activity) in ("TRAVEL", "RUN", "HIK", 
                                        "TRAIN", "SURF", "MTNCLM", "BCPK_CAMP", "SKI","SNWB")	OR 
                                        INSTR(subject,"TUNE IN LIVE") > 0 OR 
                                        INSTR(subject,"PREPARED FOR THE MOUNTAIN") > 0	OR 
                                        INSTR(subject,"DESLAURIERS") > 0 OR 
                                        INSTR(campaign_name,"_SS_") > 0 OR 
                                        INSTR(subject,"SS_LIVE") > 0 OR 
                                        INSTR(campaign_name,"SS_LIVE") > 0 
                                    THEN  "PE"
                                WHEN (INSTR(campaign_name,"MA") > 0 
                                        AND INSTR(campaign_name,"MAIL") <= 0 )	OR 
                                        INSTR(campaign_name,"MTATHLETICS") > 0 OR 
                                        INSTR(subject,"MOUNTAIN ATHLETICS") > 0 
                                    THEN  "MA"
                                WHEN INSTR(subject,"RECYCLE") > 0 OR 
                                        INSTR(campaign_name,"BACKYARD") > 0 OR 
                                        INSTR(campaign_name,"EARTH_DAY") > 0 OR 
                                        INSTR(subject,"EARTH DAY") > 0
                                    THEN "NL"
                                WHEN INSTR(campaign_name,"YOUTH") > 0 OR 
                                        INSTR(campaign_name,"KID") > 0 OR 
                                        INSTR(subject,"KID") > 0 OR 
                                        INSTR(campaign_name,"INFANT") > 0 OR 
                                        INSTR(campaign_name,"TODDLER") > 0 
                                    THEN  "FAMILY"
                                WHEN INSTR(campaign_name,"REWARD") > 0 OR 
                                        INSTR(campaign_name,"SOCHI_PROMO") > 0 OR 
                                        INSTR(subject,"VIPEAK") > 0 OR 
                                        INSTR(subject,"FREE T") > 0 OR 
                                        INSTR(subject,"GET A FREE") > 0 OR 
                                        INSTR(campaign_name,"VIPEAK") > 0 OR 
                                        INSTR(campaign_name,"BONUS") > 0 OR 
                                        INSTR(campaign_name,"VIPEAK_REMINDER") > 0 OR 
                                        INSTR(subject,"EARN MORE POINTS") > 0 OR 
                                        INSTR(subject,"CLAIM YOUR REWARD") > 0 OR 
                                        INSTR(subject,"YOUR VIP") > 0 OR 
                                        INSTR(subject,"VIP TICKET") > 0 
                                    THEN  "VIPRWRD"
                                WHEN INSTR(campaign_name,"WELCOME EMAIL") > 0 OR 
                                        INSTR(campaign_name,"WELCOME_SIGNUP") > 0 OR 
                                        INSTR(campaign_name,"WELCOMEEMAIL") > 0 OR 
                                        INSTR(campaign_name,"WELCOME_SERIES") > 0 OR 
                                        INSTR(subject,"WELCOME TO") > 0 OR 
                                        INSTR(subject,"THANKS FOR JOINING") > 0 OR  
                                        INSTR(subject,"BEGINNER") > 0 
                                    THEN  "NEW_CUST"
                                WHEN INSTR(campaign_name,"LOYALTY WELCOME") > 0 OR 
                                        INSTR(campaign_name,"LOYALTYWELCOME") > 0 OR 
                                        INSTR(subject,"PEAKPOINT") > 0 
                                    THEN  "NEW_VIPK"
                                WHEN INSTR(campaign_name,"ABANDON") > 0 
                                    THEN  "ABNCART"
                                WHEN INSTR(campaign_name,"WISH LIST") > 0 OR 
                                        INSTR(campaign_name,"WISHLIST") > 0 OR 
                                        INSTR(campaign_name,"WISH_LIST") > 0 
                                    THEN  "WISHLIST"
                                WHEN INSTR(campaign_name,"BACK IN STOCK") > 0 OR 
                                        INSTR(campaign_name,"BACK_IN_STOCK") > 0 OR 
                                        INSTR(campaign_name,"BACKINSTOCK") > 0 OR 
                                        INSTR(campaign_name,"NEW_ARRIVAL") > 0 OR 
                                        INSTR(subject,"NEW ARRIVAL") > 0 OR 
                                        INSTR(subject,"NEW_ARRIVAL") > 0 OR 
                                        INSTR(subject,"CATALOG") > 0 OR 
                                        INSTR(subject,"BOUNCE_BACK") > 0 OR 
                                        INSTR(campaign_name,"BOUNCE_BACK") > 0 OR  
                                        INSTR(subject,"INVITE") > 0 OR  
                                        INSTR(subject,"CONVIE") > 0 
                                    THEN  "REP_CUST"
                                WHEN INSTR(campaign_name,"SURVEY") > 0 OR  
                                        INSTR(subject,"SURVEY") > 0 
                                    THEN "SURVEY"
                            ELSE ""
                            END AS email_persona
                        FROM tmp_tnf_email_launch_clean_csv_2 """
            )

            tmp_tnf_email_launch_clean_csv_df3.createOrReplaceTempView(
                "tmp_tnf_email_launch_clean_csv"
            )
            print(
                "count of records in tmp_tnf_email_launch_clean_csv_df3 {}".format(
                    tmp_tnf_email_launch_clean_csv_df3.count()
                )
            )
            tmp_tnf_email_launch_clean_csv_df3.show()

            x_tmp_tnf_email_launch_clean_tmp_df = spark.sql(
                """ 
                        SELECT sub.* FROM  
                                ( SELECT *, 
                                    ROW_NUMBER() OVER(PARTITION BY account_id,campaign_id,launch_id,list_id order by account_id) as row_num FROM tmp_tnf_email_launch_clean_csv 
                                ) sub 
                        WHERE row_num = 1
                    """
            ).drop("row_num")

            x_tmp_tnf_email_launch_clean_df = (
                x_tmp_tnf_email_launch_clean_tmp_df.withColumn(
                    "event_captured_dt_con",
                    F.concat(
                        x_tmp_tnf_email_launch_clean_tmp_df["event_captured_dt"].substr(
                            0, 2
                        ),
                        F.lit("-"),
                        x_tmp_tnf_email_launch_clean_tmp_df["event_captured_dt"].substr(
                            3, 3
                        ),
                        F.lit("-"),
                        x_tmp_tnf_email_launch_clean_tmp_df["event_captured_dt"].substr(
                            6, 4
                        ),
                    ),
                )
                .drop("row_num", "event_captured_dt")
                .withColumnRenamed("event_captured_dt_con", "event_captured_dt")
            )
            x_tmp_tnf_email_launch_clean_df.createOrReplaceTempView(
                "whouse_x_tmp_tnf_email_launch_clean"
            )
            print(
                "count of records in x_tmp_tnf_email_launch_clean_df {}".format(
                    x_tmp_tnf_email_launch_clean_df.count()
                )
            )

            whouse_x_tmp_tnf_email_sent_clean_df = spark.sql(
                """
                            SELECT 
                                distinct
                                st.campaign_id,
                                st.launch_id,
                                st.list_id,
                                st.riid,
                                to_date(st.event_captured_dt , "dd-MMM-yyyy") as sent_date,
                                st.customer_id,
                                lh.email_gender     AS gen,
                                lh.email_activity   AS act,
                                lh.email_ssn        AS ssn,
                                lh.email_persona    AS prs,
                                lh.email_channel    AS chnl,
                                lh.product_category AS pcat
                            FROM whouse_tnf_email_sent_view st
                            INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                            ON st.campaign_id = lh.campaign_id
                                AND st.launch_id = lh.launch_id
                                AND st.list_id = lh.list_id
                            WHERE 
                                LOWER(TRIM(st.email_ISP))  <> 'vfc.com' 
                                AND to_date(st.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                        """.format(
                    _cutoff_date
                )
            )

            whouse_x_tmp_tnf_email_sent_clean_df.createOrReplaceTempView(
                "whouse_x_tmp_tnf_email_sent_clean"
            )
            print(
                "count of records in whouse_x_tmp_tnf_email_sent_clean_df {}".format(
                    whouse_x_tmp_tnf_email_sent_clean_df.count()
                )
            )

            x_tmp_tnf_email_open_clean_df = spark.sql(
                """
                            SELECT distinct
                                    op.campaign_id,
                                    op.launch_id,
                                    op.list_id,
                                    op.riid,
                                    MIN (to_date(op.event_captured_dt , 'dd-MMM-yyyy')) AS open_date,
                                    MAX (to_date(op.event_captured_dt , 'dd-MMM-yyyy')) AS most_recent_o
                                FROM whouse_tnf_email_open_view op
                                INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                                        ON op.campaign_id = lh.campaign_id
                                        AND op.launch_id  = lh.launch_id
                                        AND op.list_id    = lh.list_id
                                WHERE 
                                        op.event_captured_dt IS NOT NULL
                                        AND to_date(op.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                                GROUP BY 
                                        op.campaign_id,
                                        op.launch_id,
                                        op.list_id,
                                        op.riid
                                """.format(
                    _cutoff_date
                )
            )
            x_tmp_tnf_email_open_clean_df.createOrReplaceTempView(
                "whouse_x_tmp_tnf_email_open_clean"
            )
            print(
                "count of records in x_tmp_tnf_email_open_clean_df {}".format(
                    x_tmp_tnf_email_open_clean_df.count()
                )
            )

            x_tmp_tnf_email_click_clean_df = spark.sql(
                """
                            SELECT 
                                distinct
                                    cl.campaign_id,
                                    cl.launch_id,
                                    cl.list_id,
                                    cl.riid,
                                    MIN (to_date(cl.event_captured_dt , 'dd-MMM-yyyy')) AS click_date
                                FROM whouse_tnf_email_click_view cl
                                    INNER JOIN whouse_x_tmp_tnf_email_launch_clean lh
                                        ON cl.campaign_id = lh.campaign_id
                                        AND cl.launch_id  = lh.launch_id
                                        AND cl.list_id = lh.list_id
                                WHERE 
                                        LOWER(trim(cl.offer_name)) <> 'unsubscribe_footer'
                                        AND to_date(cl.event_captured_dt , 'dd-MMM-yyyy') <= to_date('{}', 'ddMMMyyyy')
                                GROUP BY cl.campaign_id,
                                        cl.launch_id,
                                        cl.list_id,
                                        cl.riid
                            """.format(
                    _cutoff_date
                )
            )
            x_tmp_tnf_email_click_clean_df.createOrReplaceTempView(
                "whouse_x_tmp_tnf_email_click_clean"
            )
            print(
                "count of records in x_tmp_tnf_email_click_clean_df {}".format(
                    x_tmp_tnf_email_click_clean_df.count()
                )
            )
            x_tmp_tnf_email_click_clean_df.show()

            whouse_x_tmp_tnf_email_inputs_df = spark.sql(
                """ 
                            SELECT 	t3.CUSTOMER_ID,
                                t3.ACT,						
                                t3.CHNL,					
                                t3.GEN,		
                                t3.PCAT,
                                t3.PRS,						
                                t3.SENT_DATE,
                                t3.SSN,
                                t3.open_date,
                                t3.days_to_open ,
                                t3.most_recent_o,
                                t3.open_ind,
                                CAST(t4.click_date as date) as click_date,
                                datediff(CAST(t4.click_date as date),CAST(t3.open_date as date)) AS days_to_click ,
                                CASE
                                    WHEN t4.click_date IS NOT NULL THEN 1
                                ELSE 0
                                END AS click_ind
                            FROM
                                (SELECT t1.ACT,
                                    t1.CAMPAIGN_ID,
                                    t1.CHNL,
                                    t1.CUSTOMER_ID,											
                                    t1.GEN,
                                    t1.LAUNCH_ID,
                                    t1.LIST_ID,
                                    t1.PCAT,
                                    t1.PRS,
                                    t1.RIID,
                                    CAST(t1.SENT_DATE as date) as SENT_DATE,
                                    t1.SSN,
                                    CAST(t2.open_date as date) as open_date,
                                    datediff(cast(t2.open_date as date),cast(t1.sent_date as date)) AS days_to_open ,
                                    CAST(t2.most_recent_o as date) as most_recent_o,
                                    CASE
                                        WHEN t2.open_date IS NOT NULL THEN 1
                                    ELSE 0
                                    END AS open_ind
                                FROM whouse_x_tmp_tnf_email_sent_clean t1
                                LEFT JOIN whouse_x_tmp_tnf_email_open_clean t2
                                        ON t1.campaign_id = t2.campaign_id
                                        AND t1.list_id = t2.list_id
                                        AND t1.launch_id = t2.launch_id
                                        AND t1.riid = t2.riid 
                                ) t3
                            LEFT JOIN whouse_x_tmp_tnf_email_click_clean t4
                            ON t4.campaign_id = t3.campaign_id
                                AND t4.list_id = t3.list_id
                                AND t4.launch_id = t3.launch_id
                                AND t4.riid = t3.riid 
                            """
            )
            whouse_x_tmp_tnf_email_inputs_df.createOrReplaceTempView(
                "whouse_x_tmp_tnf_email_inputs"
            )
            print(
                "count number of records in whouse_x_tmp_tnf_email_inputs_df {}".format(
                    whouse_x_tmp_tnf_email_inputs_df.count()
                )
            )

            # cat_list = ["ssn","act"]
            cat_list = ["ssn", "gen", "act", "prs", "chnl", "pcat"]
            # loops through all category and var lists to create median days to open, median days to click, #sent, #open, #click, %open and %click
            for i in cat_list:
                df_list = spark.sql(
                    """
                            select tmp.*,
                            datediff(to_date('{}', 'ddMMMyyyy'), dsince_o_tmp) as dsince_o,
                                ROUND((freq_o * 100 / freq_s),1) as pct_o,
                                ROUND((freq_c * 100 / freq_o),1) as pct_c
                            FROM 
                                (SELECT  customer_id, %s, 
                                    percentile_approx(days_to_open,0.5) as md2o,
                                    percentile_approx(days_to_click,0.5) as md2c,
                                    max(most_recent_o)as dsince_o_tmp,
                                    count(*) as freq_s,
                                    sum(open_ind) as freq_o,
                                    sum(click_ind) as freq_c					
                                FROM whouse_x_tmp_tnf_email_inputs
                                WHERE %s is not null
                                GROUP BY customer_id, %s ) tmp """.format(
                        _cutoff_date
                    )
                    % (i, i, i)
                )
                table_nm = "temp_tnf_" + i + "_metrics"
                df_list.createOrReplaceTempView(table_nm)

                print("count is {}".format(df_list.count()))

                var_list = [
                    "md2o",
                    "md2c",
                    "dsince_o",
                    "freq_s",
                    "freq_o",
                    "freq_c",
                    "pct_o",
                    "pct_c",
                ]
                for j in var_list:
                    pivotDF = df_list.groupBy(["customer_id"]).pivot(i).max(j)
                    df_pivot = pivotDF.select(
                        [F.col(c).alias(i + "_" + j + "_" + c) for c in pivotDF.columns]
                    ).withColumnRenamed(
                        i + "_" + j + "_" + "customer_id", "customer_id"
                    )
                    tbl_nm = "temp_" + i + "_csv_" + j
                    df_pivot.createOrReplaceTempView(tbl_nm)
                    df_pivot.show()
                    print(tbl_nm)
                df_list.show()
            print("entering inner join")
            tmp_csv_tnf_email_inputs_df = (
                spark.sql(
                    """select temp_ssn_csv_md2o.customer_id as customer_id_temp,* from temp_ssn_csv_md2o 
            inner join temp_ssn_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_md2c.customer_id
            inner join temp_ssn_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_dsince_o.customer_id
            inner join temp_ssn_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_s.customer_id
            inner join temp_ssn_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_o.customer_id
            inner join temp_ssn_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_freq_c.customer_id
            inner join temp_ssn_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_pct_o.customer_id
            inner join temp_ssn_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_ssn_csv_pct_c.customer_id
            inner join temp_gen_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_md2o.customer_id
            inner join temp_gen_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_md2c.customer_id
            inner join temp_gen_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_dsince_o.customer_id
            inner join temp_gen_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_s.customer_id
            inner join temp_gen_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_o.customer_id
            inner join temp_gen_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_freq_c.customer_id
            inner join temp_gen_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_pct_o.customer_id
            inner join temp_gen_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_gen_csv_pct_c.customer_id
            inner join temp_act_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_md2o.customer_id
            inner join temp_act_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_md2c.customer_id
            inner join temp_act_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_dsince_o.customer_id
            inner join temp_act_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_s.customer_id
            inner join temp_act_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_o.customer_id
            inner join temp_act_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_freq_c.customer_id
            inner join temp_act_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_pct_o.customer_id
            inner join temp_act_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_act_csv_pct_c.customer_id
            inner join temp_prs_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_md2o.customer_id
            inner join temp_prs_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_md2c.customer_id
            inner join temp_prs_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_dsince_o.customer_id
            inner join temp_prs_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_s.customer_id
            inner join temp_prs_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_o.customer_id
            inner join temp_prs_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_freq_c.customer_id
            inner join temp_prs_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_pct_o.customer_id
            inner join temp_prs_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_prs_csv_pct_c.customer_id
            inner join temp_chnl_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_md2o.customer_id
            inner join temp_chnl_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_md2c.customer_id
            inner join temp_chnl_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_dsince_o.customer_id
            inner join temp_chnl_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_s.customer_id
            inner join temp_chnl_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_o.customer_id
            inner join temp_chnl_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_freq_c.customer_id
            inner join temp_chnl_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_pct_o.customer_id
            inner join temp_chnl_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_chnl_csv_pct_c.customer_id
            inner join temp_pcat_csv_md2o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_md2o.customer_id
            inner join temp_pcat_csv_md2c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_md2c.customer_id
            inner join temp_pcat_csv_dsince_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_dsince_o.customer_id
            inner join temp_pcat_csv_freq_s ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_s.customer_id
            inner join temp_pcat_csv_freq_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_o.customer_id
            inner join temp_pcat_csv_freq_c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_freq_c.customer_id
            inner join temp_pcat_csv_pct_o ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_pct_o.customer_id
            inner join temp_pcat_csv_pct_c ON  temp_ssn_csv_md2o.customer_id=temp_pcat_csv_pct_c.customer_id"""
                )
                .drop("customer_id")
                .withColumnRenamed("customer_id_temp", "customer_id")
            )
            print("exiting join")
            tmp_csv_tnf_email_inputs_df.show()
            tmp_csv_tnf_email_inputs_df.createOrReplaceTempView(
                "tmp_csv_tnf_email_inputs"
            )
            tmp_csv_tnf_email_inputs_df.cache()

            csv_tnf_email_inputs_df = (
                spark.sql(
                    """SELECT *,
                            CASE WHEN act_dsince_o_WATER is null AND act_dsince_o_SURF > 0
                                THEN act_dsince_o_SURF
                                ELSE act_dsince_o_WATER
                            END AS act_dsince_o_WATER_tmp,
                            CASE WHEN act_pct_o_WATER is null  AND act_pct_o_SURF > 0
                                THEN act_pct_o_SURF
                                ELSE act_pct_o_WATER
                            END AS act_pct_o_WATER_tmp
                        FROM     tmp_csv_tnf_email_inputs """
                )
                .drop(act_dsince_o_WATER, act_pct_o_WATER)
                .withColumnRenamed("act_dsince_o_WATER_tmp", "act_dsince_o_WATER")
                .withColumnRenamed("act_pct_o_WATER_tmp", "act_pct_o_WATER")
            )
            csv_tnf_email_inputs_df.show()
            full_load_df = csv_tnf_email_inputs_df

        except Exception as error:
            full_load_df = None
            print(
                "Error Occurred while processing run_csv_tnf_build_email_inputs due to :{}".format(
                    error
                )
            )
            logger.info(
                "Error Occurred while processing run_csv_tnf_build_email_inputs due to : {}".format(
                    error
                )
            )
        return full_load_df
