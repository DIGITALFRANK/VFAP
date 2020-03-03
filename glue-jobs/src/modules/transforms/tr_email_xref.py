from pyspark.sql import functions as F
from modules.core.core_job import Core_Job
import modules.config.config as config
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.types import *


class tr_email_xref(Core_Job):
    def __init__(self, file_name):
        """Constructor for tr_email_xref

        Arguments:
            TransformCore {class} -- Base class within transform module
            df {spark.dataframe} -- Spark dataframe to be transformed
            logger {logger object} -- Logger object to create logs using the logging
            module
        """
        print("inside tr email xref constructor")
        # super(tr_email_xref, self).__init__(file_name)
        super(tr_email_xref, self).__init__(file_name)

    def transform(self, df):
        print(" Applying tr_email_xref Utility ")
        full_load_df = None
        transformed_df_dict = {}
        try:
            spark = self.spark
            params = self.params
            logger = self.logger
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
            full_load_df.show()
            logger.info(
                "count of records in email_xref table is {}".format(
                    full_load_df.count()
                )
            )
            transformed_df_dict={
                params["tgt_dstn_tbl_name"]: full_load_df
            }
        except Exception as error:
            full_load_df = None
            transformed_df_dict={}
            logger.info(
                "Error Ocuured While processiong email_xref due to : {}".format(error)
            )
        return transformed_df_dict
