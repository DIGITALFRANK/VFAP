from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
import modules.config.config as config


class map_tibco(Core_Job):
    def __init__(self, logger, spark, params):
        self.logger = logger
        self.spark = spark
        self.params = params

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_tibco")
        try:
            spark = self.spark
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utils = MapUtils(spark=spark, logger=logger, params=self.params)

            # creating loyalty_xref,need to chnage function params depending on target
            loyalty_xref_status = map_utils.create_loyalty_xref(
                src_bucket=params[config.FB_REFINED_BUCKET],
                cust_attribute_src_path="Map_Custom/cust_attribute/",
            )

            # need to chnage parameters for redshift source

            map_status = map_utils.custom_generic_map_customer_id(
                map_params=params["tr_map_params"]["map_tibco"],
                src_bucket=params[config.FB_REFINED_BUCKET],
                src_path=params[config.FB_RF_DESTINATION_FOLDER] + "/tnf_tibco.csv",
                map_bucket=params["transformed_bucket_name"],
                map_path="Map_Custom/loyalty_xref/",
            )

            # tnf_tibco_df.select("customer_id","RETAILERSHOPPERID").show(10,truncate=False)

        except Exception as error:
            map_status = False
            logger.info(
                "Error Ocuured While processiong map_tibco due to : {}".format(error)
            )

        # need to chnage if map returns multiple df
        return map_status
