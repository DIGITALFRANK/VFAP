from modules.core.core_job import Core_Job
from modules.map.utils_map import MapUtils
import modules.config.config as config


class map_zeta_preference(Core_Job):
    def __init__(self, logger, spark, params):
        self.logger = logger
        self.spark = spark
        self.params = params

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying tr_map_zeta_preference")
        try:
            spark = self.spark
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utilis = MapUtils(spark=spark, logger=logger, params=self.params)
            logger.info("inside map")

            # ZETA SENT
            # map email 2 customer id.
            map_status = map_utilis.custom_generic_map_customer_id(
                map_params=params["tr_map_params"]["map_zeta_preference"][
                    "email_2_cust_id"
                ],
                src_bucket=params[config.FB_REFINED_BUCKET],
                src_path=params[config.FB_RF_DESTINATION_FOLDER]
                + "/vans_zeta_preference/",
                map_bucket=params[config.FB_REFINED_BUCKET],
                map_path="Map_Custom/Email_xref/",
            )

        except Exception as error:
            map_status = False
            logger.info(
                "Error Ocuured While processiong map_zeta_preference due to : {}".format(
                    error
                )
            )

        # need to chnage if map returns multiple df
        return map_status
