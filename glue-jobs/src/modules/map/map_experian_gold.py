from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
import modules.config.config as config
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback


class map_experian_gold(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_experian_gold, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_experian_gold")
        try:
            spark = self.spark
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utils = MapUtils(file_name=self.file_name)

            # map cust_no_2_cust_id
            # Map the customer no to customer id
            map_status = map_utils.custom_generic_map_customer_id(
                src_table=params["map_params"]["source_target_params"][
                    "tbl_experian_gold"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_cust_xref_final"
                ],
                map_params=params["map_params"]["map_experian_gold"][
                    "cust_no_2_cust_id"
                ],
            )

            # after cust_no_2_cust_id duplicates may exists,so get duplicates and truncate and load destination logic depends on fssk
            # TODO duplicate records removal

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While map_experian_gold  due to : {}".format(error),
                exc_info=True,
            )
            raise CustomAppError(
                moduleName=constant.MAP_CUSTOM_SHOES,
                exeptionType=constant.MAP_EXCEPTION,
                message="Error Ocuured While map_experian_gold  due to : {}".format(
                    traceback.format_exc()
                ),
            )
        return map_status
