from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback

class map_custom_shoes(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_custom_shoes, self).__init__(file_name)

    def map(self):
        logger = self.logger
        logger.info("Applying map_custom_shoes")
        map_status = False
        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utils = MapUtils(file_name=self.file_name)

            # email_2cust_id
            # need to chnage parameters for redshift source
            map_status = map_utils.custom_generic_map_customer_id(
                src_table=params["map_params"]["source_target_params"][
                    "tbl_vans_custom_shoes"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
                map_params=params["map_params"]["map_custom_shoes"],
            )

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_custom_shoes due to : {}".format(
                    error
                )
            )
            raise CustomAppError(moduleName=constant.MAP_CUSTOM_SHOES,
                                 exeptionType=constant.MAP_EXCEPTION,
                                 message="Error Ocuured While processiong map_custom_shoes due to : {}".format(
                                     traceback.format_exc()))
        return map_status
