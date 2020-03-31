from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback

class map_zeta_preference(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_zeta_preference, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_zeta_preference")
        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utilis = MapUtils(file_name=self.file_name)
            logger.info("inside map")

            # ZETA SENT
            # map email 2 customer id.
            map_status = map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_zeta_preference"][
                    "email_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_vans_zeta_preference"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_zeta_preference due to : {}".format(error),exc_info=True
            )
            raise CustomAppError(moduleName=constant.MAP_ZETA_PREFERENCE,
                                 exeptionType=constant.MAP_EXCEPTION,
                                 message="Error Ocuured While processiong map_zeta_preference due to : {}".format(
                                     traceback.format_exc()))

        return map_status
