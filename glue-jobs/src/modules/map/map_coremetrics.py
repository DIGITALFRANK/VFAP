from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback
class map_coremetrics(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_coremetrics, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_coremetrics")
        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utils = MapUtils(file_name=self.file_name)

            #logger.info("Applying cm_session_xref......")
            # creating cm_session_xref,need to chnage function params depending on target
            """cm_session_xref_status = map_utils.create_cm_session_xref(
                src_table=params['map_params']['source_target_params']['tbl_cm_registration'],
                map_table=params['map_params']['source_target_params']['xref_tbl_email_xref'])"""

            # session_2_cust_id
            map_status = map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_coremetrics"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_cm_abandon"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_cm_session_xref"
                ],
            )

            # session_2_cust_id
            # need to chnage parameters for redshift source
            map_status = map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_coremetrics"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_cm_productview"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tbl_cm_session_xref"
                ],
            )

        except Exception as error:
            logger.error(
                "Error Ocuured While processiong map_coremetrics due to : {}".format(
                    error
                )
            )
            map_status = False
            raise CustomAppError(moduleName=constant.MAP_COREMETRICS,
                                 exeptionType=constant.MAP_EXCEPTION,
                                 message="Error Ocuured While processiong map_coremetrics due to : {}".format(
                                     traceback.format_exc()))

        # need to chnage if map returns multiple df
        return map_status
