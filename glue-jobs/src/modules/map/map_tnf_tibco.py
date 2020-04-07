from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.utils.utils_core import utils
import modules.config.config as config
from modules.exceptions.CustomAppException import CustomAppError
import traceback


class map_tnf_tibco(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_tnf_tibco, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_tnf_tibco")
        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utilis = MapUtils(file_name=self.file_name)
            logger.info("inside map")

            # Create Loyalty Xref table
            # drop_loyalty_xref = "Drop table if exists {}.loyalty_xref".format(self.whouse_details["dbSchema"])
            # utils.execute_query_in_redshift(drop_loyalty_xref, self.whouse_details, logger)
            # CREATE_LOYALTY_XREF_QUERY = config.LOYALTY_XREF_QUERY.format(self.whouse_details["dbSchema"])
            # utils.execute_query_in_redshift(CREATE_LOYALTY_XREF_QUERY, self.whouse_details, logger)

            # logger.info("Created Loayalty Xref query successfully!!")
            # TNF TIBCO
            # map loyalty 2 customer id.
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_tnf_tibco"]["loyalty_2_cust_id"],
                src_table=params["map_params"]["source_target_params"]["tbl_tnf_tibco"],
                map_table=params["map_params"]["source_target_params"][
                    "xref_loyalty_xref"
                ],
            )

            map_status = True

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_tnf_tibco due to : {}".format(
                    error
                ),
                exc_info=True,
            )
            raise CustomAppError(
                moduleName=constant.MAP_TNF_TIBCO,
                exeptionType=constant.MAP_EXCEPTION,
                message="Error Ocuured While processiong map_tnf_tibco due to : {}".format(
                    traceback.format_exc()
                ),
            )

        return map_status
