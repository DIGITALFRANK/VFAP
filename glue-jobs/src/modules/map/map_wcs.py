from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback

class map_wcs(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_wcs, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        print("Applying map_wcs")
        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utilis = MapUtils(file_name=self.file_name)

            # VANS_WCS
            # First map the email to customer_id
            # email_2_cust_id  -- source -> vans_wcs_abandoned_cart, map ->email_xref
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_wcs"]["email_2_cust_id_email"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_vans_wcs_abandoned_cart"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # email_2_cust_id  -- source -> vans_wcs_wishlist, map ->email_xref
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_wcs"]["email_2_cust_id_email"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_vans_wcs_wishlist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # email_2_cust_id  -- source -> vans_wcs_returns_us, map ->email_xref
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_wcs"]["email_2_cust_id_emailid"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_vans_wcs_returns_us"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # TNF
            # email_2_cust_id  -- source -> tnf_wcs_abandoned_wish_cart, map ->email_xref
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_wcs"]["email_2_cust_id_email"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_wcs_abandoned_wish_cart"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # email_2_cust_id  -- source -> tnf_wcs_returns_us, map ->email_xref
            map_utilis.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_wcs"]["email_2_cust_id_emailid"],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_wcs_returns_us"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            map_status = True

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_wcs due to : {}".format(error),exc_info=True
            )
            raise CustomAppError(moduleName=constant.MAP_WCS,
                                 exeptionType=constant.MAP_EXCEPTION,
                                 message="Error Ocuured While processiong map_wcs due to : {}".format(
                                     traceback.format_exc()))

        return map_status
