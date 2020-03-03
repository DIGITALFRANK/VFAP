from modules.core.core_job import Core_Job
from modules.utils.utils_map import MapUtils
from modules.constants import constant
from modules.exceptions.CustomAppException import CustomAppError
import traceback

class map_responsys_email(Core_Job):
    def __init__(self, file_name):
        self.file_name = file_name
        super(map_responsys_email, self).__init__(file_name)

    def map(self):
        logger = self.logger
        map_status = False
        logger.info("Applying map_responsys_email")

        try:
            params = self.params
            logger = self.logger

            # instantiate map class
            map_utils = MapUtils(file_name=self.file_name)

            # TNF_EMAIL_SENT_HIST
            # First map the email to customer_id
            # email_2_cust_id  -- source -> TNF_EMAIL_SENT_HIST, map ->email_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "email_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_sent_hist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # Second map the customer no to customer id. The previous step mapping will be overwritten. We will trust cust no mapping more than email mapping
            # cust_no_2_cust_id  -- source -> tnf_email_sent_hist, map ->cust_xref_final
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "cust_no_2_cust_id"
                ],
                # src_bucket=params[config.FB_REFINED_BUCKET],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_sent_hist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_cust_xref_final"
                ],
            )

            # TNF_EMAIL_SENT
            # First map the email  to customer id
            # email_2_cust_id  -- source -> tnf_email_sent, map ->Email_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "email_2_cust_id"
                ],
                # src_bucket=params[config.FB_REFINED_BUCKET],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_sent"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_email_xref"
                ],
            )

            # Second map the customer no to customer id. The previous step mapping will be overwritten. We will trust cust no mapping more than email mapping
            # cust_no_2_cust_id  -- source -> tnf_email_sent, map ->cust_xref_final
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "cust_no_2_cust_id"
                ],
                # src_bucket=params[config.FB_REFINED_BUCKET],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_sent"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_cust_xref_final"
                ],
            )

            # create tnf_responsys_xref which will be used for further mappings
            # This created table will be used as maptbl for further updates to other email response tables
            # source -- tnf_email_sent_view
            tnf_responsys_xref_status = map_utils.create_tnf_responsys_xref(
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_sent_view"
                ],
                tgt_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # TNF_EMAIL_CLICK
            # list_2_cust_id  -- source -> tnf_email_sent, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_click"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # list_2_cust_id  -- source -> tnf_email_clik_hist, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                # src_bucket=params[config.FB_REFINED_BUCKET],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_click_hist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # TNF_EMAIL_OPEN
            # list_2_cust_id  -- source -> tnf_email_open, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_open"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # list_2_cust_id  -- source -> tnf_email_open_hist, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_open_hist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # TNF_EMAIL_CONVERT
            # list_2_cust_id  -- source -> tnf_email_open_hist, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_convert"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            # list_2_cust_id  -- source -> tnf_email_convert_hist, map ->tnf_responsys_xref
            map_utils.custom_generic_map_customer_id(
                map_params=params["map_params"]["map_responsys_email"][
                    "list_2_cust_id"
                ],
                src_table=params["map_params"]["source_target_params"][
                    "tbl_tnf_email_convert_hist"
                ],
                map_table=params["map_params"]["source_target_params"][
                    "xref_tnf_responsys_xref"
                ],
            )

            map_status = True

        except Exception as error:
            map_status = False
            logger.error(
                "Error Ocuured While processiong map_responsys_email due to : {}".format(
                    error
                )
            )
            raise CustomAppError(moduleName=constant.MAP_RESPONSYS_EMAIL,
                                 exeptionType=constant.MAP_EXCEPTION,
                                 message="Error Ocuured While processiong map_responsys_email due to : {}".format(
                                     traceback.format_exc()))

        # need to chnage if map returns multiple df

        return map_status
