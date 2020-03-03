##################################################
# Name: custom_logger
# Description: To create custom logger level and module level logger objects
# Usage: Logger object for entire ETL app
##################################################
# Developer: Akash Gandhi
# Copyright: Copyright 2019, VFC-SAS-CLM
# Python Version: 3.8
# License: {license}
# Module Version: 0.1.1
# Maintainer:  Akash Gandhi
# Email: akash.gandhi@corecompete.com
# Status: Development in progress
# History: Developed on Dec 02, 2019
##################################################
import logging
from modules.config import config


def module_logger(name):
    """Create module level logger objects and custom log levels

    Arguments:
        name {String} -- Module name for the logger object to be created.

    Returns:
        Object -- Return a logger object to be used for the module
    """
    try:
        # *! Check environ from param store before using logger
        if config.env.lower() == "production":
            mode = logging.ERROR
        elif config.env.lower() == "qa":
            mode = logging.INFO
        else:
            mode = logging.DEBUG
        logging.basicConfig(
            level=mode, format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
        )
        logger = logging.getLogger(name)
    except Exception as error:
        print(
            "Error while creating a custom logger.Returning a default logger set to level INFO ".format(
                error
            )
        )
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
        logger = logging.getLogger(__name__)
    return logger
