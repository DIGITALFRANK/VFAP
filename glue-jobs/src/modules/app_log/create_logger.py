import logging
from pathlib import Path
from modules.utils.utils_core import utils
from modules.config import config


def get_logger(logger_name=None):
    """Create Logger Object for logging application logs to S3

    Returns:
        Object -- Returns a logger object
    """
    # *! Check environ from param store before using logger
    try:
        key = utils.get_parameter_store_key()
        env_params = utils.get_param_store_configs(key)
        if (
            env_params["env_name"].lower() == "production"
            or env_params["env_name"].lower() == "prod"
        ):
            mode = logging.ERROR
        elif (
            env_params["env_name"].lower() == "qa"
            or env_params["env_name"].lower() == "uat"
        ):
            mode = logging.INFO
        elif (
            env_params["env_name"].lower() == "development"
            or env_params["env_name"].lower() == "dev"
        ):
            mode = logging.DEBUG
        else:
            mode = logging.INFO
    except Exception as error:
        logging.error("Error file configuring logger {}".format(error))
        mode = logging.DEBUG
    # *!Hardcoded to test.Make this configurable
    finally:
        Path(config.LOG_FILE).touch()
        # print("List of subdirectories are as follows : ", os.listdir(path="./tmp"))
        logging.basicConfig(
            level=mode,
            format="%(asctime)s %(levelname)s [%(name)s] (%(filename)s:%(lineno)d)\
                %(message)s",
            filename=config.LOG_FILE,
        )
        if logger_name is None:
            logger = logging.getLogger(__name__)
        else:
            logger = logging.getLogger(logger_name)
    return logger
