import logging
from modules.config import config


def module_logger(name):
    try:
        # *! Check environ from param store before using logger
        if config.env.lower() == "production":
            mode = logging.ERROR
        elif config.env.lower() == "dev":
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
