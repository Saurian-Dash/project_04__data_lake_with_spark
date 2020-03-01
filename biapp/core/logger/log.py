import logging


LEVEL = 'INFO'
FORMAT = (
    '%(asctime)s | Module: %(module)s | Function: %(funcName)s | %(message)s'
)
DATEFMT = '%Y-%m-%d %H:%M:%S'


def setup_custom_logger(name):
    """
    Sets up a custom logger for use in the application. The logging level
    is set to DEBUG by default; all debug messages will be logged to the
    terminal.

    Set LEVEL to 'INFO' to disable the debug information.

    Args:
        name: __name__ property of module you are instantiating a logger
        object from.

    Returns:
        logging.Logger
    """
    formatter = logging.Formatter(fmt=FORMAT, datefmt=DATEFMT)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(LEVEL)
    logger.addHandler(handler)

    return logger
