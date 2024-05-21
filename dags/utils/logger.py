import logging

class LoggerFactory(object):
    """
    A factory class for creating and retrieving a logger instance.

    This class provides a static method `get_logger` that can be called by other modules
    to initialize a logger with the specified log level.

    Attributes:
        _LOGGER (logging.Logger): The logger instance.

    Methods:
        __create_logger(log_level): A private method that creates a logger instance with the specified log level.
        get_logger(log_level): A static method that returns the logger instance with the specified log level.
    """
    _LOGGER = None

    @staticmethod
    def __create_logger(log_level):
        '''
        A private method that creates and configures a logger object.

        Parameters:
        log_level (str): The desired log level for the logger.

        Returns:
        logging.Logger: The configured logger object.
        '''
        # set the logging format
        formatter = logging.Formatter(
            '%(asctime)s : %(name)s : %(levelname)s : %(module)s : %(funcName)s : %(lineno)d :  %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # create a console handler and set its level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.getLevelName(log_level))
        console_handler.setFormatter(formatter)

        # create logger and add the console handler
        logger = logging.getLogger()
        logger.setLevel(logging.getLevelName(log_level))
        logger.addHandler(console_handler)

        # Disable propagation to prevent log duplication
        logger.propagate = False

        # Set the 'airflow' logger's level to ERROR to prevent lower severity logs from propagating
        airflow_logger = logging.getLogger('airflow')
        airflow_logger.setLevel(logging.ERROR)

        return logger

    @staticmethod
    def get_logger(log_level):
        '''
        A static method that's called by other modules to initialize logger on their own module

        Args:
            log_level (str): The log level to be set for the logger.

        Returns:
            logging.Logger: The logger instance with the specified log level.
        '''
        if not LoggerFactory._LOGGER:
            LoggerFactory._LOGGER = LoggerFactory.__create_logger(log_level)
        return LoggerFactory._LOGGER
