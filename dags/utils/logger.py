import logging

class LoggerFactory(object):
    
    _LOGGER = None

    @staticmethod
    def __create_logger(log_file, log_level):
        '''
        A private method that interacts with the python logging module.
        '''
        # set the logging format
        formatter = '%(asctime)s : %(name)s : %(levelname)s : %(message)s'

        # initialize the logger object
        LoggerFactory._LOGGER = logging.getLogger(log_file)
        logging.basicConfig(filename=log_file, level=logging.INFO, format=formatter, datefmt='%Y-%m-%d %H:%M:%S')

        # set logging level based on user input
        if log_level == 'INFO':
            LoggerFactory._LOGGER.setLevel(logging.INFO)
        elif log_level == 'DEBUG':
            LoggerFactory._LOGGER.setLevel(logging.DEBUG)
        elif log_level == 'WARNING':
            LoggerFactory._LOGGER.setLevel(logging.WARNING)
        elif log_level == 'ERROR':
            LoggerFactory._LOGGER.setLevel(logging.ERROR)
        elif log_level == 'CRITICAL':
            LoggerFactory._LOGGER.setLevel(logging.CRITICAL)
        
        return LoggerFactory._LOGGER
    
    @staticmethod
    def get_logger(log_file, log_level):
        '''
        A static method that's called by other modules to initialize logger on their own module
        '''
        return LoggerFactory.__create_logger(log_file, log_level)