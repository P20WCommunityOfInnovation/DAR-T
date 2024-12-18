import logging.config
import logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {

            'format' : '%(asctime)s - %(levelname)s - %(name)s - %(funcName)s - Line %(lineno)d - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'standard',
        }
        ,
        # 'file': {
        #     'class': 'logging.FileHandler',
        #     'filename': 'app.log',
        #     'level': 'INFO',
        #     'formatter': 'standard',
        # },
    },
    'loggers': {
        '': {  # Root logger
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True,
        },
    }
}

logging.config.dictConfig(LOGGING_CONFIG)

def create_logger(name:str)-> logging:
    return logging.getLogger(name)