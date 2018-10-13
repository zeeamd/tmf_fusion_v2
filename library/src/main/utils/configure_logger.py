import logging
import logging.config
import sys


def configure_logger(name, parent_level_name, log_path):

    logging.config.dictConfig({'version': 1, 'disable_existing_loggers': False,
                                'formatters': {
                                    'standard': {
                                        'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
                                    },
                                },
                                'handlers': {
                                    'default': {
                                        'level': 'INFO',
                                        'formatter': 'standard',
                                        'class': 'logging.StreamHandler',
                                    },
                                    'file_handler': {
                                        'level': 'INFO',
                                        'filename': log_path,
                                        'class': 'logging.FileHandler',
                                        'formatter': 'standard'
                                    }
                                },
                                'loggers': {
                                    name: {
                                        'handlers': ['file_handler', 'default'],
                                        'level': 'INFO',
                                        'propagate': True
                                    },
                                    parent_level_name: {
                                        'handlers': ['file_handler'],
                                        'level': 'INFO',
                                        'propagate': True
                                    },
                                    'library': {
                                        'handlers': ['file_handler'],
                                        'level': 'INFO',
                                        'propagate': True
                                    },
                                    'STDOUT': {
                                        'handlers': ['file_handler'],
                                        'level': 'INFO',
                                        'propagate': True
                                    },
                                    'STDERR': {
                                        'handlers': ['file_handler'],
                                        'level': 'INFO',
                                        'propagate': True
                                    }
                                }
                               }
                              )
    return logging.getLogger(name)


class StreamToLogger(object):
    """
    Fake file-like stream object that redirects writes to a logger instance.
    """

    def __init__(self, logger):
        self.logger = logger
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.info(line.rstrip())

    def flush(self):
        # create a flush method so things can be flushed when
        # the system wants to. Not sure if simply 'printing'
        # sys.stderr is the correct way to do it, but it seemed
        # to work properly for me.
        self.logger.info('')
