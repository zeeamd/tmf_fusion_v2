import pandas as pd
import logging


def read_test_data(file_dir, flnm):
    logger = logging.getLogger(__name__)

    logger.info('Reading test dataset')

    try:
        return pd.read_csv(file_dir + '/' + flnm + '.csv')
    except Exception as e:
        logger.fatal(e, exc_info=True)

        logger.info('Trying to find the SAS file')

        try:

            return pd.read_sas(file_dir + '/' + flnm + '.sas7bdat')

        except Exception as e:
            logger.fatal(e, exc_info=True)

            return pd.DataFrame()


def force_decode(x, codecs=None):
    if codecs is None:
        codecs = ['utf8', 'cp1252']
    for i in codecs:
        try:
            if not type(x) == str:
                return x.decode(i)
            else:
                return x
        except UnicodeDecodeError:
            pass