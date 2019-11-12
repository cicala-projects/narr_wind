import os
import logging
import pandas as pd
from functools import partial
from datetime import datetime
from multiprocessing import Pool
from dateutil.relativedelta import relativedelta
from src.get_narr import download_process_data_local, datetime_range
from src.luigi_config.config import s3Bucket


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s %(levelname)s] %(message)s',
                        datefmt="%Y-%m-%d %H:%M:%S",
                        filename='log_batch.log',
                        filemode='a')
    logger = logging.getLogger(__name__)

    # Read band data and subset wind bands
    bands_grib = pd.read_csv('data/inventory_grib.csv')
    wind_bands = bands_grib[bands_grib[' elem'].str.contains('wind')]['MsgNum'].tolist()

    # Run
    date_generator = datetime_range(datetime(2000, 1, 1), 
                                    datetime(2013, 1, 1), 
                                    {'months': 1})


    for date_month in date_generator:
        logging.info(f'Processing {date_month.strftime("%Y-%m")}')
        download_process_data_local(start_date=date_month,
                                    aws_key=s3Bucket().key,
                                    aws_secret=s3Bucket().secret,
                                    aws_bucket_name=s3Bucket().bucket,
                                    bands=wind_bands,
                                    max_workers=20,
                                    delta=None,
                                    end_date=None)


