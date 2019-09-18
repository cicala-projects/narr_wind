"""
Retrieve and store NARR files from the NOAA FTP server
"""

import os
import logging
from itertools import product
from ftplib import FTP, all_errors
from functools import partial
from multiprocessing import Pool


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def datetime_range(start, end, delta):
    current = start
    if not isinstance(delta, timedelta):
        delta = timedelta(**delta)
    while current < end:
        yield current
        current += delta

def retrieve_year(start_year, 
                  end_year,
                  save_path):
    """
    Download year directory of .grd files to local directory.

    Params:
        - start_year str: year to start download.
        - end_year str: year to stop download.
    """

    year_month = [f'{year}0{month}' if month < 10 
                  else f'{year}{month}'  for year, month
                  in product(range(int(start_year), int(end_year) + 1),
                             range(1, 13))
                 ]

    with FTP('nomads.ncdc.noaa.gov') as ftp:
        response = ftp.login()
        if response == '230 Anonymous access granted, restrictions apply':
            for i in year_month:
            print(f'Starting year/month: {i}')
                try:
                    ftp.cwd(f'NARR/{i}')
                    dirnames = ftp.nlst()
                    for day in dirnames:
                        ftp.cwd(f'{day}')
                        grb_files = [f for f in ftp.nlst() if '.grb' in f]

                        for grb in grb_files:
                            path = os.path.join(save_path, i, day)

                            if os.path.exists(path):
                                logger.info('Directory exists! Skip dir creation')
                            else:
                                os.makedirs(path)

                            with open(os.path.join(path, grb), 'wb') as file_grb:
                                ftp.retrbinary(f'RETR {grb}', file_grb.write)
                        ftp.cwd('..')
                    ftp.cwd('../..')
                except all_errors as e:
                    logger.debug(f'Oops! Something went wrong. FTP error: {e}')

        else:
            print(f'FTP Connection error: {response}')


def retrieve_year_parallel(date_range,
                           number_workers,
                           path):
    '''
    Parallel implementation of retrieve year function. 
    '''

    logger - logging.getLogger(__name__)

    with Pool(number_wokers) as p:
        func_map = partial(retrieve_year,
                           end_year=None,
                           path=path)

        results = p.map(func_map, date_range, chunksize=1)
        print(results)



