"""
Retrieve and store NARR files from the NOAA FTP server
"""


import os
import docker
import logging
import requests
import asyncio
from aiohttp import ClientSession
from itertools import product
from functools import partial
from multiprocessing import Pool
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


def dict_product(d):
    """
    Create a cartesian product of dictionary values (or flatten a dict of lists
    keeping the keys).  
    """
    return list(dict(zip(d.keys(), values)) for values in product(*d.values()))


def datetime_range(start, end, delta):
    """
    Build a generator with dates within a range of datetime objects
    """

    current = start
    if not isinstance(delta, (timedelta, relativedelta)):
        delta = relativedelta(**delta)
    while current < end:
        yield current
        current += delta


async def retrieve_day_async(start_month_date,
                               save_path):
     """
    Download individual month directory of .grd files to local directory in
    asynchronous implementation.

    This function will download using the ftplib all the .grd files between the
    start_date and the end_date. All dates in the NOAA NARR FTP server are
    stored following this order:
        data
        ├── year/month
            ├── year/month/day01
            ├── year/month/day02

    Here we download the monthly directory with the user-defined dates in the
    start and end dates. 

    Params:
        - start_year str: year to start download.
        - end_year str: year to stop download.
    """

    logger = logging.getLogger(__name__)

    if not isinstance(start_day_date, datetime):
        start_day_date = datetime.strptime(start_day_date, '%Y-%m-%d')
    else:
        ValueError(f'{start_date} is not in the correct format or not a valid type')

    base_url = "https://nomads.ncdc.noaa.gov/data/narr"
    month_dir, day_dir = (start_month_date.strftime('%Y%m'), start_month_date.strftime('%Y%m%d'))
    url_time = URL(base_url, month_dir, day_dir)

    path = os.path.join(save_path, month_dir, day_dir)
    logger.info(f'Starting transmission for {grb} file')

    if os.path.exists(path):
        logger.info('Directory exists! Skip dir creation')
    else:
        os.makedirs(path)


    date_range_daily = datetime_range(start=start_day_date,
                                      end=start_day_date +
                                      relativedelta(days=1),
                                      delta={'hours': 3})

    grib_files_day = [f'narr-a_221_{date.strftime("%Y%m%d_%H%M")}_000.grb' for
                      date in date_range_daily]

    async def download_unique_date(session, url):
        with async_timeout.timeout(10):
            async with session.get(url) as response:
                async with aiofiles.open(path, url.name) as file_stream:
                    while True:
                        chunk = await response.content.read()
                        if not chunk:
                            break
                        await file_streak.write(chunk)
                return await response.release()


    tasks = []
    async with aiohttp.ClientSession() as session:
        for grib_file in grib_files_day:
            url_download = URL(url_time, grib_file)
            task = asyncio.ensure_future(download_unique_date(session,
                                                              url_download))
            tasks.append(task)
            await asyncio.gather(*tasks)


def retrieve_individual_month(start_date,
                              save_path):
    """
    Download individual month directory of .grd files to local directory.

    This function will download using the ftplib all the .grd files between the
    start_date and the end_date. All dates in the NOAA NARR FTP server are
    stored following this order:
        data
        ├── year/month
            ├── year/month/day01
            ├── year/month/day02

    Here we download the monthly directory with the user-defined dates in the
    start and end dates. 

    Params:
        - start_year str: year to start download.
        - end_year str: year to stop download.
    """

    logger = logging.getLogger(__name__)

    if not isinstance(start_date, datetime):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    else:
        ValueError(f'{start_date} is not in the correct format or not a valid type')


    with FTP('nomads.ncdc.noaa.gov') as ftp:
        response = ftp.login()
        if response == '230 Anonymous access granted, restrictions apply':

            date_str = start_date.strftime('%Y%m')
            logger.info(f'Starting year/month: {date_str}')
            try:
                ftp.cwd(f'NARR/{date_str}')
                dirnames = ftp.nlst()
                for day in dirnames:
                    ftp.cwd(f'{day}')

                    # Select only narr-b files with the measured (not
                    # predicted) values.
                    grb_files = [f for f in ftp.nlst() if
                                 f.startswith('narr-a') and 
                                 f.endswith('.grb')]

                    for grb in grb_files:
                        path = os.path.join(save_path, date_str, day)
                        logger.info(f'Starting transmission for {grb} file')

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
            logger.error(f'FTP Connection error: {response}')

        # Close conn, allow more workers to login
        ftp.close()


def retrieve_year_months(start_date,
                         end_date,
                         delta,
                         save_path):
    """
    Download year directory of .grd files to local directory.

    This function will download using the ftplib all the .grd files between the
    start_date and the end_date. All dates in the NOAA NARR FTP server are
    stored following this order:
        data
        ├── year/month
            ├── year/month/day01
            ├── year/month/day02

    Here we download the monthly directory with the user-defined dates in the
    start and end dates. If delta is: {'months': 1}, then we will retrieve the
    monthly directories corresponding to the date range. If delta is {'years':1}
    then the functgion will dowload complete years within the date range. In
    the case the end_date is None, the function will take the use the next
    month as the end of the date range. Hence, if start_date = '2012-01-01',
    the end_date will be '2012-02-01'. 

    Params:
        - start_year str: year to start download.
        - end_year str: year to stop download.
    """

    logger = logging.getLogger(__name__)


    if end_date is not None:
        dateranges = datetime_range(start_date,
                                    end_date,
                                    delta)
    else:
        dateranges = datetime_range(start_year,
                                    start_year + relativedelta(**{'months': 1}),
                                    {'months': 1})

    with FTP('nomads.ncdc.noaa.gov') as ftp:
        response = ftp.login()
        if response == '230 Anonymous access granted, restrictions apply':

            for date in dateranges:
                date_str = date.strftime('%Y%m')
                logger.info(f'Starting year/month: {date_str}')
                try:
                    ftp.cwd(f'NARR/{date_str}')
                    dirnames = ftp.nlst()
                    for day in dirnames:
                        ftp.cwd(f'{day}')

                        # Select only narr-b files with the measured (not
                        # predicted) values.
                        grb_files = [f for f in ftp.nlst() if
                                     f.startswith('narr-a') and 
                                     f.endswith('.grb')]

                        for grb in grb_files:
                            path = os.path.join(save_path, date_str, day)
                            logger.info(f'Starting transmission for {grb} file')

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
            logger.error(f'FTP Connection error: {response}')

    # Close conn after getting year. Allow other workers to login
    ftp.close()


def retrieve_year_parallel(start_date,
                           end_date,
                           number_workers,
                           path):
    '''
    Parallel implementation of retrieve year function. The function will take
    two initial dates and build a date range. Each element of the range will be
    processed in parallel. 
    '''

    logger = logging.getLogger(__name__)

    year_range = datetime_range(start=start_date,
                                end=end_date,
                                delta={'months': 1})

    with Pool(number_workers) as p:
        func_map = partial(retrieve_year,
                           end_year=None,
                           save_path=path)

        results = p.map(func_map, year_range, chunksize=1)
        print(results)


def docker_execute_degrib(client_name,
                          path_dir,
                          message_code):
    """
    Execute degrib command in Docker container

    This function uses the Docker Python SDK to execute a command inside the
    client_name Docker container. In this function, we will run de degrib
    command with a GNU parallel approach on the .grb files inside the path_dir
    argument. 

    In the luigi.contrib there is a Docker module, but as all things in luigi,
    commands are rather esoteric at times.
    """

    logger = logging.getLogger(__name__)

    command = (f"find {path_dir} -name '*.grb' -type f"
               " | parallel -- degrib {}"
               " -C"
               f" -msg {message_code}"
               " -Csv"
               " -namePath /tmp/data_degrib/csv_data"
               " -nMet"
               " -nameStyle 131_%e%V.csv"
              )

    client = docker.from_env()
    container_id = [x.attrs['Id'] for x in client.containers.list() 
                    if client_name in x.attrs['Config']['Image']]

    container = client.containers.get(container_id[0])
    container.exec_run(['/bin/bash',  '-c', command], detach=True)


