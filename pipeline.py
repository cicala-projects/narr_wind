"""
Luigi pipeline to download and extract NARR data from NOAA FTP servers,
extract, and process data. 
"""

import os
import luigi
import logging
import pandas as pd
from datetime import datetime
from luigi import date_interval
from luigi_monitor import monitor
from luigi.tools.range import RangeDailyBase
from dateutil.relativedelta import relativedelta

from luigi.contrib import docker_runner, s3, external_program
from src.luigi_config.config import s3Bucket
from src.get_narr import (datetime_range,
                          dict_product,
                          download_process_data_local,
                          docker_execute_degrib)

logger = logging.getLogger('luigi-interface')


class OrderDownload(luigi.WrapperTask):
    date_list = [
        datetim1, 1),?!? (*_*args*_*, kwargs) ?!?jedi?!?
        datetime(2001, 2, 1),
        datetime(2001, 3, 1),
        datetime(2001, 4, 1),
        datetime(2001, 5, 1),
        datetime(2001, 6, 1),
        datetime(2001, 7, 1),
        datetime(2001, 8, 1),
        datetime(2001, 9, 1),
        datetime(2001, 10, 1),
        datetime(2001, 11, 1),
        datetime(2001, 12, 1),
        datetime(2002, 1, 1),
        datetime(2002, 2, 1),
        datetime(2002, 3, 1),
        datetime(2002, 4, 1),
        datetime(2002, 5, 1),
        datetime(2002, 6, 1),
        datetime(2002, 7, 1),
        datetime(2002, 8, 1),
        datetime(2002, 9, 1),
        datetime(2002, 10, 1),
        datetime(2002, 11, 1),
        datetime(2002, 12, 1),
        datetime(2003, 1, 1),
        datetime(2003, 2, 1),
        datetime(2003, 3, 1),
        datetime(2003, 4, 1),
        datetime(2003, 5, 1),
        datetime(2003, 6, 1),
        datetime(2003, 7, 1),
        datetime(2003, 8, 1),
        datetime(2003, 9, 1),
        datetime(2003, 10, 1),
        datetime(2003, 11, 1),
        datetime(2003, 12, 1),
        datetime(2004, 1, 1),
        datetime(2004, 2, 1),
        datetime(2004, 3, 1),
        datetime(2004, 4, 1),
        datetime(2004, 5, 1),
        datetime(2004, 6, 1),
        datetime(2004, 7, 1),
        datetime(2004, 8, 1),
        datetime(2004, 9, 1),
        datetime(2004, 10, 1),
        datetime(2004, 11, 1),
        datetime(2004, 12, 1),
        datetime(2005, 1, 1),
        datetime(2005, 2, 1),
        datetime(2005, 3, 1),
        datetime(2005, 4, 1),
        datetime(2005, 5, 1),
        datetime(2005, 6, 1),
        datetime(2005, 7, 1),
        datetime(2005, 8, 1),
        datetime(2005, 9, 1),
        datetime(2005, 10, 1),
        datetime(2005, 11, 1),
        datetime(2005, 12, 1),
    ] 


    def requires(self):
        for date in self.date_list:
            yield MonthDownload(start_date=date)


class MonthDownload(luigi.Task):
    start_date = luigi.Parameter(default=str)
    max_workers = luigi.Parameter(default=int, significant=False)
    chunksize = luigi.Parameter(default=int, significant=False)

    @property
    def client(self):
        key = s3Bucket().key
        secret = s3Bucket().secret
        return s3.S3Client(aws_access_key_id=key,
                           aws_secret_access_key=secret)

    @property
    def file_key(self):
        path = os.path.join(
            'raw_data_narr',
            f"narr_{self.start_date.strftime('%Y%m')}.zip")
        return path

    @property
    def bands(self):
        bands_grib = pd.read_csv('data/inventory_grib.csv')
        return bands_grib[bands_grib[' elem'].str.contains('wind')]['MsgNum'].tolist()

    def requires(self):
        pass

    def complete(self):
        check_path = f's3://{s3Bucket().bucket}/{self.file_key}'
        return self.client.exists(check_path)

    def run(self):
        logger.info(f'Things are running in {self.start_date}')
        download_process_data_local(self.start_date,
                                    aws_key=s3Bucket().key,
                                    aws_secret=s3Bucket().secret,
                                    aws_bucket_name=s3Bucket().bucket,
                                    bands=self.bands,
                                    max_workers=int(self.max_workers),
                                    chunksize=int(self.chunksize),
                                    delta=None,
                                    end_date=None
                            )


class ReprojectMaskRaster(luigi.Task):

    def requires(self):
        return GRIB2TIFF(
            start_date = self.start_date,
            save_dir = self.save_dir,
            bands = self.bands
        )

    def output(self):
        return luigi.LocalTarget(f'{self.save_path}')

    def run(self):
        pass


class ExtractGRB2CSV(luigi.Task):
    start_date = luigi.Parameter(significant=True)
    save_path = luigi.Parameter(default=str)
    message_extract=luigi.Parameter(significant=True)
    client_name=luigi.Parameter()


    def requires(self):
        return MonthFTPDOwnload(start_date=self.start_date,
                               save_path=self.save_path)

    @property
    def path_extract(self):
        path_extract = os.path.join(
            self.save_path,
            'csv_extract',
            start_date.strptime('%Y_%M')
        )

        return path_extract

    def output(self):
        return luigi.LocalTarget(self.path_extract)

    def run(self):
        return docker_execute(client_name=self.client_name,
                              path_dir=self.path_extract,
                             message_code=self.message_extract)

if __name__ == '__main__':
    config = luigi.configuration.get_config()
    slack_url = config.get('luigi-monitor', 'slack_url', None)
    max_print = config.get('luigi-monitor', 'max_print', 5)
    username = config.get('luigi-monitor', 'username', None)

    with monitor(slack_url=slack_url, username=username, max_print=max_print):
        luigi.run(main_task_cls=OrderDownload) 


