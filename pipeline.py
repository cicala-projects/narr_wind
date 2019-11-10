"""
Luigi pipeline to download and extract NARR data from NOAA FTP servers,
extract, and process data. 
"""

import os
import luigi
from datetime import datetime
from luigi_monitor import monitor
from luigi.contrib import docker_runner, s3, external_program

from src.get_narr import (datetime_range,
                         retrieve_individual_month,
                         dict_product,
                         docker_execute_degrib)


class OrderDownload(luigi.WrapperTask):
    months_narr = {
        'start_date': datetime_range(datetime(2000, 1, 1),
                                     datetime(2001, 1, 1),
                                     {'months': 1 }),
        'bands': [283, 284]
    }

    def requires(self):
            for month_message in dict_product(self.months_narr):
                yield MonthDownload(start_date=months_narr['start_date'])


class MonthDownload(luigi.task):
    start_date = luigi.Parameter(significant=True)
    save_path = luigi.Parameter()


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
            f'narr_{self.start_date.strftime('%Y%m')}.zip'
        )

        return path

    def complete(self):
        return self.client.exists(self.file_key)

    def run(self):
        stream_time_range_s3(start_time=self.start_date,
                             end_date=self.start_date + relativedelta({'months': 1}),
                             aws_key=s3Bucket().key,
                             aws_secret=s3Bucket().secret,
                             aws_bucket_name=s3Bucket().bucket,
                             key=self.file_key,
                             max_workers=self.max_workers)


def GRIB2TIFF(external_program.ExternalProgramTask):
    start_date = luigi.Parameter()
    save_dir = luigi.Parameter()
    bands = luigi.Parameter()

    @property
    def save_path(self):
        path = os.path.join(f'tiff_{self.start_date.strftime("%Y%m")}')

    @propery
    def s3_path(self): 
        path = os.path.join(
            s3Bucket().bucket,
            'raw_data_narr',
            f'narr_{self.start_date.strftime('%Y%m')}.zip'
        )

    @property
    def bands_parse(self):
        return ','.join([str(a) for a in self.bands])

    def requires(self):
        return MonthDownload(start_date=self.start_date,
                             save_path=self.save_path)

    def output(self):
        return luigi.LocalTarget(f'{self.save_path}')

    def program_args(self):
        return ['./gdal_raster.sh', 
                self.s3_path,
                self.save_path,
                self.bands_parse
               ]


def ReprojectMaskRaster(self.Task):

    def requires(self):
        return GRIB2TIFF(
            start_date = self.start_date,
            save_dir = self.save_dir,
            bands = self.bands
        )

    def output(self):
        return luigi.LocalTarget(f'{self.save_path}')

    def run():




def ExtractGRB2CSV(luigi.Task):
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

