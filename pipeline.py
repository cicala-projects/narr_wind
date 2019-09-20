"""
Luigi pipeline to download and extract NARR data from NOAA FTP servers,
extract, and process data. 
"""

import os
import luigi
from luigi.contrib import docker_runner
from datetime import datetime
from luigi_monitor import monitor

from src.get_narr import (datetime_range,
                         retrieve_individual_month,
                         dict_product,
                         docker_execute_degrib)


class OrderDownload(luigi.WrapperTask):
    months_narr = {
        'start_date': datetime_range(datetime(2000, 1, 1),
                                    datetime(2001, 1, 1),
                                     {'months': 1 }),
        'message_extract': [131, 133]
    }


class MonthFTPDownload(luigi.task):
    start_date = luigi.Parameter(significant=True)
    save_path = luigi.Parameter()

    @property
    def path_dir(self):
        path = os.path.join(
            self.save_path,
            start_date.strptime('%Y%m')
        )

        return path

    def run(self):
        retrieve_individual_month(start_date=start_date,
                                 save_path=save_path)

    def output(self):
        return luigi.LocalTarget(self.path_dir)


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

