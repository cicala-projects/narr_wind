"""
Luigi configuration classes to pass credentials to Luihi tasks
"""

import luigi
from sqlalchemy import create_engine


class s3Bucket(luigi.Config):
    key = luigi.Parameter()
    secret = luigi.Parameter()
    bucket = luigi.Parameter()


