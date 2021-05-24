import os
import logging.config

import boto3
import pandas as pd

from utils.postgresql import PostgreSQLEngine


class LoadData(object):
    def __init__(
            self,
            table: str,
            local_mode: bool = True,
            parquet_filename: str = "2021/05/23/data-2021-05-23T22:09:41.100389.parquet",
    ):
        self.parquet_filename = parquet_filename
        self.table = table
        self.local_mode = local_mode
        self.postgresql_engine = PostgreSQLEngine()

    def load_data(self):
        try:
            try:
                os.mkdir("/tmp")
            except FileExistsError:
                pass

            self.download_file_and_load_into_table()
        finally:
            self.postgresql_engine.connection.close()

    def download_file_and_load_into_table(self):
        s3 = boto3.client("s3")

        s3.download_file(
            "nba-project-1233123494218913",
            self.parquet_filename,
            "./tmp/tmp.parquet"
        )

        df = pd.read_parquet("./tmp/tmp.parquet")

        for row in df.values:
            self.postgresql_engine.insert_row_into_db(
                self.table,
                row
            )

        self.postgresql_engine.connection.commit()

    @staticmethod
    def set_up_local_boto_session():
        """Sets up local Boto Default Session, used for Local Development"""
        boto3.setup_default_session(profile_name="jack-development")
