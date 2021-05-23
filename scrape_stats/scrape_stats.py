from datetime import datetime
import logging.config
from random import random
from time import sleep
from typing import List
import uuid

import boto3
from bs4 import BeautifulSoup, element
import pandas as pd
import requests

from configs.config import config
from exceptions import ExponentialBackoffFailed


uuid_str = str(uuid.uuid4()).upper()


class ScrapeStats(object):
    scraper_url = \
        "https://www.basketball-reference.com/leagues/NBA_2021_totals.html"

    def __init__(self, local_mode: bool = True):
        self.local_mode = local_mode
        self.datetime_now = datetime.now()
        self.logger = logging.getLogger(__name__ + "." + uuid_str)
        self.set_logger()

    def scrape_stats(self):
        if self.local_mode is True:
            self.set_up_local_boto_session()

        self.logger.info("Starting Stats Scraper")

        self.logger.info("Fetching HTML")
        html = self.fetch_html()
        self.logger.info("Fetched HTML Successfully")

        self.logger.info("Parsing HTML to DataFrame")
        df = self.parse_html_to_dataframe(html)
        self.logger.info("Parsed HTML to DataFrame Successfully")

        self.logger.info("Converting DF to Parquet File and Loading to AWS S3")
        self.convert_to_parquet_and_load_to_s3(df)
        self.logger.info(
            "Converted DF to Parquet File and Loaded to AWS S3 Successfully"
        )

        self.logger.info("Stats Scraper Completed Successfully")

    def parse_html_to_dataframe(self, html: str) -> pd.DataFrame:
        soup = BeautifulSoup(html, 'lxml')

        header_row = [header_tag.get_text() for header_tag in
                      soup.select("table.stats_table thead tr th")
                      if header_tag.get_text() != "Rk"]
        rows = soup.select("table.stats_table tbody tr.full_table")

        data_list = [self.parse_row(row) for row in rows]

        df = pd.DataFrame(
            data=data_list,
            columns=header_row
        )
        df["Datetime_Pulled"] = self.datetime_now.isoformat()

        return df

    def fetch_html(self) -> str:
        response = None

        for i in range(5):
            response = requests.get(self.scraper_url)

            if response.status_code == 200:
                break

            if i == 4:
                self.logger.critical("Exponential Backoff Failed")
                raise ExponentialBackoffFailed

            self.logger.warning(
                "Request failed, attempting Exponential Backoff"
            )
            sleep((2 ** i) + random())

        return response.text

    def convert_to_parquet_and_load_to_s3(self, df: pd.DataFrame):
        path = "s3://nba-project-1233123494218913/" \
               f"{self.datetime_now.strftime('%Y')}/" \
               f"{self.datetime_now.strftime('%m')}/" \
               f"{self.datetime_now.strftime('%d')}/" \
               f"data-{self.datetime_now.isoformat()}.parquet"

        df.to_parquet(path=path)

    @staticmethod
    def parse_row(row: element.Tag) -> List:
        return [row_tag.get_text() for row_tag in row.find_all("td")]

    @staticmethod
    def set_logger():
        logging.config.dictConfig(config["logging_dict"])

    @staticmethod
    def set_up_local_boto_session():
        boto3.setup_default_session(profile_name="jack-development")
