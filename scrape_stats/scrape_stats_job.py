from datetime import datetime
import logging.config
from random import random
from time import perf_counter, sleep
from typing import List
import uuid

import boto3
from bs4 import BeautifulSoup, element
import pandas as pd
import requests

from scrape_stats.configs.config import config
from exceptions import ExponentialBackoffFailed


uuid_str = str(uuid.uuid4()).upper()


class ScrapeStats(object):
    """Used to Scrape Data from Basketball-Reference.com
    Parameters:
        local_mode: Boolean that tells script to set up local AWS mode if it's
            being ran locally
    Attributes:
        local_mode: Boolean that tells script to set up local AWS mode if it's
            being ran locally
        datetime_now: Datetime that is used in filling datetime_pulled column in
            data and used to create path for uploaded parquet file
        logger: Logging that is used to log info/warnings/errors/critical
            throughout running of script
    """

    # Scraped URL
    scraper_url = \
        "https://www.basketball-reference.com/leagues/NBA_2021_totals.html"

    def __init__(self, local_mode: bool = True):
        self.local_mode = local_mode
        self.datetime_now = datetime.now()
        self.logger = logging.getLogger(__name__ + "." + uuid_str)
        self.set_logger()

    def scrape_stats(self):
        """Pipeline for the data fetching process
            1.) Check if local mode and set up AWS default session if so
            2.) Fetch page HTML, nothing specific just all of the scraper_url
                page
            3.) Parse the HTML into a DataFrame
            4.) Convert the DataFrame to a Parquet file and upload to AWS S3
                Bucket
        """
        start_time = perf_counter()

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

        end_time = perf_counter()
        total_time = end_time - start_time

        self.logger.info(
            f"Stats Scraper Completed Successfully in {total_time:.2f} Seconds"
        )

    def parse_html_to_dataframe(self, html: str) -> pd.DataFrame:
        """Parses the Fetched HTML into Pandas DataFrame
        :param str html: fetched HTML text
        :return: stats data stored in Pandas DataFrame
        :rtype: Pandas DataFrame
        """
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
        """Fetches the HTML from Basketball-Reference.com
        :raises:
            ExponentialBackoffFailed: if the Exponential Backoff fails
        :return: HTML string of HTML text
        :rtype: string
        """
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
        """Converts Pandas DataFrame to Parquet and uploads to AWS S3
        :param pd.DataFrame df: Pandas DataFrame of stats data
        """
        path = "s3://nba-project-1233123494218913/" \
               f"{self.datetime_now.strftime('%Y')}/" \
               f"{self.datetime_now.strftime('%m')}/" \
               f"{self.datetime_now.strftime('%d')}/" \
               f"data-{self.datetime_now.isoformat()}.parquet"

        df.to_parquet(path=path)

    @staticmethod
    def parse_row(row: element.Tag) -> List:
        """Parses row of data into data list
        :params element.Tag row: Soup Element.Tag that needs to be parsed
        :return: list of the parsed row data columns
        :rtype: List
        """
        return [row_tag.get_text() for row_tag in row.find_all("td")]

    @staticmethod
    def set_logger():
        """Sets the Class Logger with the config file logging_dict value"""
        logging.config.dictConfig(config["logging_dict"])

    @staticmethod
    def set_up_local_boto_session():
        """Sets up local Boto Default Session, used for Local Development"""
        boto3.setup_default_session(profile_name="jack-development")
