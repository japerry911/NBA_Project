import logging.config
import uuid

from configs.config import config


uuid_str = str(uuid.uuid4()).upper()


class ScrapeStats(object):
    def __init__(self):
        self.logger = logging.getLogger(__name__ + "." + uuid_str)
        self.set_logger()

    def scrape_stats(self):
        self.logger.info("Starting Stats Scraper")

        self.logger.info("Stats Scraper Completed Successfully")

    @staticmethod
    def set_logger():
        logging.config.dictConfig(config["logging_dict"])
