from scrape_stats.scrape_stats_job import ScrapeStats


def main():
    ScrapeStats(local_mode=True).scrape_stats()


if __name__ == "__main__":
    main()
