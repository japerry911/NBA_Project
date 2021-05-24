from pipeline.load_data.load_data import LoadData


def main():
    LoadData(
        "dailyStatsLanding"
    ).load_data()


if __name__ == "__main__":
    main()
