from typing import List

from psycopg2 import connect, extensions

from utils.get_environment_variable import get_environment_variable


class PostgreSQLEngine(object):
    """PostgreSQL Psycopg2 Engine
    Attributes:
        dbname: database name
        user: database username
        password: database password
        host: database hostname
        port: database port, defaults to 5432
        connection: the Psycopg2 PostgreSQL Connection
    """
    def __init__(self):
        self.dbname = get_environment_variable("DB_NAME")
        self.user = get_environment_variable("DB_USER")
        self.password = get_environment_variable("DB_PASSWORD")
        self.host = get_environment_variable("DB_HOST")
        self.port = get_environment_variable("DB_PORT")
        self.connection = self.create_connection()

    def create_connection(self) -> extensions.connection:
        """Creates PostgreSQL Psycopg2 Connection
        :return: PostgreSQL Psycopg2 Connection
        :rtype: psycopg2.extensions.connection
        """
        return connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    def insert_row_into_db(self, table: str, row: List):
        cur = self.connection.cursor()

        try:
            cur.execute(
                f"""INSERT INTO "{table}" 
                    ("playerName", "position", "age", "team", "gamesPlayed",
                    "gamesStarted", "minutesPlayed", "fieldGoals", 
                    "fieldGoalAttempts", "fieldGoalPercentage", 
                    "threePointFieldGoals", "threePointFieldGoalAttempts",
                    "threePointFieldGoalPercentage", "twoPointFieldGoals",
                    "twoPointFieldGoalAttempts", "twoPointFieldGoalPercentage",
                    "effectiveFieldGoalPercentage", "freeThrows", 
                    "freeThrowAttempts", "freeThrowPercentage", 
                    "offensiveRebounds", "defensiveRebounds", "totalRebounds",
                    "assists", "steals", "blocks", "turnovers", 
                    "personalFouls", "points", "datetimePulled"
                    )
                    VALUES (
                    {",".join(["%s" for _ in range(len(row))])}
                    )
                """,
                row
            )
        finally:
            cur.close()
