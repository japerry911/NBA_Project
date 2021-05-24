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
        self.dbname = get_environment_variable("DBNAME")
        self.user = get_environment_variable("USER")
        self.password = get_environment_variable("PASSWORD")
        self.host = get_environment_variable("HOST")
        self.port = get_environment_variable("PORT")
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
                f"""INSERT INTO {table} 
                    VALUES (
                    {",".join(["%s" for _ in range(len(row))])}
                    )
                """,
                row
            )
        finally:
            cur.close()
