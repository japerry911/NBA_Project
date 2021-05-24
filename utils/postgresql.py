from typing import List

from psycopg2 import connect, extensions


class PostgreSQLEngine(object):
    """PostgreSQL Psycopg2 Engine
    Parameters:
        dbname: database name
        user: database username
        password: database password
        host: database hostname
        port: database port, defaults to 5432
    Attributes:
        dbname: database name
        user: database username
        password: database password
        host: database hostname
        port: database port, defaults to 5432
        connection: the Psycopg2 PostgreSQL Connection
    """
    def __init__(
            self,
            dbname: str,
            user: str,
            password: str,
            host: str,
            port: int = 5432
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
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
