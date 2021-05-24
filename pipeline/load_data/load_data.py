import logging.config

from utils.postgresql import PostgreSQLEngine


class LoadData(object):
    def __init__(self):
        pass

    def load_data(self):
        host = "batyr.db.elephantsql.com"
        user = "owtiurxu"
        dbname = "owtiurxu"
        password = "QCoCzveGAH8lXLFYQIixt6B_argF6_V2"

        a = PostgreSQLEngine(
            dbname, user, password, host
        )

        print(a.connection)
        print(type(a.connection))

        a.connection.close()
