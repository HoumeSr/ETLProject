from PostgreSQLDatabase import PostgreSQLDatabase
from psycopg2 import OperationalError


def command_1():
    database = PostgreSQLDatabase()
    try:
        with database as cursor:
            cursor.execute("")
    except (Exception, OperationalError) as error:
        print("Ошибка при выполнении команды", error)
