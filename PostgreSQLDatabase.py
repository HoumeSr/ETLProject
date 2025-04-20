import psycopg2
from psycopg2 import DatabaseError
from config import *
from psycopg2 import pool


class PostgreSQLDatabase:
    def __init__(self):
        self._connection = None
        try:
            self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                minconn=1, maxconn=10, user=user, password=password, host=host, port=port, database=database_name)
            if self._connection_pool:
                print("Пул соединений создан успешно")
        except (Exception, DatabaseError) as error:
            print("Ошибка при подключении PostgreSQL", error)

    def __enter__(self):
        cursor = None
        try:
            self._connection = self._connection_pool.getconn()
            cursor = self._connection.cursor()
            if self._connection:
                print("Соединение установлено")
        except (Exception, DatabaseError) as error:
            print("Ошибка при соединении PostgreSQL", error)
        return cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection:
            self._connection.commit()
            self._connection_pool.putconn(self._connection)
            print("Соединение закрыто")