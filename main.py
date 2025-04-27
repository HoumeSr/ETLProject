import clickhouse_connect
import pandas as pd
import config as config
import postgresql_query as query
import clickhouse_query as ch_query
from PostgreSQLDatabase import PostgreSQLDatabase
from ClickHouseClient import ClickHouseClient
from psycopg.errors import OperationalError
from datetime import datetime
import openpyxl


def create_data(cur):
    def create_temp_table(cur):
        cur.execute(query.create_query)

    def create_all_tables(cur):
        cur.execute(query.create_query2)

    create_temp_table(cur)
    create_all_tables(cur)


def insert_data(cur, lst_of_tuples):
    def insert_into_temp_table(cur, lst_of_tuples):
        cur.executemany(query.insert_query1, lst_of_tuples)

    def insert_into_all_tables(cur):
        cur.execute(query.insert_into_tmp_table)

    insert_into_temp_table(cur, lst_of_tuples)
    insert_into_all_tables(cur)


# def prepare_data(df):
#     """Преобразуем DataFrame в список кортежей с обработкой NULL"""
#     my_list = []
#     for _, row in df.iterrows():
#         # Заменяем NaN/None на None (для PostgreSQL NULL)
#         row = row.where(pd.notnull(row), None)
#         # Конвертируем в кортеж
#         my_list.append(tuple(row))
#     return my_list


def init_postgreSQLDatabase(cur):
    try:
        # 1. Создание таблиц
        create_data(cur)

        # 2. Проверка пустой таблицы
        cur.execute("SELECT id FROM temp_data LIMIT 1")
        assert not bool(cur.fetchall())

        # 3. Чтение и подготовка данных
        # Это нужно откомментировать
        # df = pd.read_excel(config.file_path)
        # data = prepare_data(df)

        # Это я сделал только для себя. Считывает только первые 10 строчек из xls файла
        data = []
        book = openpyxl.load_workbook(filename=config.file_path, read_only=True, data_only=True)
        first_sheet = book.worksheets[0]
        rows_generator = first_sheet.values
        header_row = next(rows_generator)
        for index, row in zip(range(10), rows_generator):
            data.append(row)

        # 4. Вставка данных
        insert_data(cur, data)
        print("Данные успешно загружены")
    except (Exception, OperationalError) as e:
        print(f"Ошибка при выполнении команд: {e}")


def connect_to_clickhouse(cur):
    client = ClickHouseClient()

    with client as client_cur:
        # Пример выполнения запроса
        # client.query(query.clickhouse_create_query)
        lst = [
        {'id': 1, 'name': 'Товар 3', 'price': 100.50, 'timestamp': '2023-05-15 14:30:22'},
        {'id': 2, 'name': 'Товар 4', 'price': 200.00, 'timestamp': '2023-05-15 14:30:22'}]
        row1 = ['Товар 1', 100.50, 1, datetime.now()]
        row2 = ['Товар 1', 100.50, 1, datetime.now()]
        data = [row1, row2]

        client_cur.execute(ch_query.clickhouse_insert_query, data)


if __name__ == "__main__":
    postgre_db = PostgreSQLDatabase()
    ch_client = ClickHouseClient()
    # Это нужно откомментировать, когда таблица postgresql пустая
    # with postgre_db as cur:
    #     init_postgreSQLDatabase(cur)
    with ch_client as client_cur:
        connect_to_clickhouse(client_cur)
    c = clickhouse_connect.get_client(username="admin", password="admin")


