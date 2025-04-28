import pandas as pd
import config as config
import postgresql_query as query
import clickhouse_query as ch_query
from PostgreSQLDatabase import PostgreSQLDatabase
from ClickHouseClient import ClickHouseClient
from psycopg.errors import OperationalError


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


def prepare_data(df):
    """Преобразуем DataFrame в список кортежей с обработкой NULL"""
    my_list = []
    for _, row in df.iterrows():
        # Заменяем NaN/None на None (для PostgreSQL NULL)
        row = row.where(pd.notnull(row), None)
        # Конвертируем в кортеж
        my_list.append(tuple(row))
    return my_list


def init_postgreSQLDatabase(cur):
    try:
        # 1. Создание таблиц
        create_data(cur)

        # 2. Проверка пустой таблицы
        cur.execute("SELECT id FROM temp_data LIMIT 1")
        assert not bool(cur.fetchall())

        # 3. Чтение и подготовка данных
        # Это нужно откомментировать
        df = pd.read_excel(config.file_path)
        data = prepare_data(df)

        # 4. Вставка данных
        insert_data(cur, data)
        print("Данные успешно загружены")
    except (Exception, OperationalError) as e:
        print(f"Ошибка при выполнении команд: {e}")


def connect_to_clickhouse(client_cur, data):
    # c.query(ch_query.drop_purchases)
    # c.query(ch_query.drop_date_purchases)
    # c.query(ch_query.drop_date_purchases_by_gender)
    for i in range(len(data)):
        client_cur.execute(ch_query.create_purchases, data[i])
    client_cur.execute(ch_query.create_date_purchases)
    client_cur.execute(ch_query.create_date_purchases_by_gender)
    client_cur.execute(ch_query.insert_data_to_purchases)
    client_cur.execute(ch_query.insert_data_to_date_purchases)
    client_cur.execute(ch_query.insert_data_to_date_purchases_by_gender)
    amount = client_cur.execute(ch_query.get_sum_all_time).first_row[0]
    amount_gender_E = client_cur.execute(ch_query.get_sum_all_time_by_gender.format(gender="E")).first_row[0]
    amount_gender_K = client_cur.execute(ch_query.get_sum_all_time_by_gender.format(gender="K")).first_row[0]
    print(amount, amount_gender_E, amount_gender_K)


if __name__ == "__main__":
    postgre_db = PostgreSQLDatabase()
    ch_client = ClickHouseClient()
    with postgre_db as cur:
        init_postgreSQLDatabase(cur)
    with postgre_db as db_cur:
        data = db_cur.execute("""SELECT GENDER, PRICE, AMOUNT, DATE_ FROM temp_data""").fetchall()
    with ch_client as client_cur:
        connect_to_clickhouse(client_cur, data)
