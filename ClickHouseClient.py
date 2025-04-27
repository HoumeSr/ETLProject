from clickhouse_driver import Client


class ClickHouseClient:
    def __init__(self):
        self._client = None
        try:
            self._client = Client(host="127.0.0.1", port=9000, user='admin', password='admin')
            print("Соединение с клиентом ClickHouse установлено")
        except Exception as e:
            print("Ошибка при соединении с клиентом ClickHouse", e)

    def __enter__(self):
        return self._client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.disconnect()
        print("Соединение с клиентом ClickHouse закрыто")

