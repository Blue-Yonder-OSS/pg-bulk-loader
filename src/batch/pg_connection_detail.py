import psycopg
from psycopg_pool import AsyncConnectionPool
from src.utils.constants import SSL_MODE


class PgConnectionDetail:
    def __init__(
            self, user: str, password: str, database: str, schema: str, host: str = "localhost", port: int = 5432
    ):
        self.user = user
        self.password = password
        self.db = database
        self.host = host
        self.port = port
        self.schema = schema

    def create_connection_pool(self, min_size=5, max_size=10):
        if not min_size or not max_size:
            raise Exception("min and max connection pool size cannot be null or zero!")

        conn_str = f"host={self.host} user={self.user} password={self.password} dbname={self.db} port={self.port} sslmode={SSL_MODE}"
        return AsyncConnectionPool(conninfo=conn_str, min_size=min_size, max_size=max_size, open=False)

    def get_psycopg_connection(self):
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.db,
            user=self.user,
            password=self.password,
            sslmode=SSL_MODE
        )
