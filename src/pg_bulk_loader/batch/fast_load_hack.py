from .pg_connection_detail import PgConnectionDetail
from concurrent.futures import ProcessPoolExecutor
from ..utils.time_it_decorator import time_it


class FastLoadHack:

    def __init__(self, pg_conn_details: PgConnectionDetail, table_name: str):
        self.pg_conn_details = pg_conn_details
        self.schema = self.pg_conn_details.schema
        self.table_name = table_name

    @time_it
    def set_table_unlogged(self):  # pragma: no cover
        pg_session = self.pg_conn_details.get_psycopg_connection()
        try:
            with pg_session.cursor() as cursor:
                query = f"Alter table {self.schema}.{self.table_name} SET UNLOGGED;"
                cursor.execute(query)
                pg_session.commit()
        finally:
            pg_session.close()

    @time_it
    def set_table_logged(self):  # pragma: no cover
        pg_session = self.pg_conn_details.get_psycopg_connection()
        try:
            with pg_session.cursor() as cursor:
                query = f"Alter table {self.schema}.{self.table_name} SET LOGGED;"
                cursor.execute(query)
                pg_session.commit()
        finally:
            pg_session.close()

    @time_it
    def drop_indexes(self, index_names: list[str]):
        if not index_names:
            return

        pg_session = self.pg_conn_details.get_psycopg_connection()
        try:
            with pg_session.cursor() as cursor:
                query = f"DROP INDEX IF EXISTS {','.join(index_names)};"
                cursor.execute(query)
            pg_session.commit()
        finally:
            pg_session.close()

    def create_index(self, index_query: str):
        pg_session = self.pg_conn_details.get_psycopg_connection()
        try:
            with pg_session.cursor() as cursor:
                cursor.execute(index_query)
                pg_session.commit()
        finally:
            pg_session.close()

    @time_it
    def create_indexes(self, index_queries: list[str], use_multi_process=False):
        if use_multi_process:
            with ProcessPoolExecutor() as executor:
                for index_query in index_queries:
                    executor.submit(self.create_index, index_query)
        else:
            for index_query in index_queries:
                self.create_index(index_query)

    def get_indexes(self):
        pg_session = self.pg_conn_details.get_psycopg_connection()
        try:
            with pg_session.cursor() as cursor:
                query = f"""
                    select indexname, indexdef from pg_indexes where 
                    tablename='{self.table_name}' and indexdef like 'CREATE INDEX %'
                """
                results = cursor.execute(query)
                indexes = {}
                for result in results:
                    # Adding schema in front of index name is needed to find and drop the index
                    indexes[f"{self.schema}.{result[0]}"] = result[1]
                return indexes
        finally:
            pg_session.close()
