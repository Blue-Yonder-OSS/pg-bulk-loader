import psycopg
import pandas as pd
from src.pg_bulk_loader.batch.pg_connection_detail import PgConnectionDetail


def init_db(postgresql):
    args = postgresql.dsn()
    conn = psycopg.connect(host=args['host'], port=args['port'], dbname='postgres', user=args['user'], password='')
    cursor = conn.cursor()
    create_table_query = """CREATE TABLE public.aop_dummy (
        p_code text NOT NULL,
        s_code text NOT NULL,
        _from date NOT NULL,
        upto date NOT NULL,
        mean numeric NOT NULL,
        ss numeric NOT NULL DEFAULT 0.0,
        CONSTRAINT aggregated_order_projections_dummy_pk PRIMARY KEY (p_code, s_code, _from)
    );"""
    cursor.execute(create_table_query)

    cursor.close()
    conn.commit()
    conn.close()


def create_indexes(pg_connection: PgConnectionDetail):
    pg_conn = pg_connection.get_psycopg_connection()
    try:
        create_index1 = "CREATE INDEX aop_dummy_batch_scope_index ON public.aop_dummy USING btree (upto);"
        create_index2 = "CREATE INDEX p_s_aopd_index ON public.aop_dummy USING btree (p_code, s_code);"

        curser = pg_conn.cursor()
        curser.execute(create_index1)
        curser.execute(create_index2)
        curser.close()
        pg_conn.commit()
    finally:
        pg_conn.close()


def drop_indexes(pg_connection: PgConnectionDetail):
    pg_conn = pg_connection.get_psycopg_connection()
    try:
        query = "DROP INDEX aop_dummy_batch_scope_index, p_s_aopd_index"

        curser = pg_conn.cursor()
        curser.execute(query)
        curser.close()
        pg_conn.commit()
    finally:
        pg_conn.close()


def fetch_result(postgresql, query):
    args = postgresql.dsn()
    conn = psycopg.connect(host=args['host'], port=args['port'], dbname='postgres', user=args['user'], password='')
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(result, columns=["p_code", "s_code", "_from", "upto", "mean", "ss"])


def fetch_rows_count_and_assert(pg_conn_details: PgConnectionDetail, table_name: str, expected):
    pg_conn = pg_conn_details.get_psycopg_connection()
    try:
        curser = pg_conn.cursor()
        result = curser.execute(f"select count(1) from {table_name}").fetchone()
        curser.close()
        pg_conn.commit()
        return result[0]
    finally:
        pg_conn.close()


def truncate_table_and_assert(pg_conn_details: PgConnectionDetail, table_name: str):
    pg_conn = pg_conn_details.get_psycopg_connection()
    try:
        curser = pg_conn.cursor()
        curser.execute(f"truncate table {table_name};")
        curser.close()
        pg_conn.commit()

        # Validate from DB
        fetch_rows_count_and_assert(pg_conn_details, table_name, expected=0)
    finally:
        pg_conn.close()


def assert_data_count(result_count, expected_count):
    assert result_count == expected_count, f"Expected: {expected_count}, Actual: {result_count}"
