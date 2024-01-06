import unittest
import pytest
import psycopg
import testing.postgresql
import pandas as pd
from src.batch.batch_insert import BatchInsert
from src.batch.pg_connection_detail import PgConnectionDetail


def init_db(postgresql):
    args = postgresql.dsn()
    conn = psycopg.connect(host=args['host'], port=args['port'], dbname='postgres', user=args['user'], password='')
    cursor = conn.cursor()
    create_table_query = """CREATE TABLE public.test_batch (
        test_id int4 NOT NULL,
        test_name varchar NOT NULL,
        CONSTRAINT test_batch_pk PRIMARY KEY (test_id)
    );
    """
    cursor.execute(create_table_query)
    cursor.close()
    conn.commit()
    conn.close()


def fetch_result(postgresql, query):
    args = postgresql.dsn()
    conn = psycopg.connect(host=args['host'], port=args['port'], dbname='postgres', user=args['user'], password='')
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(result, columns=["test_id", "test_name"])


def assert_data_count(data, expected_count):
    assert len(data) == expected_count, f"Expected: {expected_count}, Actual: {(len(data))}"


class TestBatchInsert(unittest.IsolatedAsyncioTestCase):

    postgres_ = None

    @classmethod
    def setUpClass(cls):
        Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True, on_initialized=init_db)
        cls.postgres_ = Postgresql()
        params = cls.postgres_.dsn()
        params['password'] = ""
        params['schema'] = "public"
        params['database'] = "postgres"
        cls.pg_connection = PgConnectionDetail(**params)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.postgres_.stop()

    async def test_batch_insert_when_data_is_empty_df(self):
        """
        The process doesn't fail. It just doesn't insert anything
        """
        input_df = pd.DataFrame({
            'test_id': [],
            'test_name': [],
        })

        batch_ = BatchInsert(
            batch_size=1, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=1
        )
        await batch_.open_connection_pool()
        await batch_.execute(input_df)
        await batch_.close_connection_pool()

        # After process over the data should be wiped off from the batch_ object
        assert batch_.data_df is None

    async def test_batch_insert_when_invalid_pg_connection_details_object_passed(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=1, table_name="test_batch", pg_conn_details=None, min_conn=1, max_conn=1
            )
            assert batch_ is None
        assert e is not None

    async def test_batch_insert_when_min_conn_passed_as_0(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=0, max_conn=1
            )
            assert batch_ is None

        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_min_conn_passed_as_null(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=None, max_conn=1
            )
            assert batch_ is None

        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_max_conn_passed_as_0(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=0
            )
            assert batch_ is None

        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_max_conn_passed_as_null(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=None
            )
            assert batch_ is None
        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_both_min_and_max_conn_passed_as_null(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=None, max_conn=None
            )
            assert batch_ is None
        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_both_min_and_max_conn_passed_as_0(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=0, max_conn=0
            )
            assert batch_ is None
        assert str(e.value) == "min and max connection pool size cannot be null or zero!"

    async def test_batch_insert_when_both_min_conn_is_greater_than_max_conn(self):
        with pytest.raises(Exception) as e:
            batch_ = BatchInsert(
                batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=3, max_conn=1
            )
            assert batch_ is None
        assert str(e.value) == "max_size must be greater or equal than min_size"

    async def test_batch_insert(self):
        input_df = pd.DataFrame({
            'test_id': [1, 2, 3],
            'test_name': ["aditya", "adam", "lalu"],
        })

        batch_ = BatchInsert(
            batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=1
        )
        await batch_.open_connection_pool()
        await batch_.execute(input_df)
        await batch_.close_connection_pool()

        # After process over the data should be wiped off from the batch_ object
        assert batch_.data_df is None

        # Validate from DB
        data = fetch_result(self.postgres_, "select * from test_batch where test_id in (1, 2, 3)")
        assert_data_count(data, 3)

    async def test_batch_insert_when_columns_to_be_inserted_are_passed(self):
        input_df = pd.DataFrame({
            'test_id': [7, 8, 9],
            'test_name': ["aditya", "adam", "lalu"],
            'test_class': ['h1', 'h2', 'h3']
        })

        batch_ = BatchInsert(
            batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=1
        )
        await batch_.open_connection_pool()
        await batch_.execute(input_df, col_names=["test_id", "test_name"])
        await batch_.close_connection_pool()

        # Validate from DB
        data = fetch_result(self.postgres_, "select * from test_batch where test_id in (7, 8, 9)")
        assert_data_count(data, 3)

    async def test_batch_insert_where_pk_constraint_violates(self):
        input_df = pd.DataFrame({
            'test_id': [4, 5, 6],
            'test_name': ["aditya", "adam", "lalu"],
        })

        batch_ = BatchInsert(
            batch_size=2, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=1
        )
        await batch_.open_connection_pool()

        await batch_.execute(input_df)

        # Inserting the same data again
        input_df = pd.DataFrame({
            'test_id': [4, 5, 6],
            'test_name': ["aditya", "adam", "lalu"],
        })

        with pytest.raises(Exception) as e:
            await batch_.execute(input_df)

        await batch_.close_connection_pool()
        assert 'duplicate key value violates unique constraint "test_batch_pk"' in str(e)

        # Validate from DB
        data = fetch_result(self.postgres_, "select * from test_batch where test_id in (4, 5, 6)")
        assert_data_count(data, 3)

    async def test_batch_insert_when_pk_constraint_fails_in_last_batch(self):
        """
            First three batches will get inserted and only last batch will get rolled back
        """
        input_df = pd.DataFrame({
            'test_id': [10, 11, 12, 10],
            'test_name': ["Hari", "adam", "lalu", "Hari"],
        })

        batch_ = BatchInsert(
            batch_size=1, table_name="test_batch", pg_conn_details=self.pg_connection, min_conn=1, max_conn=1
        )
        await batch_.open_connection_pool()

        with pytest.raises(Exception) as e:
            await batch_.execute(input_df)

        await batch_.close_connection_pool()
        assert 'duplicate key value violates unique constraint "test_batch_pk"' in str(e)

        # Validate from DB
        data = fetch_result(self.postgres_, "select * from test_batch where test_id in (10, 11, 12)")
        assert_data_count(data, 3)
