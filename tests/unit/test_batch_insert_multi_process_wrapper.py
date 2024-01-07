import unittest
import pytest
import testing.postgresql
import pandas as pd
from src.pg_bulk_loader.batch.batch_insert_wrapper import batch_insert_to_postgres_with_multi_process
from src.pg_bulk_loader.batch.pg_connection_detail import PgConnectionDetail
from .pg_helper import init_db, fetch_rows_count_and_assert, truncate_table_and_assert, create_indexes, drop_indexes


class TestBatchInsertMultiProcessWrapper(unittest.IsolatedAsyncioTestCase):

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

    async def test_batch_insert_ms_when_generated_data_is_empty(self):
        df_data_generator = []
        with pytest.raises(Exception) as e:
            await batch_insert_to_postgres_with_multi_process(
                pg_conn_details=self.pg_connection,
                table_name="aop_dummy",
                data_generator=df_data_generator,
                batch_size=100,
                min_conn_pool_size=3,
                max_conn_pool_size=5,
                no_of_processes=2,
                drop_and_create_index=False
            )

        assert str(e.value) == "Invalid data input!"

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=0)

    async def test_batch_insert_ms_when_generated_data_is_null(self):
        df_data_generator = None
        with pytest.raises(Exception) as e:
            await batch_insert_to_postgres_with_multi_process(
                pg_conn_details=self.pg_connection,
                table_name="aop_dummy",
                data_generator=df_data_generator,
                batch_size=100,
                min_conn_pool_size=3,
                max_conn_pool_size=5,
                no_of_processes=2,
                drop_and_create_index=False
            )

        assert str(e.value) == "Invalid data input!"

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=0)

    async def test_batch_insert_ms_when_no_of_processes_are_null(self):
        """
        no_of_processes can be null. The ProcessPoolExecutor can determine the ideal value based on the number
        of cores available on the machine where test case is being executed.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        await batch_insert_to_postgres_with_multi_process(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            data_generator=df_data_generator,
            batch_size=100,
            min_conn_pool_size=3,
            max_conn_pool_size=5,
            no_of_processes=None,
            drop_and_create_index=True
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_ms_when_no_of_processes_is_zero(self):
        """
        no_of_processes can't be 0.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        with pytest.raises(Exception) as e:
            await batch_insert_to_postgres_with_multi_process(
                pg_conn_details=self.pg_connection,
                table_name="aop_dummy",
                data_generator=df_data_generator,
                batch_size=100,
                min_conn_pool_size=3,
                max_conn_pool_size=5,
                no_of_processes=0,
                drop_and_create_index=True
            )

        assert str(e.value) == "max_workers must be greater than 0"

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=0)

    async def test_batch_insert_ms_when_table_does_not_have_indexes_and_drop_and_create_index_is_true(self):
        """
        The total records are 1000
        The chunksize is 300. That means the generator will generate 4 dfs. First three will have data size of 300 and
        last one will have 100 records.

        Now the batch size is given as 100. That means in each generated df, the data will be inserted in the batch of
        100.

        Here every generated df will be processed in a separate process. So it depends on how many processes
        (no of cores) available to process in parallel. In the test cases, we are giving 2.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        await batch_insert_to_postgres_with_multi_process(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            data_generator=df_data_generator,
            batch_size=100,
            min_conn_pool_size=3,
            max_conn_pool_size=5,
            no_of_processes=2,
            drop_and_create_index=True
        )
        
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_ms_when_table_does_not_have_indexes_and_drop_and_create_index_is_false(self):
        """
        The total records are 1000
        The chunksize is 300. That means the generator will generate 4 dfs. First three will have data size of 300 and
        last one will have 100 records.

        Now the batch size is given as 100. That means in each generated df, the data will be inserted in the batch of
        100.

        Here every generated df will be processed in a separate process. So it depends on how many processes
        (no of cores) available to process in parallel. In the test cases, we are giving 2.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        await batch_insert_to_postgres_with_multi_process(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            data_generator=df_data_generator,
            batch_size=100,
            min_conn_pool_size=3,
            max_conn_pool_size=5,
            no_of_processes=2,
            drop_and_create_index=False
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_ms_when_table_have_indexes_and_drop_and_create_index_is_false(self):
        """
        The total records are 1000
        The chunksize is 300. That means the generator will generate 4 dfs. First three will have data size of 300 and
        last one will have 100 records.

        Now the batch size is given as 100. That means in each generated df, the data will be inserted in the batch of
        100.

        Here every generated df will be processed in a separate process. So it depends on how many processes
        (no of cores) available to process in parallel. In the test cases, we are giving 2.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        create_indexes(self.pg_connection)
        await batch_insert_to_postgres_with_multi_process(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            data_generator=df_data_generator,
            batch_size=100,
            min_conn_pool_size=3,
            max_conn_pool_size=5,
            no_of_processes=2,
            drop_and_create_index=False
        )
        drop_indexes(self.pg_connection)
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_ms_when_table_have_indexes_and_drop_and_create_index_is_true(self):
        """
        The total records are 1000
        The chunksize is 300. That means the generator will generate 4 dfs. First three will have data size of 300 and
        last one will have 100 records.

        Now the batch size is given as 100. That means in each generated df, the data will be inserted in the batch of
        100.

        Here every generated df will be processed in a separate process. So it depends on how many processes
        (no of cores) available to process in parallel. In the test cases, we are giving 2.
        """
        df_data_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=300)
        create_indexes(self.pg_connection)
        await batch_insert_to_postgres_with_multi_process(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            data_generator=df_data_generator,
            batch_size=100,
            min_conn_pool_size=3,
            max_conn_pool_size=5,
            no_of_processes=2,
            drop_and_create_index=True
        )
        drop_indexes(self.pg_connection)
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")
