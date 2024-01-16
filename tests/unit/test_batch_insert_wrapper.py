import unittest
from unittest.mock import patch

import pytest
import testing.postgresql
import pandas as pd

from src.pg_bulk_loader.batch.batch_insert_wrapper import batch_insert_to_postgres
from src.pg_bulk_loader.batch.pg_connection_detail import PgConnectionDetail
from .pg_helper import init_db, fetch_rows_count_and_assert, truncate_table_and_assert, create_indexes, drop_indexes


class TestBatchInsertWrapper(unittest.IsolatedAsyncioTestCase):

    postgres_ = None

    async def test_batch_insert_when_input_is_null(self):
        with pytest.raises(Exception) as e:
            await batch_insert_to_postgres(
                pg_conn_details=self.pg_connection,
                table_name="aop_dummy",
                input_data=None,
                batch_size=200,
                min_conn_pool_size=5,
                max_conn_pool_size=7,
                use_multi_process_for_create_index=False,
                drop_and_create_index=False
            )
        assert str(e.value) == "Data input cannot be empty!"
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=0)

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

    @patch("src.pg_bulk_loader.batch.batch_insert_wrapper.run")
    async def test_batch_insert_when_exception_is_thrown(self, mock_run):
        mock_run.side_effect = Exception("Custom Exception!")

        input_df = pd.read_csv("tests/unit/aopd-1k.csv")

        with pytest.raises(Exception) as e:
            await batch_insert_to_postgres(
                pg_conn_details=self.pg_connection,
                table_name="aop_dummy",
                input_data=input_df,
                batch_size=200,
                min_conn_pool_size=5,
                max_conn_pool_size=7,
                use_multi_process_for_create_index=True,
                drop_and_create_index=True
            )

        assert str(e.value) == "Custom Exception!"

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=0)

    async def test_batch_insert_when_table_does_not_have_indexes_and_drop_and_create_index_is_true(self):
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=5,
            max_conn_pool_size=7,
            use_multi_process_for_create_index=True,
            drop_and_create_index=True
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_table_does_not_have_indexes_and_drop_and_create_index_is_false(self):
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=5,
            max_conn_pool_size=7,
            use_multi_process_for_create_index=True,
            drop_and_create_index=False
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_table_have_indexes_and_drop_and_create_index_is_true(self):
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")
        create_indexes(self.pg_connection)

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=5,
            max_conn_pool_size=7,
            use_multi_process_for_create_index=True,
            drop_and_create_index=True
        )
        drop_indexes(self.pg_connection)
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_table_have_indexes_and_drop_and_create_index_is_false(self):
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")
        create_indexes(self.pg_connection)

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=5,
            max_conn_pool_size=7,
            use_multi_process_for_create_index=True,
            drop_and_create_index=False
        )
        drop_indexes(self.pg_connection)
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_table_have_indexes_and_drop_and_create_index_happens_sequentially(self):
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")
        create_indexes(self.pg_connection)

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=5,
            max_conn_pool_size=7,
            use_multi_process_for_create_index=False,
            drop_and_create_index=True
        )
        drop_indexes(self.pg_connection)
        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_conn_pool_has_less_connection_than_total_batches(self):
        """
        Total data size is: 1000
        batch size is: 200
        So total number of batches will be 5.
        We are keeping max 3 connections in the pool.

        :expect - The code to pass
        """
        input_df = pd.read_csv("tests/unit/aopd-1k.csv")

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df,
            batch_size=200,
            min_conn_pool_size=2,
            max_conn_pool_size=3,
            use_multi_process_for_create_index=True,
            drop_and_create_index=True
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")

    async def test_batch_insert_when_input_is_given_as_data_generator(self):
        input_df_generator = pd.read_csv("tests/unit/aopd-1k.csv", chunksize=500)

        await batch_insert_to_postgres(
            pg_conn_details=self.pg_connection,
            table_name="aop_dummy",
            input_data=input_df_generator,
            batch_size=200,
            min_conn_pool_size=2,
            max_conn_pool_size=3,
            use_multi_process_for_create_index=True,
            drop_and_create_index=True
        )

        # Validate from DB
        fetch_rows_count_and_assert(self.pg_connection, "aop_dummy", expected=1000)

        # Truncate table and assert
        truncate_table_and_assert(self.pg_connection, "aop_dummy")
