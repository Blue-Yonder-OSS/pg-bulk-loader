from src.batch.pg_connection_detail import PgConnectionDetail
from src.batch.fast_load_hack import FastLoadHack
from src.batch.batch_insert import BatchInsert
import pandas as pd
from src.utils.time_it_decorator import time_it
import asyncio
from concurrent.futures import ProcessPoolExecutor


def run_batch_task(data_df, batch_size, pg_conn_details, table_name, min_conn, max_conn):  # pragma: no cover
    """
        Helper method to achieve multiprocess execution with ProcessPoolExecutor class.
        This method can be executed per process.
    """
    asyncio.run(run(data_df, batch_size, pg_conn_details, table_name, min_conn, max_conn))


async def run(data_df, batch_size, pg_conn_details, table_name, min_conn, max_conn):
    batch_ = BatchInsert(
        batch_size=batch_size,
        pg_conn_details=pg_conn_details,
        table_name=table_name,
        min_conn=min_conn,
        max_conn=max_conn
    )
    try:
        await batch_.open_connection_pool()
        await batch_.execute(data_df)
    finally:
        await batch_.close_connection_pool()


@time_it
async def batch_insert_to_postgres(
        pg_conn_details: PgConnectionDetail,
        table_name: str,
        data_df: pd.DataFrame,
        batch_size: int,
        min_conn_pool_size: int = 5,
        max_conn_pool_size: int = 10,
        use_multi_process_for_create_index: bool = True,
        drop_and_create_index: bool = True
):
    """
    :param pg_conn_details: Instance of PgConnectionDetail class which contains postgres connection details
    :param table_name: Name of the table
    :param data_df: Data to be inserted
    :param batch_size: Number of records to insert at a time
    :param min_conn_pool_size: Min PG connections created and saved in connection pool
    :param max_conn_pool_size: Max PG connections created and saved in connection pool
    :param use_multi_process_for_create_index: This being True, makes the index(es) creation in parallel
    :param drop_and_create_index: This being True, drops the indexes from the table, inserts data and crates them back
    Note: Only non-pk indexes are dropped and re-created.
    :return:
    """
    fast_load_hack = FastLoadHack(pg_conn_details=pg_conn_details, table_name=table_name)
    indexes = {}
    if drop_and_create_index:
        indexes: dict = fast_load_hack.get_indexes()
        print(f'Indexes to be dropped and re-created: {indexes.keys()}')
        fast_load_hack.drop_indexes(list(indexes.keys()))

    try:
        await run(data_df, batch_size, pg_conn_details, table_name, min_conn_pool_size, max_conn_pool_size)
    except Exception as e:
        raise e
    finally:
        if drop_and_create_index:
            fast_load_hack.create_indexes(list(indexes.values()), use_multi_process_for_create_index)


@time_it
async def batch_insert_to_postgres_with_multi_process(
        pg_conn_details: PgConnectionDetail,
        table_name: str,
        data_generator,
        batch_size: int,
        min_conn_pool_size: int = 5,
        max_conn_pool_size: int = 10,
        no_of_processes: int = 1,
        drop_and_create_index: bool = True
):
    """
    This wrapper function is useful when you have a data generator on Dataframes
    The data_generator is iterated over a loop and every df is given to a separate process.

    :param pg_conn_details: Instance of PgConnectionDetail class which contains postgres connection details
    :param table_name: Name of the table
    :param data_generator: generator to provide dataset per process
    :param batch_size: Number of records to insert at a time
    :param min_conn_pool_size: Min PG connections created and saved in connection pool
    :param max_conn_pool_size: Max PG connections created and saved in connection pool
    :param no_of_processes: int = 1
    :param drop_and_create_index: This being True, drops the indexes from the table, inserts data and crates them back
    Note: Only non-pk indexes are dropped and re-created.
    :return:
    """
    if not data_generator:
        raise Exception("Invalid data input!")

    fast_load_hack = FastLoadHack(pg_conn_details=pg_conn_details, table_name=table_name)
    indexes = {}
    if drop_and_create_index:
        indexes = fast_load_hack.get_indexes()
        print(f'Indexes to be dropped and re-created: {indexes.keys()}')
        fast_load_hack.drop_indexes(list(indexes.keys()))

    try:
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=no_of_processes) as executor:
            tasks = []
            for df in data_generator:
                tasks.append(
                    loop.run_in_executor(
                        executor,
                        run_batch_task,
                        df,
                        batch_size,
                        pg_conn_details,
                        table_name,
                        min_conn_pool_size,
                        max_conn_pool_size
                    )
                )
        await asyncio.gather(*tasks)
    except Exception as e:
        raise e
    finally:
        if drop_and_create_index:
            fast_load_hack.create_indexes(list(indexes.values()), use_multi_process=True)