import io
import pandas as pd
import asyncio
from .pg_connection_detail import PgConnectionDetail
from ..utils.common_utils import get_ranges
import logging
from retry import retry

logger = logging.getLogger(__name__)


class BatchInsert:

    def __init__(
            self,
            batch_size: int,
            table_name: str,
            pg_conn_details: PgConnectionDetail,
            min_conn: int = 5,
            max_conn: int = 10
    ):
        """
        :param batch_size: Number of records to insert at a time
        :param table_name: Name of the table
        :param pg_conn_details: Instance of PgConnectionDetail class which contains postgres connection details
        :param min_conn: Min PG connections created and saved in connection pool
        :param max_conn: Max PG connections created and saved in connection pool
        """
        self.batch_size = batch_size
        self.pg_conn_details = pg_conn_details
        self.table_name = table_name
        self.min_conn = min_conn
        self.max_conn = max_conn
        self.data_df = None
        self.pool = self.pg_conn_details.create_connection_pool(min_size=self.min_conn, max_size=self.max_conn)

    @retry(Exception, tries=3, delay=2, backoff=1)
    async def open_connection_pool(self):
        await self.pool.open(wait=True)

    @retry(Exception, tries=3, delay=2, backoff=1)
    async def close_connection_pool(self):
        await self.pool.close()

    async def execute(self, data_df: pd.DataFrame, col_names: list = None):
        """
        :param data_df: Data to be inserted
        :param col_names: column(s) to be considered for insert from the data_df
        """
        try:
            partition_ranges = get_ranges(data_df.shape[0], self.batch_size)
            logger.debug(f"Created {len(partition_ranges)} partitions!")

            if not partition_ranges:
                logger.warning("No data found to be inserted!")
                return

            if col_names:
                data_df = data_df[col_names]

            col_names = ",".join(col_names if col_names else data_df.columns)

            # Sharing the data among all processes
            self.data_df = data_df
            await self.handle_csv_bulk_insert(partition_ranges, col_names)
        except Exception as e:
            raise e
        finally:
            self.data_df = None

    async def handle_csv_bulk_insert(self, partition_ranges, col_names):
        tasks = []
        # At a time only self.min_conn async threads are allowed to execute
        semaphore = asyncio.Semaphore(self.min_conn)
        for range_ in partition_ranges:
            tasks.append(
                self.bulk_load(
                    range_, f"{self.pg_conn_details.schema}.{self.table_name}", col_names, self.pool, semaphore
                )
            )
        await asyncio.gather(*tasks)

    @retry(Exception, tries=3, delay=2, backoff=1)
    async def bulk_load(self, range_, table_name: str, col_names: list[str], pool, semaphore):
        async with semaphore:
            copy_query = f"""COPY {table_name} ({col_names}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')"""
            async with pool.connection(timeout=60) as pg_session:
                async with pg_session.cursor() as acur:
                    async with acur.copy(copy_query) as copy:
                        with io.StringIO() as io_buffer:
                            data_df = self.data_df[range_[0]: range_[1]]
                            data_df.to_csv(io_buffer, header=False, index=False)
                            io_buffer.seek(0)
                            await copy.write(io_buffer.read())
