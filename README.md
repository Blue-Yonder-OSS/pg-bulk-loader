# pandas-to-postgres

<h2>Overview</h2>

**pandas-to-postgres** is a utility package designed to facilitate faster bulk insertion from pandas DataFrame to a PostgreSQL table.

<h2>Purpose</h2>

This utility leverages the power of PostgreSQL in combination with Python to efficiently handle the bulk insertion of large datasets. The key features that contribute to its speed include:

1. Utilization of Postgres' copy command
2. Integration of Psycopg3's pipeline feature
3. Implementation of Python's coroutines
4. Harnessing the power of multiprocessing
5. Capability to drop indexes during insertion and recreate them in parallel

<h2>Usage</h2>

The utility provides the following useful functions and classes:

1. **batch_insert_to_postgres**
2. **batch_insert_to_postgres_with_multi_process**
3. **BatchInsert**


<h3>batch_insert_to_postgres() function</h3>

- `pg_conn_details`: Instance of the PgConnectionDetail class containing PostgreSQL server connection details.
- `table_name`: Name of the table for bulk insertion.
- `data_df`: Data in the form of a pandas DataFrame.
- `batch_size`: Number of records to insert and commit at a time.
- `min_conn_pool_size`, `max_conn_pool_size`: Determine the number of PostgreSQL connections in the connection pool.
- `drop_and_create_index`: Set to True if indexes need to be dropped during insert and re-created once insertion is complete.
- `use_multi_process_for_create_index`: Set to True if indexes need to be re-created in parallel; otherwise, they will be created sequentially.

<h3>batch_insert_to_postgres_with_multi_process() function</h3> 

- `pg_conn_details`: Instance of the PgConnectionDetail class containing PostgreSQL server connection details.
- `table_name`: Name of the table for bulk insertion.
- `data_generator`: Python generator containing DataFrames.
- `batch_size`: Number of records to insert and commit at a time.
- `min_conn_pool_size`, `max_conn_pool_size`: Determine the number of PostgreSQL connections in the connection pool.
- `drop_and_create_index`: Set to True if indexes need to be dropped during insert and re-created once insertion is complete.
- `no_of_processes`: Specify the number of cores for multiprocessing.

<h3>BatchInsert class</h3>
This class serves as the core logic for the utility and is wrapped by the first two utility functions. Users may find it useful if additional logic needs to be developed around the functionality or if a custom sequential or parallel computation logic is required.

Properties to create an instance of BatchInsert class:
- `batch_size`:Number of records to insert and commit at a time.
- `table_name`: Name of the table for bulk insertion.
- `pg_conn_details`: Instance of the PgConnectionDetail class containing PostgreSQL server connection details.
- `min_conn`, `max_conn`: Determine the number of PostgreSQL connections in the connection pool.

<h3>Developer Notes:</h3>

- The `min_conn` or `min_conn_pool_size` can be either equal to or less than the result of `ceil(total_data_size / batch_size)`.
- The `max_conn` or `max_conn_pool_size` can be either equal to or greater than the result of `ceil(total_data_size / batch_size)`.
- The `no_of_processes` can be set to the number of available cores or left as None for the system to determine the optimal number based on resource availability.
- The ideal `batch_size`, as observed during testing, typically falls within the range of 100,000 to 250,000. However, this recommendation is contingent upon the characteristics of the data and table structure.
The multiprocessing function execution must start in the __main__ block.

<h2>Package installation:</h2>
    `pip install pg-bulk-loader`

<h2>Examples:</h2>

1. Loading entire dataset once and sending for bulk insert in batches:

```python
import pandas as pd
import asyncio
from pg_bulk_loader import PgConnectionDetail, batch_insert_to_postgres


async def run():
    # Read data. Let's suppose below DataFrame has 20M records
    input_data_df = pd.DataFrame()
    
    # Create Postgres Connection Details object. This will help in creating and managing the database connections 
    pg_conn_details = PgConnectionDetail(
        user="<postgres username>",
        password="<postgres password>",
        database="<postgres database>",
        host="<host address to postgres server>",
        port="<port>",
        schema="<schema name where table exist>"
    )
    
    # Data will be inserted and committed in the batch of 2,50,000
    await batch_insert_to_postgres(
        pg_conn_details=pg_conn_details,
        table_name="<table_name>",
        data_df=input_data_df,
        batch_size=250000,
        min_conn_pool_size=20,
        max_conn_pool_size=25,
        use_multi_process_for_create_index=True,
        drop_and_create_index=True
    )


if __name__ == '__main__':
    asyncio.run(run())
```

2. Loading dataset in chunks and sending for bulk insert in batches:

```python
import pandas as pd
import asyncio
from pg_bulk_loader import PgConnectionDetail, FastLoadHack, BatchInsert


async def run():
    # Create Postgres Connection Details object. This will help in creating and managing the database connections 
    pg_conn_details = PgConnectionDetail(
        user="<postgres username>",
        password="<postgres password>",
        database="<postgres database>",
        host="<host address to postgres server>",
        port="<port>",
        schema="<schema name where table exist>"
    )
    batch_ = BatchInsert(
        batch_size=250000,
        table_name="<table_name>",
        pg_conn_details=pg_conn_details,
        min_conn=20,
        max_conn=25
    )
    
    # If index needs to be dropped before insertion
    fast_load_hack = FastLoadHack(pg_conn_details=pg_conn_details, table_name=table_name)
    indexes: dict = fast_load_hack.get_indexes()
    fast_load_hack.drop_indexes(list(indexes.keys()))
    
    try:
        # Open and create the connections in the connection pool
        await batch_.open_connection_pool()
    
        # Lets load only a chunk of 1M from the csv file of 20M
        for input_df in pd.read_csv("file-name.csv", chunksize=1000000):
            # This will partition the 1M data into 4 partitions of size 250000 each as the batch_size is 250000.
            await batch_.execute(input_df)
    finally:
        # Close the connection pool
        await batch_.close_connection_pool()
        # Re-create indexes once insertion is done
        fast_load_hack.create_indexes(list(indexes.values()), use_multi_process=True)  # Set to True if indexes need to be created parallely

        
if __name__ == '__main__':
    asyncio.run(run())

```

3. Parallel insertion using multiprocessing:

The below code uses 5 cores and processes 5M records parallely i.e. 1M on one core with 250000 records insertion at a time.

```python
import pandas as pd
import asyncio
from pg_bulk_loader import PgConnectionDetail, batch_insert_to_postgres_with_multi_process


async def run():
    # Create Postgres Connection Details object. This will help in creating and managing the database connections 
    pg_conn_details = PgConnectionDetail(
        user="<postgres username>",
        password="<postgres password>",
        database="<postgres database>",
        host="<host address to postgres server>",
        port="<port>",
        schema="<schema name where table exist>"
    )
    
    df_generator = pd.read_csv("20M-file.csv", chunksize=1000000)
    
    # Data will be inserted and committed in the batch of 2,50,000
    await batch_insert_to_postgres_with_multi_process(
        pg_conn_details=pg_conn_details,
        table_name="<table_name>",
        data_generator=df_generator,
        batch_size=250000,
        min_conn_pool_size=20,
        max_conn_pool_size=25,
        no_of_processes=5,
        drop_and_create_index=True
    )


# The multiprocessing execution must start in the __main__.
if __name__ == '__main__':
    asyncio.run(run())
```
