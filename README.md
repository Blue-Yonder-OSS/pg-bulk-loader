# pg-bulk-loader

## Description
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

**pg-bulk-loader** is a utility package designed to facilitate faster bulk insertion DataFrame to a PostgreSQL Database.
Currently, it supports load from pandas DataFrame only.

This utility leverages the power of PostgreSQL in combination with Python to efficiently handle the bulk insertion of large datasets. The key features that contribute to its speed include:

1. Utilization of Postgres' copy command
2. Implementation of Python's coroutines
3. Harnessing the power of multiprocessing
4. Capability to drop indexes during insertion and recreate them in parallel
5. Connection pooling

📢 **Blog article:** [Quick load from Pandas to Postgres](https://medium.com/@adityajaroli/quick-load-from-pandas-to-postgres-80c0187c1bdf)

![Screenshot](https://github.com/Blue-Yonder-OSS/pg-bulk-loader/tree/master/public/pandastopostgres.png)

## Performance in Numbers

**Machine:**
- Resource config - 5 core, 8GB
- Azure hosted PostgreSQL Server
- Azure hosted Python service (jupyter notebook)

**Table info:**
- 12 columns (3 texts, 2 date, 7 double)
- Primary key: 3 columns (2 text and 1 date)
- Indexes: 2 b-tree. (1 on single column and another on three columns)

**Runtime on 20M Dataset:**
- The table has no PK and no index:
  - When table is UNLOGGED - ~55 seconds.
  - When table is LOGGED - ~68 seconds.

- The table has PK with 3 columns and 2 b-tree indexes (maintenance work memory at 512 MB):
  - When table is UNLOGGED - ~150 seconds (~66s are taken for drop-and-create index operation).
  - When table is LOGGED - ~180 seconds (~68s are taken for drop-and-create index operation).

**Runtime on 1M Dataset without having PK and Indexes with different approaches:**

![Screenshot](https://github.com/Blue-Yonder-OSS/pg-bulk-loader/tree/master/public/pg-bulk-loader.png)

## Usage

The utility provides the following useful functions and classes:

1. **batch_insert_to_postgres**
2. **batch_insert_to_postgres_with_multi_process**
3. **BatchInsert**


<h3>batch_insert_to_postgres() function</h3>

- `pg_conn_details`: Instance of the PgConnectionDetail class containing PostgreSQL server connection details.
- `table_name`: Name of the table for bulk insertion.
- `input_data`: Data in the form of a pandas DataFrame or Python generator containing DataFrames.
- `batch_size`: Number of records to insert and commit at a time.
- `min_conn_pool_size`, `max_conn_pool_size`: Determine the number of PostgreSQL connections in the connection pool.
- `drop_and_create_index`: Set to True if indexes need to be dropped during insert and re-created once insertion is complete.
- `use_multi_process_for_create_index`: Set to True if indexes need to be re-created in parallel; otherwise, they will be created sequentially.

**Note:** Provide input either in the form of DataFrame or DataFrame generator

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

## Prerequisites

Before cloning/forking this project, make sure you have the following tools installed:

- Git
- Python3.9++
- pg-bulk-loader package (`pip install pg-bulk-loader`)


## Examples

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
        input_data=input_data_df,
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
from pg_bulk_loader import PgConnectionDetail, batch_insert_to_postgres


async def run():
    # Read data. Let's suppose below DataFrame has 20M records
    input_data_df_generator = pd.read_csv("file.csv", chunksize=1000000)

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
        data_df=None,
        input_data=input_data_df_generator,
        batch_size=250000,
        min_conn_pool_size=20,
        max_conn_pool_size=25,
        use_multi_process_for_create_index=True,
        drop_and_create_index=True
    )


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

## Installation

1. Fork the project
2. Clone the project
3. Follow the instructions in the console:
4. Navigate to the project directory cd pg-bulk-loader
5. Install the dependencies `pip install -r dev-requirements.txt`
6. Run `pre-commit install` to creates a hook with `git commit` command. This will ensure basic code formatting before you make any commit.
7. Run test cases to verify installation. Run command `pytest`.

## Contributors

[//]: contributor-faces

<a href="https://github.com/adityajaroli"><img class="avatar rounded-2 avatar-user" src="https://avatars.githubusercontent.com/u/15523808?s=400&u=b1fabb9bbce05976f4c18ea83f9ab1b60890cec0&v=4" title="Aditya Jaroli" width="80" height="80"></a>

[//]: contributor-faces
