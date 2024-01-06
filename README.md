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
- `no_of_processes`: Specify the number of cores for multiprocessing; set to None for auto-assignment.

<h3>BatchInsert class</h3>
This class serves as the core logic for the utility and is wrapped by the first two utility functions. Users may find it useful if additional logic needs to be developed around the functionality or if a custom sequential or parallel computation logic is required.

Properties to create an instance of BatchInsert class:
- `batch_size`:Number of records to insert and commit at a time.
- `table_name`: Name of the table for bulk insertion.
- `pg_conn_details`: Instance of the PgConnectionDetail class containing PostgreSQL server connection details.
- `min_conn`, `max_conn`: Determine the number of PostgreSQL connections in the connection pool.

<h2>Examples:</h2>

1. Sequential insertion with a specified batch size:

```python
import pandas as pd
from src.batch.batch_insert_wrapper import batch_insert_to_postgres
from src.batch.pg_connection_detail import PgConnectionDetail

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
```

2. Sequential insertion without loading the entire dataset into memory:
```python
import pandas as pd
from src.batch.batch_insert import BatchInsert
from src.batch.pg_connection_detail import PgConnectionDetail
from src.batch.fast_load_hack import FastLoadHack

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
    fast_load_hack.create_indexes(list(indexes.values()), use_multi_process_for_create_index=True/False) # Use this based on either sequential or parallel building of index
```

3. Parallel insertion using multiprocessing:

The below code uses 5 cores and processes 5M records parallely i.e. 1M on one core with 250000 records insertion at a time.
 
```python
import pandas as pd
from src.batch.batch_insert_wrapper import batch_insert_to_postgres_with_multi_process
from src.batch.pg_connection_detail import PgConnectionDetail

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
```
