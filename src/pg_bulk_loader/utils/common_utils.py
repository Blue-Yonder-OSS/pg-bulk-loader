import pandas as pd


def is_empty(df: pd.DataFrame):
    if df is None:
        return True

    if not isinstance(df, pd.DataFrame):
        raise Exception("Invalid parameter! Data type should be pandas DataFrame")
    return df.empty


def partition_df(df: pd.DataFrame, partition_size: int):
    if not is_empty(df):
        if not partition_size:
            raise Exception("Invalid partition size.")

        df_size = df.shape[0]
        if partition_size > df_size:
            partition_size = df_size

        return [df[i:i + partition_size] for i in range(0, df_size, partition_size)]


def get_ranges(data_size: int, batch_size: int):
    ranges = []
    if isinstance(data_size, int) and isinstance(batch_size, int) and data_size > 0 and batch_size > 0:
        start = 0
        end = min(data_size, batch_size)
        while start < data_size:
            ranges.append((start, end))
            start = end
            end = min(data_size, batch_size+end)
    return ranges
