import unittest
import pytest
import pandas as pd
from src.pg_bulk_loader.utils.dataframe_utils import partition_df, get_ranges


class TestDataFrameUtils(unittest.TestCase):

    def test_partition_df_with_invalid_partition_size(self):
        with pytest.raises(Exception) as e:
            input_df = pd.DataFrame({
                'test': [1, 2, 3]
            })
            partition_df(df=input_df, partition_size=0)
        assert str(e.value) == "Invalid partition size."

    def test_partition_df_with_invalid_input_df(self):
        with pytest.raises(Exception) as e:
            input_df = "invalid"
            partition_df(df=input_df, partition_size=0)
        assert str(e.value) == "Invalid parameter! Data type should be pandas DataFrame"

    def test_partition_df_when_input_df_is_none(self):
        input_df = None
        result = partition_df(df=input_df, partition_size=0)
        assert result is None

    def test_partition_df_when_input_df_is_empty(self):
        input_df = pd.DataFrame()
        result = partition_df(df=input_df, partition_size=0)
        assert result is None

    def test_partition_df_when_partition_size_is_more_than_df_length(self):
        input_df = pd.DataFrame({
            'test': [1, 2, 3]
        })
        result = partition_df(df=input_df, partition_size=5)
        assert len(result) == 1
        assert result[0].shape == (3, 1)

    def test_partition_df_when_partition_size_is_equal_to_df_length(self):
        input_df = pd.DataFrame({
            'test': [1, 2, 3]
        })
        result = partition_df(df=input_df, partition_size=3)
        assert len(result) == 1
        assert result[0].shape == (3, 1)

    def test_partition_df_when_partition_size_is_less_than_df_length(self):
        input_df = pd.DataFrame({
            'test': [1, 2, 3]
        })
        result = partition_df(df=input_df, partition_size=2)
        assert len(result) == 2
        assert result[0].shape == (2, 1)
        assert result[1].shape == (1, 1)

    def test_get_ranges_when_data_size_is_zero(self):
        data_size = 0
        batch_size = 100
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_data_size_is_null(self):
        data_size = None
        batch_size = 100
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_data_size_is_negative(self):
        data_size = -1
        batch_size = 100
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_data_size_is_string_value(self):
        data_size = "hello"
        batch_size = 100
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_batch_size_is_zero(self):
        data_size = 100
        batch_size = 0
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_batch_size_is_null(self):
        data_size = 100
        batch_size = None
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_batch_size_is_negative(self):
        data_size = 100
        batch_size = -1
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_batch_size_is_string_value(self):
        data_size = 100
        batch_size = "hell0"
        ranges = get_ranges(data_size, batch_size)
        assert ranges == []

    def test_get_ranges_when_batch_size_is_greater_than_data_size(self):
        data_size = 100
        batch_size = 150
        ranges = get_ranges(data_size, batch_size)
        assert ranges == [(0, 100)]

    def test_get_ranges_when_batch_size_is_equal_to_data_size(self):
        data_size = 99
        batch_size = 99
        ranges = get_ranges(data_size, batch_size)
        assert ranges == [(0, 99)]

    def test_get_ranges_when_batch_size_is_less_than_data_size(self):
        data_size = 101
        batch_size = 50
        ranges = get_ranges(data_size, batch_size)
        assert ranges == [(0, 50), (50, 100), (100, 101)]

    def test_get_ranges(self):
        data_size = 59
        batch_size = 9
        ranges = get_ranges(data_size, batch_size)
        assert ranges == [(0, 9), (9, 18), (18, 27), (27, 36), (36, 45), (45, 54), (54, 59)]
