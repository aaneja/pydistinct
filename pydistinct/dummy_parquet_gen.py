import pyarrow as pa
import pyarrow.parquet as pq


def generate_large_footer():
    # Define a metadata dictionary with a large number of key-value pairs
    metadata = {}
    for i in range(10000):
        metadata[f"key{i}"] = f"value{i}"
    # Create a PyArrow table with the data you want to write
    col1 = [1, 2, 3, 4, 5]
    col2 = [6, 7, 8, 9, 10]
    schema = pa.schema([("col1", pa.int64()), ("col2", pa.int64())], metadata=metadata)
    table = pa.Table.from_arrays([col1, col2], schema=schema)
    # Write the table to a parquet file with the new schema
    pq.write_table(table, "large_footer.parquet")


def generate_parquet_sample():
    col1 = [1, None, 2, 3, 4, 5]
    col2 = [6, 7, 8, 9, None, 10]
    col3 = [None, "the", "quick", "brown", "fox", None]
    schema = pa.schema([("col1", pa.int64()), ("col2", pa.int32()), ("col3", pa.string())])
    table = pa.Table.from_arrays([col1, col2, col3], schema=schema)
    # Write the table to a parquet file with the new schema
    pq.write_table(table, "small_file.parquet")


# generate_parquet()
def print_metadata(filename):
    parquet_file = pq.ParquetFile(filename)
    metadata = parquet_file.metadata
    print(metadata)


# Create a ParquetFile object with the path to the parquet file
# print_metadata('large_footer.parquet')
#generate_parquet_sample()
print_metadata("/tmp/tpcds_store_sales_sf1.parquet")
