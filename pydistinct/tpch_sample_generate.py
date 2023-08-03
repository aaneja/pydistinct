import pandas as pd
import prestodb

params = {'host': 'localhost', 'port': '8085', 'user': 'admin',
          'catalog': 'tpch', 'schema': 'sf100'}


def run_query(query_text: str):
    conn = prestodb.dbapi.connect(**params)
    cur = conn.cursor()
    cur.execute(query_text)
    return pd.DataFrame.from_records(cur.fetchall(), columns=[i[0] for i in cur.description])


def write_parquet(col_name, query):
    result_df = run_query(query)
    result_df.to_parquet(f'{col_name}_sample.parquet', engine='pyarrow', compression='gzip')


def read_parquet_col0(file):
    df = pd.read_parquet(file, engine='pyarrow')
    return df.iloc[:, 0].to_list()


if __name__ == '__main__':
    write_parquet('orderkey_lineitem_40_bernoulli', 'SELECT orderkey FROM lineitem TABLESAMPLE BERNOULLI (40)')
