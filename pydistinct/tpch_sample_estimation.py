from pydistinct.ensemble_estimators import *
from pydistinct.tpch_sample_generate import *


def read_file_ints(file: str):
    with open(file, 'r') as f:
        return [int(x) for x in f.read().split()]


def run_estimators(sample_name, cardinality):
    sample = [int(x) for x in read_parquet_col0(sample_name)]  # values may not be ints
    print(f'\n## For sample {sample_name}')
    print(f'Length of sample : {"{:,.0f}".format(len(sample))}')

    # print(f'Smoothed Jacknife  {"{:,.0f}".format(smoothed_jackknife_estimator(sequence=sample, n_pop=cardinality))}')
    #print(f'shlossers   {"{:,.0f}".format(shlossers_estimator(sequence=sample, n_pop=cardinality))}')
    # print(f'horvitz   {"{:,.0f}".format(horvitz_thompson_estimator(sequence=sample, n_pop=cardinality))}')
    print(f'chao_estimator   {"{:,.0f}".format(chao_estimator(sequence=sample))}')
    print(f'chao_lee_estimator   {"{:,.0f}".format(chao_lee_estimator(sequence=sample))}')
    print(f'jackknife_estimator   {"{:,.0f}".format(jackknife_estimator(sequence=sample))}')
    print(f'bootstrap_estimator   {"{:,.0f}".format(bootstrap_estimator(sequence=sample))}')


def orderkey_lineitem():
    print(f'Expected NDV is {"{:,.0f}".format(1.54258345E8)}')
    run_estimators('orderkey_lineitem_5_system_sample.parquet', 600037902)
    run_estimators('orderkey_lineitem_1_bernoulli_sample.parquet', 600037902)
    run_estimators('orderkey_lineitem_5_bernoulli_sample.parquet', 600037902)
    run_estimators('orderkey_lineitem_10_bernoulli_sample.parquet', 600037902)


def quantity_lineitem():
    print(f'Expected NDV for quantity is {50}')
    run_estimators('quantity_lineitem_system_sample.parquet', 600037902)


def suppkey_lineitem():
    print(f'Expected NDV is {"{:,.0f}".format(981802)}')
    run_estimators('suppkey_lineitem_sample.parquet', 600037902)


if __name__ == '__main__':
    suppkey_lineitem()
