# -*- coding: utf-8 -*-
"""
Created on Mon Jan 27 18:46:55 2014

@author: Zelazny7
"""

import pandas as pd

# Grab the perf from the training dset:

LOAN = pd.HDFStore("../data/LOAN.h5", mode='r')

loss = LOAN.select('TRAIN', columns=['loss'])


# column that chunks an array into set intervals
def chunk_columns(array, size):
    for i in range(0, len(array), size):
        yield array[i:i+size]
    
# Variables that show promise:
# f13
# f8
# dflt.groupby(f13['f13']).agg([pd.Series.count, np.sum, np.mean])

# TODO: Code up a proc means function that reports the following:
# N unique, N missing, Min, Max, Mean, Median, Std
def proc_means(s):
    idx = ['N Uniq', 'N Miss', 'N Popd', 'Min', 'Max', 'Mean', 'Median', 'Std']
    return pd.Series({
        "N Uniq": s.nunique(),
        "N Miss": s.isnull().sum(),
        "N Popd": s.count(),
        "Min"   : s.min(),
        "Max"   : s.max(),
        "Mean"  : s.mean(),
        "Median": s.median(),
        "Std"   : s.std()}, index=idx)


# now iterate over the store and run proc means
results = []

batch_size = 20
dset_cols = LOAN.get_node('TRAIN').table.attrs.values_block_0_kind

for cols in chunk_columns(dset_cols, batch_size):
    df = LOAN.select('TRAIN', columns=cols)
    results.append(df.apply(proc_means))

res_df = pd.concat(results)