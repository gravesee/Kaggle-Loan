# -*- coding: utf-8 -*-
"""
Spyder Editor

This temporary script file is located here:
C:\Users\Zelazny7\.spyder2\.temp.py
"""

import pandas as pd

# Ok, we have a huge csv and want to know how to import it and store it as an HDF
# let's built up a dictionary of inferred types



# first pass to determine file formats using pd.read_csv inference
fmts = []
chunker = pd.read_csv('../data/train.csv', chunksize=10000)

for chunk in chunker:
    fmts.append(chunk.dtypes)

# reduce the dtypes into one
fmts = reduce(lambda x,y: x.combine(y, max), fmts)

# second pass now to find which are objects and loop again to get the max lengths
objs = fmts[fmts == 'object'].index
cnvt = {obj : str for obj in objs}
lens = []


chunker = pd.read_csv('../data/train.csv', chunksize=10000,
                      converters=cnvt, usecols=objs)
for chunk in chunker:
    for col in chunk:
        lens.append(chunk.apply(lambda x: max(x.apply(len))))

# reduce the lens into one
lens = dict(reduce(lambda x,y: x.combine(y, max), lens))


# Lastly loop through once more to append to an HDFStore table!
store = pd.HDFStore("../data/train.h5")

chunker = pd.read_csv('../data/train.csv', chunksize=10000, dtype=dict(fmts))
for chunk in chunker:
    store.append('train', chunk, min_itemsize=lens)

# first read it in as all string to do physically look at the data
store = pd.HDFStore("../data/train3.h5")

chunker = pd.read_csv('../data/train.csv', chunksize=50000, dtype='string')
for chunk in chunker:
    store.put('train', chunk, min_itemsize=200)