# -*- coding: utf-8 -*-
"""
Spyder Editor

This temporary script file is located here:
C:\Users\Zelazny7\.spyder2\.temp.py
"""

import pandas as pd

LOAN = pd.HDFStore("../data/LOAN.h5", mode='a')

chunker = pd.read_csv('../data/train.csv', chunksize=50000, dtype='float64')
for chunk in chunker:
    LOAN.append('TRAIN', chunk, min_itemsize=50)