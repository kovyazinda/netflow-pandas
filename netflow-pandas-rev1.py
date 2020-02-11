#!/usr/bin/env python
# coding: utf-8

# import Pandas and Numpy for data processing
import pandas as pd
import numpy as np

# import matloit for visualisazion
# import matplotlib.pyplot as plt

# import common modules
import sys
import re
import csv
import datetime

# source file name  
inputfile = 'netflow.csv'
        
# reading csv file with pandas, ignoring bad lines tokenization errors
df = pd.read_csv(inputfile,sep = ',',decimal=".",error_bad_lines=False,low_memory=False)


# showing first 5 rows - jupyter only
df.head()


# dimensions of data set
print("Dimensions:")
print(df.shape)

# data columns
print("Columns:")
print(df.columns)

# data frame info
print("Data Frame Info:")
print(df.info())


# Pivot is faster and supports aggregation
pivot=df.pivot_table(['nf_pkts','nf_bytes'],index=['nf_direction','nf_proto','nf_src_port','nf_dst_port','nf_dst_address','nf_src_address'],aggfunc='sum', fill_value = 0, margins=True,margins_name= 'Grand Total')




pivot.to_csv('output_netflow.csv', sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
