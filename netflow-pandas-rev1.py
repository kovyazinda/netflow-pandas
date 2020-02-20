#!/usr/bin/env python
# coding: utf-8


# import Pandas and Numpy for data processing
import pandas as pd
import numpy as np

# import matзlotlib for visualisazion
import matplotlib.pyplot as plt

# import common modules
import sys
import re
import csv
import datetime

# reading source file name form 1st argument 
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

#df.pivot_table(['nf_pkts','nf_bytes'],index=['nf_direction','nf_proto','nf_src_port','nf_dst_port','nf_dst_address','nf_src_address'],aggfunc='sum', fill_value = 0, margins=False)

#display gateways
#pivot_gw=df.pivot_table(['nf_pkts'],index=['nf_ipv4_next_hop'],aggfunc='sum', fill_value = 0, margins=False)
#pivot_gw

#v4 - getting list of gateways and forming pivots by src and dst for each
print("Found Gateways:")
for gateway in df.nf_ipv4_next_hop.unique():
    print(gateway)
# generate pivots for each gateway src and dst
    source_pivot_filename='netflow_src_'+gateway+'.csv'
    destination_pivot_filename='netflow_dst_'+gateway+'.csv'
    pivot_src=df[df.nf_ipv4_next_hop == gateway].pivot_table(['nf_pkts'],index=['nf_proto','nf_src','nf_dst_address'],aggfunc='sum', fill_value = 0, margins=False)
    pivot_dst=df[df.nf_ipv4_next_hop == gateway].pivot_table(['nf_pkts'],index=['nf_proto','nf_dst','nf_src_address'],aggfunc='sum', fill_value = 0, margins=False)
    print("writing:",source_pivot_filename)
    pivot_src.to_csv(source_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
#    pivot_src.to_csv(source_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
    print("writing:",destination_pivot_filename)
    pivot_dst.to_csv(destination_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
#    pivot_dst.to_csv(destination_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')


