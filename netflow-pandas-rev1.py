#!/usr/bin/env python
# coding: utf-8

# In[12]:


# import Pandas and Numpy for data processing
import pandas as pd
import numpy as np

# import matloit for visualisazion
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


# In[13]:


# showing first 5 rows - jupyter only
df.head()


# In[14]:


# dimensions of data set
print("Dimensions:")
print(df.shape)

# data columns
print("Columns:")
print(df.columns)

# data frame info
print("Data Frame Info:")
print(df.info())


# In[15]:


#Grouping - slow
#columns_to_show = ['Размер начислений','Объем трафика в МБ']
#df.groupby(['Описание услуги']).sum()[columns_to_show]

# Pivot is faster and supports aggregation

#df.pivot_table(['nf_pkts','nf_bytes'],index=['nf_direction','nf_proto','nf_src_port','nf_dst_port','nf_dst_address','nf_src_address'],aggfunc='sum', fill_value = 0, margins=False)

#df.pivot_table(['Размер начислений','Объем трафика в МБ'],index=['Описание услуги'],aggfunc='sum', fill_value = 0, margins=True,margins_name= 'Grand Total')


# In[ ]:





# In[16]:


#v1 - too much unstructured data with many to many connection
#pivot=df.pivot_table(['nf_pkts','nf_bytes'],index=['nf_direction','nf_proto','nf_src_port','nf_dst_port','nf_dst_address','nf_src_address'],aggfunc='sum', fill_value = 0, margins=True,margins_name= 'Grand Total')
#v2 - separate pivots for nf_src to nf_dst_address  and nf_dst to nf_src_address
#pivot_src=df.pivot_table(['nf_pkts'],index=['nf_direction','nf_proto','nf_src','nf_dst_address'],aggfunc='sum', fill_value = 0, margins=False)
#v3 - with filter by direction to filter ingress
pivot_src=df[df.nf_ipv4_next_hop == "10.156.64.221"].pivot_table(['nf_pkts'],index=['nf_proto','nf_src','nf_dst_address'],aggfunc='sum', fill_value = 0, margins=False)


# In[17]:


pivot_src


# In[18]:


#v2
#pivot_dst=df.pivot_table(['nf_pkts'],index=['nf_direction','nf_proto','nf_dst','nf_src_address'],aggfunc='sum', fill_value = 0, margins=False)
#v3 - with filter for direction egress
pivot_dst=df[df.nf_ipv4_next_hop == "10.130.251.190"].pivot_table(['nf_pkts'],index=['nf_proto','nf_dst','nf_src_address'],aggfunc='sum', fill_value = 0, margins=False)


# In[19]:


pivot_dst


# In[20]:


# Not working until Unicode decoding issue can be resovled
#with pd.ExcelWriter('output.xlsx') as writer:
# pivot.to_excel(writer)

#plotting
#small_table=df.pivot_table(['nf_pkts'],index=['nf_direction','nf_proto'],aggfunc='sum', fill_value = 0)
#plt.figure(figsize=(20,10))
#small_table.plot(kind='bar',figsize=(20,10))
#pivot.plot();
#pivot_src.plot(kind='bar',figsize=(20,10))


# In[21]:



#v1
#pivot.to_csv('output_netflow.csv', sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
pivot_src.to_csv('output_netflow_src.csv', sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
pivot_dst.to_csv('output_netflow_dst.csv', sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')


# In[ ]:





# In[ ]:



