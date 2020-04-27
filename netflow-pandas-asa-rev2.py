#!/usr/bin/env python
# coding: utf-8

# In[25]:


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
inputfile = 'netflow-full.csv'
        
# reading csv file with pandas, ignoring bad lines tokenization errors
dfraw = pd.read_csv(inputfile,sep = ',',decimal=".",error_bad_lines=False,low_memory=False)


# In[147]:


#Read ACL hashes for ASA config extract show access list | inc 0x - comment if you don't have ACL exctract
aclfilename = 'acl-id.txt'
aclfile = open(aclfilename, "r")
acldict = {}
for aclline in aclfile:
    aclparsed=re.sub('\;\ .*0x',';',aclline.rstrip())
    aclparsed=re.sub('\ \(.*0x',';',aclparsed)
    # print(aclparsed)
    acllist = aclparsed.split(';')
    
#    print(acllist[0])
#    print(acllist[1])

    if acllist[0]:
        try: 
            acldict[acllist[1]]=acllist[0]
        except:
            pass

#for aclelement in acldict:
# print(aclelement)

#skipping problem statements
#dfacl=pd.DataFrame.from_dict(acldict).dropna()
#dfacl=pd.DataFrame.from_dict(acldict,orient='index').dropna()

dfacl=pd.DataFrame(acldict.items(), columns=['acl_id', 'acl_statement'])

#dfacl.columns=['acl_id','acl_statement']
dfacl.head(20)


# In[82]:


# showing first 5 rows - jupyter only
df=dfraw.fillna(0)
df.head(20)


# In[84]:


dfnfacl=df['netflow_ingress_acl_id'].str.split("-", n = 3, expand = True).fillna(0)
dfnfacl.head(20)


# In[87]:


df["netflow_ingress_acl_id_l1"]= dfnfacl[0]
df["netflow_ingress_acl_id_l2"]= dfnfacl[1]
df["netflow_ingress_acl_id_l3"]= dfnfacl[2]

df.head(20)


# In[5]:


# dimensions of data set
print("Dimensions:")
print(df.shape)

# data columns
print("Columns:")
print(df.columns)

# data frame info
print("Data Frame Info:")
print(df.info())


# In[164]:


# mapping ACL to statement
#df['netflow_ingress_acl_id_l1_statement'] = dfacl.lookup(df.index, df['acl_statement'])
df['netflow_ingress_acl_id_l1_statement'] = df['netflow_ingress_acl_id_l1'].map(dfacl.set_index('acl_id')['acl_statement']).fillna("Not mapped")
df['netflow_ingress_acl_id_l2_statement'] = df['netflow_ingress_acl_id_l2'].map(dfacl.set_index('acl_id')['acl_statement']).fillna("Not mapped")
df['netflow_ingress_acl_id_l3_statement'] = df['netflow_ingress_acl_id_l3'].map(dfacl.set_index('acl_id')['acl_statement']).fillna("Not mapped")
df.head(20)


# In[165]:


# Forming pivots

pivot_icmp=df[df.netflow_protocol_name == 'icmp'].pivot_table(['netflow_fwd_flow_delta_bytes'],index=['netflow_ingress_acl_id','netflow_ingress_acl_id_l1_statement','netflow_ingress_acl_id_l2_statement','netflow_ingress_acl_id_l3_statement','netflow_protocol_name','netflow_xlate_src_addr_ipv4','netflow_xlate_dst_addr_ipv4'],aggfunc='sum', fill_value = 0, margins=False)
pivot_src=df[df.netflow_protocol_name != 'icmp'].pivot_table(['netflow_fwd_flow_delta_bytes'],index=['netflow_ingress_acl_id','netflow_ingress_acl_id_l1_statement','netflow_ingress_acl_id_l2_statement','netflow_ingress_acl_id_l3_statement','netflow_protocol_name','netflow_xlate_src_addr_ipv4','netflow_xlate_dst_addr_ipv4','netflow_xlate_dst_port'],aggfunc='sum', fill_value = 0, margins=False)
pivot_dst=df[(df.netflow_protocol_name != 'icmp') & (df.netflow_xlate_src_port <= 5000)].pivot_table(['netflow_fwd_flow_delta_bytes'],index=['netflow_ingress_acl_id','netflow_ingress_acl_id_l1_statement','netflow_ingress_acl_id_l2_statement','netflow_ingress_acl_id_l3_statement','netflow_protocol_name','netflow_xlate_dst_addr_ipv4','netflow_xlate_src_addr_ipv4','netflow_xlate_src_port'],aggfunc='sum', fill_value = 0, margins=False)


# In[166]:


#Export to CSV
icmp_pivot_filename='output_netflow_icmp.csv'
source_pivot_filename='output_netflow_ingress.csv'
destination_pivot_filename='output_netflow_egress-wellknown.csv'
acl_pivot_filename='output-netflow-acl.csv'


print("writing:",icmp_pivot_filename)
pivot_icmp.to_csv(icmp_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
print("writing:",source_pivot_filename)
pivot_src.to_csv(source_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
print("writing:",destination_pivot_filename)
pivot_dst.to_csv(destination_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')
print("writing:",acl_pivot_filename)
dfacl.to_csv(acl_pivot_filename, sep=';', na_rep='', header=True, index=True,mode='w', encoding='utf8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal=',')


# In[167]:


#export to Excel
excelfile = "netflow.xlsx"

with pd.ExcelWriter(excelfile) as writer:
 dfacl.to_excel(writer,sheet_name='acl')
 pivot_icmp.to_excel(writer,sheet_name='icmp')
 pivot_src.to_excel(writer,sheet_name='tcpudp-ingress-dst-port')
 pivot_dst.to_excel(writer,sheet_name='tcpudp-egress-src-wellknown')


# In[ ]:




