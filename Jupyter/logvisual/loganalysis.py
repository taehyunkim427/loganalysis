#!/usr/bin/env python
# coding: utf-8

# ## import modules

# In[1]:


import os
import pandas as pd
import re
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import warnings


# ---

# ## load data to pandas dataframe

# In[2]:


path_dir = 'c:/logs'
file_list = os.listdir(path_dir)
file_list.sort()
data = pd.DataFrame()
for file in file_list:
    now = pd.read_csv(f'./{file}', sep='\r\s', header=None, engine='python')
    data = pd.concat([data,now])
data[['date', 'time','loglevel' ,'serviceID', 'message']] = data[0].str.split(' ', n=4, expand=True)
warnings.filterwarnings(action='ignore')
data.describe()


# ---

# ## assign index and datetime

# In[3]:


data['datetime'] = data['date'] + data['time']
data['timestamp'] = pd.to_datetime(data['datetime'], format="%d/%m/%Y%H:%M:%S")
data.drop(columns=[0,'date','time','datetime'], inplace=True)
data.insert(0,'index', range(len(data)))
data.set_index('index',inplace=True)
data = data[['timestamp', 'serviceID', 'loglevel', 'message']]
data


# ---

# ## extract command messages and processing time

# In[4]:


took = data[data['message'].str.contains('Took')]
process = data.iloc[list(took.index-1)]
process.insert(4,'processtime', list(took['message']))
process['processtime'] = process['processtime'].str.replace('[^0-9.]','')
process['processtime'] = process['processtime'].str[1:]
process['processtime'] = pd.to_numeric(process['processtime'])
process.head()


# ---

# ## extract inserts and updates

# In[5]:


idx = process[process['message'].str.contains('Done Indexing')]
idx['document'] = list(data.iloc[list(idx.index-1)].message)
idx['document'] = idx['document'].str.replace('[^\d,]','')
idx[['insert', 'update']] = idx['document'].str.split(',', n=2, expand=True)
idx['insert'] = idx['insert'].str.replace('^\s*$', '0')
idx['update'].fillna(0,inplace = True)
idx['insert'] = pd.to_numeric(idx['insert'])
idx['update'] = pd.to_numeric(idx['update'])
idx.drop(columns='document',inplace=True)
idx.head()


# ---

# ## group by hour

# In[6]:


idx['timestamphour'] = idx['timestamp'].dt.hour
pivot = idx.pivot_table(values=['processtime', 'insert', 'update'], index='timestamphour' ,aggfunc=np.sum)
pivot['indexpersecond(docs/s)'] = ((pivot['insert'] + pivot['update']) / pivot['processtime']).astype(int)
pivot2 = idx.pivot_table(values='processtime', index='timestamphour', aggfunc=np.mean)
pivot3 = idx.pivot_table(values='processtime', index='timestamphour', aggfunc=np.max)
pivot4 = idx.pivot_table(values='processtime', index='timestamphour', aggfunc=np.min)
pivot5 = idx.pivot_table(values='processtime', index='timestamphour', aggfunc='count')
pivot['processtimemean(s)'] = pivot2['processtime'].round(2)
pivot['processtimemax(s)'] = pivot3['processtime'].round(2)
pivot['processtimemin(s)'] = pivot4['processtime'].round(2)
pivot['count(cmds)'] = pivot5['processtime'].round(2)
new_index = ['insert','update','indexpersecond(docs/s)','processtime','processtimemean(s)','processtimemax(s)','processtimemin(s)','count(cmds)']
new_index_name = {'insert':'insertsum(docs)','update':'updatesum(docs)','processtime':'processtimesum(s)'}
pivot = pivot.reindex(new_index, axis='columns')
pivot.rename(new_index_name,axis='columns',inplace=True)
pivot


# ---

# ## data visualization

# In[7]:


plt.figure(figsize=[18,4])

sns.pointplot(x=pivot.index, y='indexpersecond(docs/s)',data=pivot)


# In[8]:


fig, ax3 = plt.subplots(nrows=1, ncols=1)
fig.set_size_inches(18, 4)

sns.barplot(data=pivot, x=pivot.index, y="insertsum(docs)", ax=ax3)


# In[9]:


fig, ax4 = plt.subplots(nrows=1, ncols=1)
fig.set_size_inches(18, 4)

sns.barplot(data=pivot, x=pivot.index, y="updatesum(docs)", ax=ax4)


# In[10]:


pivot.to_excel("output.xlsx")

