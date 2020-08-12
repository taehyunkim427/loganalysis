import pandas as pd
import numpy as np

def get_processtable(data):
    took = data[data['message'].str.contains('Took')]
    processtable = data.iloc[list(took.index - 1)]
    processtable.insert(4, 'processtime', list(took['message']))
    processtable['processtime'] = processtable['processtime'].str.replace('[^0-9.]', '')
    processtable['processtime'] = processtable['processtime'].str[1:]
    processtable['processtime'] = pd.to_numeric(processtable['processtime'])
    return processtable

def get_documenttable(data, processtable):
    idx = processtable[processtable['message'].str.contains('Done Indexing')]
    idx['document'] = list(data.iloc[list(idx.index - 1)].message)
    idx['document'] = idx['document'].str.replace('[^\d,]', '')
    idx[['insert', 'update']] = idx['document'].str.split(',', n=2, expand=True)
    idx['insert'] = idx['insert'].str.replace('^\s*$', '0')
    idx['update'].fillna(0, inplace=True)
    idx['insert'] = pd.to_numeric(idx['insert'])
    idx['update'] = pd.to_numeric(idx['update'])
    idx.drop(columns='document', inplace=True)
    return idx

def get_pivot(idx):
    idx['timestamphour'] = idx['timestamp'].dt.hour
    for i in range(24):
        if i < 10:
            idx.replace({'timestamphour': {i: f"0{i}:00 ~ 0{i}:59"}}, inplace=True)
        else:
            idx.replace({'timestamphour': {i: f"{i}:00 ~ {i}:59"}}, inplace=True)
    idx['timestampdate'] = idx['timestamp'].dt.date
    pivot = idx.pivot_table(values=['processtime', 'insert', 'update'], index=['timestampdate','timestamphour'], aggfunc=np.sum)
    pivot['indexpersecond(docs/s)'] = ((pivot['insert'] + pivot['update']) / pivot['processtime']).astype(int)
    pivot2 = idx.pivot_table(values='processtime', index=['timestampdate','timestamphour'], aggfunc=np.mean)
    pivot3 = idx.pivot_table(values='processtime', index=['timestampdate','timestamphour'], aggfunc=np.max)
    pivot4 = idx.pivot_table(values='processtime', index=['timestampdate','timestamphour'], aggfunc=np.min)
    pivot5 = idx.pivot_table(values='processtime', index=['timestampdate','timestamphour'], aggfunc='count')
    pivot['processtimemean(s)'] = pivot2['processtime'].round(2)
    pivot['processtimemax(s)'] = pivot3['processtime'].round(2)
    pivot['processtimemin(s)'] = pivot4['processtime'].round(2)
    pivot['count(cmds)'] = pivot5['processtime'].round(2)
    new_index = ['insert', 'update', 'indexpersecond(docs/s)', 'processtime', 'processtimemean(s)', 'processtimemax(s)',
                 'processtimemin(s)', 'count(cmds)']
    new_index_name = {'insert': 'insertsum(docs)', 'update': 'updatesum(docs)', 'processtime': 'processtimesum(s)'}
    pivot = pivot.reindex(new_index, axis='columns')
    pivot.rename(new_index_name, axis='columns', inplace=True)
    pivot.insert(2, 'indexsum(docs)', pivot['insertsum(docs)'] + pivot['updatesum(docs)'])
    return pivot

