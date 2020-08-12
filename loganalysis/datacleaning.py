import pandas as pd
import os

def load_dataframe(path_dir, seperate):
    file_list = os.listdir(path_dir)
    file_list.sort()
    data = pd.DataFrame()
    for file in file_list:
        now = pd.read_csv(f'{path_dir}/{file}', sep=seperate, header=None, engine='python')
        data = pd.concat([data, now])
    return data

def split_dataframe(data, seperate, colnums, columns):
    data[columns] = data[0].str.split(seperate, n=colnums, expand=True)
    return data

def assign_datetime(data,datecol,timecol,datetimeformat):
    data[datecol+timecol] = data[datecol] + data[timecol]
    data['timestamp'] = pd.to_datetime(data[datecol+timecol], format=datetimeformat)
    data.drop(columns=[0, datecol, timecol, datecol+timecol], inplace=True)
    return data

def assign_index(data):
    data.insert(0, 'index', range(len(data)))
    data.set_index('index', inplace=True)
    return data

def align_columns(data, columns):
    data = data[columns]
    return data


