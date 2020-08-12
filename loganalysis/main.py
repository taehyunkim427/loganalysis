#-*- coding: utf-8 -*-
from warnings import filterwarnings
import datacleaning as dc
import dataanalysis as da
import datavisualize as dv
import pandas as pd

if __name__ == '__main__':
    # data cleaning
    filterwarnings(action='ignore')
    data = dc.load_dataframe('./logs', '\r\s')
    dc.split_dataframe(data,' ', 4, ['date', 'time', 'loglevel', 'serviceID', 'message'])
    dc.assign_datetime(data,'date','time',"%d/%m/%Y%H:%M:%S")
    dc.assign_index(data)
    dc.align_columns(data, ['timestamp', 'serviceID', 'loglevel', 'message'])

    # data analysis
    processtable = da.get_processtable(data)
    idx = da.get_documenttable(data, processtable)
    pivot = da.get_pivot(idx)

    # data visualization
    dv.rename_columns(pivot)
    writer = pd.ExcelWriter("loganalysis.xlsx", engine='xlsxwriter')
    writer = dv.get_xlsxframe(pivot, writer)
    writer.save()
