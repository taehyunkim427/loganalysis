#-*- coding: utf-8 -*-
import pandas as pd

def rename_columns(data):
    columns = {"insertsum(docs)": "색인 추가(회)", "updatesum(docs)": "색인 수정(회)", "indexsum(docs)": "총색인(회)",
               "indexpersecond(docs/s)": "초당 색인 횟수(회/초)", "processtimesum(s)": "색인 시간(초)",
               "processtimemean(s)": "평균 색인 시간(초)", "processtimemax(s)": "최고 색인 시간(초)",
               "processtimemin(s)": "최저 색인 시간(초)", "count(cmds)": "색인 횟수(회)"}
    data.rename(columns=columns, inplace=True)
    return data

def get_xlsxframe(pivot, writer):
    pivot.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.set_column('A:A', 12)
    worksheet.set_column('B:B', 15)
    worksheet.set_column('C:C', 10)
    worksheet.set_column('D:D', 10)
    worksheet.set_column('E:E', 10)
    worksheet.set_column('F:F', 10)
    worksheet.set_column('G:G', 10)
    worksheet.set_column('H:H', 10)
    worksheet.set_column('I:I', 10)
    worksheet.set_column('K:K', 10)
    worksheet.conditional_format('C2:C9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('D2:D9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('E2:E9999', {'type': 'data_bar', 'bar_color': '#81D4FA'})
    worksheet.conditional_format('F2:F9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('G2:G9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('H2:H9999', {'type': 'data_bar', 'bar_color': '#81D4FA'})
    worksheet.conditional_format('I2:I9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('J2:J9999', {'type': 'data_bar', 'bar_color': '#B3E5FC'})
    worksheet.conditional_format('K2:K9999', {'type': 'data_bar', 'bar_color': '#81D4FA'})


    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': True,
        'valign': 'top',
        'fg_color': '#E8F5FF',
        'border': 1})

    # Write the column headers with the defined format.
    for col_num, value in enumerate(pivot.columns.values):
        worksheet.write(0, col_num + 2, value, header_format)

    worksheet.write(0, 0, "날짜",header_format)
    worksheet.write(0, 1, "시간",header_format)

    return writer

