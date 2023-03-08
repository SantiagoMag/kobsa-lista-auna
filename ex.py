import pandas as pd
import pyodbc 
import connectorx as cx
import time
from datetime import date, datetime
import os
# mssql
# MSSQL_HOST=192.168.30.51
# MSSQL_PORT=1433
# MSSQL_USER=s_magallanes
# MSSQL_PASSWORD=smagall@n3$
# MSSQL_DB=SCORE_TEMPRANA
# MSSQL_URL=mssql://username:password@hostname:1433/db



# SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_LISTA_AUNA;
db_url="mssql://s_magallanes:smagall@n3$@192.168.30.51:1433/SCORE_TEMPRANA"
sql_querys_sheet = {"IVR":"SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_IVR_AUNA",
                    "LISTA":"SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_LISTA_AUNA"}


def generate_excel(db_url, sql_querys_sheet ):

    now = datetime.now()
    today = date.today().strftime("%Y%m%d")
    name = today + r'_LISTA_AUNA'
    folder = r'C:\\Users\\lmagallanes\\OneDrive\\LISTA_AUNA\\'
    final_name = folder + name + '.xlsx'

    files = os.listdir(r'C:\\Users\\lmagallanes\\OneDrive\\LISTA_AUNA\\')
    if len(files) != 1:
        final_name = folder + name + '_' + str(len(files)) + '.xlsx'

    writer = pd.ExcelWriter(final_name, engine='xlsxwriter')


    for sheet,sql_query  in sql_querys_sheet.items():

        df  = cx.read_sql(db_url,sql_query)

        # Write the dataframe data to XlsxWriter. Turn off the default header and
        # index and skip one row to allow us to insert a user defined header.
        df.to_excel(writer, sheet_name=sheet, startrow=1, header=False, index=False)

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets[sheet]

        # Get the dimensions of the dataframe.
        (max_row, max_col) = df.shape

        # Create a list of column headers, to use in add_table().
        column_settings = [{'header': column} for column in df.columns]

        # Add the Excel table structure. Pandas will add the data.
        worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})
        # Make the columns wider for clarity.
        worksheet.set_column(0, max_col - 1, 12)

    writer.close()


    time_diff = now - datetime.now()
    print('Execution: {:.1f}'.format(time_diff.total_seconds()))


generate_excel(db_url, sql_querys_sheet)
