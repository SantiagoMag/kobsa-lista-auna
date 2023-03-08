from dagster import asset
import pandas as pd
import pyodbc 
import connectorx as cx
import time
from datetime import date, datetime
import os
# mssql
import pyodbc

@asset
def write_auna_csv() -> bool:

    return True


@asset
def run_sql_bulk(write_auna_csv: bool)  -> bool:

    
    sql1 = """
        USE [SCORE_TEMPRANA];
        exec [dbo].[sp_bulk_score_temprana];
    """
    conn_str = (
        r'Driver=SQL Server;'
        r'Server=192.168.30.51;'
        r'Database=SCORE_TEMPRANA;'
        r'UID=s_magallanes;'
        r'PWD=smagall@n3$;'
        )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    cursor.execute(sql1)
    cnxn.close()
   
    return True

@asset
def run_sql_contact()  -> bool:

    sql2 = """
        USE [AUNA];
        exec [dbo].[sp_contac_w_rg] ;
    """
    conn_str = (
        r'Driver=SQL Server;'
        r'Server=192.168.30.51;'
        r'Database=AUNA;'
        r'UID=s_magallanes;'
        r'PWD=smagall@n3$;'
        )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    cursor.execute(sql2)
    cnxn.close()
   
    return True


@asset
def run_sql_cuentas()  -> bool:


    sql3 = """
        USE [AUNA];
        exec [dbo].[sp_cuentas];
    """
    conn_str = (
        r'Driver=SQL Server;'
        r'Server=192.168.30.51;'
        r'Database=AUNA;'
        r'UID=s_magallanes;'
        r'PWD=smagall@n3$;'
        )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    cursor.execute(sql3)
    cnxn.close()
    return True


@asset
def run_sql_lt(run_sql_bulk:bool, run_sql_contact:bool, run_sql_cuentas:bool  )  -> bool:

    sql4 = """
        USE [SCORE_TEMPRANA];
        exec [dbo].[sp_contact_cuentas_auna_lt];
        exec [dbo].[ivr_auna];
        exec [dbo].[lista_auna];
    """
    conn_str = (
        r'Driver=SQL Server;'
        r'Server=192.168.30.51;'
        r'Database=AUNA;'
        r'UID=s_magallanes;'
        r'PWD=smagall@n3$;'
        )
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    cursor.execute(sql4)
    cnxn.close()


    return True


@asset
def generate_lt_xlsx(run_sql_lt:bool) -> bool:
    now = datetime.now()

    db_url="mssql://s_magallanes:smagall@n3$@192.168.30.51:1433/SCORE_TEMPRANA"
    sql_querys_sheet = {"IVR":"SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_IVR_AUNA",
                        "LISTA":"SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_LISTA_AUNA"}


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
    print('Execution generate_lt_xlsx: {:.1f}'.format(time_diff.total_seconds()))  
    
    return True



