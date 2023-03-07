import pandas as pd
import pyodbc 

writer = pd.ExcelWriter('LISTA AUNA.xlsx', engine='xlsxwriter')
cnxn = pyodbc.connect("Driver={SQL Server}; Server=192.168.30.51; uid=s_magallanes; pwd=smagall@n3$; Trusted_Connection=No;")

script = """
SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_IVR_AUNA
"""
df = pd.read_sql_query(script, cnxn)
df.to_excel(writer, sheet_name='IVR', index=False)


script = """
SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_LISTA_AUNA;
"""
df = pd.read_sql_query(script, cnxn)
df.to_excel(writer, sheet_name='LISTA', index=False)
    
# Save Data to File
writer.close()