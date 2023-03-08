import pyodbc 

sql1 = """
    USE [SCORE_TEMPRANA];
    exec [dbo].[sp_bulk_score_temprana];
"""

sql2 = """
    USE [AUNA];
    exec [dbo].[sp_contac_w_rg] ;
"""

sql3 = """
    USE [AUNA];
    exec [dbo].[sp_cuentas];
"""

sql4 = """
    USE [SCORE_TEMPRANA];
    exec [dbo].[sp_contact_cuentas_auna_lt];
    exec [dbo].[ivr_auna];
    exec [dbo].[lista_auna];

"""

#### CORRER EN PARALELO SQL 1 - 3

#### AL TERMINAR CORRER SQL 4

#### DESCARGAR EXCEL CON 

import pyodbc
conn_str = (
    r'Driver=SQL Server;'
    r'Server=192.168.30.51;'
    r'Database=SCORE_TEMPRANA;'
    r'Trusted_Connection=yes;'
    )
cnxn = pyodbc.connect(conn_str)
cursor = cnxn.cursor()
cursor.execute("SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_LISTA_AUNA")
cursor.execute("SELECT * FROM [SCORE_TEMPRANA].DBO.TEMP_IVR_AUNA")
row = cursor.fetchone() 
print(row)
cnxn.close()