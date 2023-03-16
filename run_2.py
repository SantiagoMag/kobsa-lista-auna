import pyodbc
from dagster import job, op, get_dagster_logger, In, Out

# conn_str = (
#     r'Driver=SQL Server;'
#     r'Server=192.168.30.51;'
#     r'Database=SCORE_TEMPRANA;'
#     r'UID=procesamiento;'
#     r'PWD=pr0c3s@m13nt0;'
#     )
# cnxn = pyodbc.connect(conn_str)
# cursor = cnxn.cursor()
# cursor.execute("select top 1 * from APOYO")
# row = cursor.fetchone() 
# print(row)

# cnxn.close()
import pandas as pd

#### CARGA TELEFONOS ASIGNACIONES Y BUSQUEDAS EN DF
@op
def load_df():

    df = pd.DataFrame()
    return df

#### 
@op
def transform_df(df):




def run_sql(file, completed):
    try:
        conn_str = (
            r'Driver=SQL Server;'
            r'Server=192.168.30.51;'
            r'Database=SCORE_TEMPRANA;'
            r'UID=procesamiento;'
            r'PWD=pr0c3s@m13nt0;'
            )
        con = pyodbc.connect(conn_str)
        cursor = con.cursor()
        get_dagster_logger().info("Conectado ... ")

        with open(file, 'r') as myfile:
                data = myfile.read()
                get_dagster_logger().info("Reading ... " + file)
                get_dagster_logger().info("Script ... " + str(data))
                print(data.split(sep=";"))
                for j in data.split(sep=";"):
                    if j != "":
                        sql = j
                        get_dagster_logger().info("Executing ... " + str(sql))
                        cursor.execute(sql)
                        #con.commit()
                        get_dagster_logger().info("Executed ...")
                completed = True

    except Exception as err :
        print(err)

    finally:
        cursor.close()
        con.close()
        get_dagster_logger().info("Se cerró la conexión... ")

    return completed        


@op
def sql_bulks_score_temprana():
    completed = False
    completed = run_sql("sql/1. BULKS SCORE TEMPRANA.sql",completed)
    return completed



@op
def sql_cuentas(completed_before = False):
    completed = False
    if completed_before == False:
        completed = run_sql("sql/2. CUENTAS.sql",completed)
    return completed

@op
def sql_contact(completed_before = False):
    completed = False
    if completed_before == False:
        completed = run_sql("sql/3. CONTAC w. RG.sql",completed)
    return completed


# @op
# def sql_listas(completed_before_cuentas, completed_before_contact):
#     completed = False
#     if completed_before_cuentas == True and completed_before_contact == True:
#         completed = run_sql("sql/5. PRUEBA.sql",completed)
#     return completed



@op
def sql_listas(completed_before_cuentas):
    completed = False
    if completed_before_cuentas == True:
        completed = run_sql("sql/4. PARA LA LISTA DE LUIS.sql",completed)
    return completed


@job
def run():
    r = sql_bulks_score_temprana()
    r1 = sql_cuentas(r)
    r2 = sql_contact(r)
    # r3 = sql_listas(r1,r2)
    # r2 = sql_listas(r)