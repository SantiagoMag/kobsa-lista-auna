import os
from datetime import datetime
import pandas as pd

folder = 'origen'
files = {i.split("_")[0]:folder + r'/' + i for i in os.listdir('origen')}
dict_dtype = {'idfuente': int, 'idorigentelefono': int,'idcliente':str,'telefono':str,'anexo':str,'idestado':int,'prioridad':int}

def transform_df(df):    
    df["PMT_IDFUENTE"] = df["idfuente"]
    df["PMT_IDORIGEN"] = df["idorigentelefono"]
    df["PMT_IDCARTERA"] = df["idcartera"]
    df["PMT_DOCUMENTO"] = df["idcliente"].str.strip()

    pmt_telefono_values = []
    for i in df["telefono"].values:
        if len(i)==8 and i[0]=="9":
            value = "9" + str(i)
        elif len(i)==8 and i[0]!="9":
            value = "0" + str(i)
        elif len(i)==7 :
            value = "01" + str(i)
        elif len(i)==10 and i[:2]=="10":
            value = i[1:10]
        else :
            value = i
        pmt_telefono_values.append(i)
    df["PMT_TELEFONO"] = pmt_telefono_values
    df["PMT_TELEFONO"] = df["PMT_TELEFONO"].str.strip()

    df["PMT_CADENA"] = df["PMT_DOCUMENTO"]+df["PMT_TELEFONO"]
    df["PMT_PRIORIDAD"] = df["prioridad"]
    df["PMT_OBSERVACION"] = ["Asignacion Cliente" if i=="" else i for i in df["prioridad"].values]

    df["AUX_MB"] = None
    df["FEC_INGRESO"] = datetime.today().strftime('%Y-%m-%d')
    df["CLARO_ESTADOS"] = ["" if i=="" else i for i in df["prioridad"].values]

    columns = ['PMT_IDFUENTE','PMT_IDORIGEN','PMT_IDCARTERA','PMT_DOCUMENTO','PMT_TELEFONO','PMT_CADENA','PMT_PRIORIDAD','PMT_OBSERVACION','AUX_MB','FEC_INGRESO','CLARO_ESTADOS',]
    return df[columns]


df_asignaciones = pd.read_excel(files["ASIGNACIONES"],dtype=dict_dtype)
df_asignaciones = df_asignaciones[list(dict_dtype.keys())]
df_asignaciones["observacion"] = None
df_asignaciones["idcartera"] = 7
df_asignaciones["estado_servicio"] = None
df_asignaciones = transform_df(df_asignaciones)


df_busquedas = pd.read_excel(files["BUSQUEDA"],dtype=dict_dtype)
df_busquedas = df_busquedas[list(dict_dtype.keys())]
df_busquedas["idfuente"] = 5
df_busquedas["prioridad"] = 5
df_busquedas["observacion"] = None
df_busquedas["idcartera"] = 7
df_busquedas["estado_servicio"] = None
df_busquedas = transform_df(df_busquedas)

auna = pd.concat([df_asignaciones,df_busquedas])
auna.to_excel("auna.xlsx",index=False,header=False)
