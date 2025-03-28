# ========================================================================
#                              Imports
# ========================================================================

import time
import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from core.funciones_estandar import clear_screen
from core.funciones_estandar import consulta_a_df
from scripts.anonimizacion_rut import extrae_rut_a_ingresar, carga_rut, extrae_info_bd
from scripts.cargar_archivos_bucket import cargar_archivo_en_s3

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE_PATH = BASE_DIR / 'config' / 'config.yaml'

with open(CONFIG_FILE_PATH, 'r') as file:
    config = yaml.safe_load(file)

# ========================================================================
#                              Constantes
# ========================================================================

output_dir = Path(config['paths']['output_dir'])

CODIFICACION = "utf-8"
SEPARADOR = ";"
TIPOS_CONTACTO = {
    1002: "Titular",
    1201: "Titular",
    1204: "Sin Contacto",
    1205: "Tercero",
}

NO_CONSIDERAR = (1020, 1407, 1420, 1425, 1476, 1477, 1528, 1802)

CLASIFICACIONES = {
    0: "Administrativo",
    "0": "Administrativo",
    1: "Administrativo",
    4: "Administrativo",
    'IVR': "IVR",
    3: "Carta",
    'MAIL': "Correo Electrónico",
    'TEL': "Llamado",
    'SIS': "Sistema",
    'SMS': "Mensaje de Texto",
}

RESPUESTAS = {
    'Llamado': {1: "Contesta", 0: "No Contesta"},
    'Correo Electrónico': {1: "Enviado", 0: "No Enviado"},
    'Mensaje de Texto': {1: "Recibido", 0: "No Recibido"},
    'IVR': {1: "Contesta", 0: "No Contesta"},
    'Bot': {1: "Contesta", 0: "No Contesta"},
}

CONTACTO_EXCEPCION = {
    (3026, 1, 1): 1,
    (3026, 2, 1): 0,    
}

TIPO_CONTACTO_EXCEPCION = {
    (3026, 1, 1): "Tercero",    
}

# ========================================================================
#                          Funciones Auxiliares
# ========================================================================

def debug_print(message, end="\n"):
    if __name__ == "__main__":
        print(message, end=end)

def obtener_tabla_gestiones(tabla):
    consulta = f"SELECT * FROM {tabla}"
    df = consulta_a_df(consulta, servidor=72, database="BTC")
    if tabla == "TBGESCOD":
        df['tipo_gestion'] = df['TIPO'].map(CLASIFICACIONES)
    return df

# ========================================================================
#                          Funciones Anonimizacion
# ========================================================================

def extrae_base_anonimizacion(lista_rut:list):
    debug_print(f"Extraccion de Base para anonimizar...", end="\r")
    nuevos_ruts = extrae_rut_a_ingresar(lista_rut)

    if nuevos_ruts:
        carga_rut(nuevos_ruts)
    
    inicio = time.time()
    
    df_rut_homologados = extrae_info_bd(lista_rut)    

    fin = time.time()
    duracion = fin - inicio
    debug_print(f"Base Extraida: {duracion:.2f} segundos                           ")
    
    return df_rut_homologados

# ========================================================================
#                           Funciones Principales
# ========================================================================

def extrae_gestiones(dia_gestion:str):        
    debug_print(f"Extrayendo Gestiones...", end="\r")
    inicio = time.time()
    consulta_gestiones = f"""
        SELECT 
          CLIRUT          
        , GESFEG
        , GESCOD
        , HOUR(HORA) AS HORA
        , GESCOD1N
        , GESCOD2N
        , GESCOD3N
        , GESRESP
        FROM COBGES
        WHERE 
            GESFEG = CAST('{dia_gestion}'  AS DATE)
            AND GESCOD NOT IN {NO_CONSIDERAR}
    """
    
    df_gestiones = consulta_a_df(consulta_gestiones, servidor=72, database="CLARO")
    
    if df_gestiones is not None:
        df_gestiones['GESFEG'] = pd.to_datetime(df_gestiones['GESFEG'])
        df_gestiones['periodo'] = df_gestiones['GESFEG'].dt.to_period('M').astype(str)
    else:
        debug_print("Sin Gestiones")

    fin = time.time()
    duracion = fin - inicio
    debug_print(f"Gestiones Extraidas: {duracion:.2f} segundos                       ")

    registros_gestiones = f"{df_gestiones.shape[0]:,}".replace(",",".")    
    debug_print(f"Inicio Proceso de gestiones: {registros_gestiones} registros")
    
    return df_gestiones

def obtener_ultimo_dia_gestion_periodo(periodo):
    consulta_ultimo_dia = f"""
        SELECT MAX(GESFEG) AS ultimo_dia
        FROM COBGES
        WHERE 
            GESFEG >= CAST('{periodo}01'  AS DATE)
            AND GESCOD NOT IN (1020, 1407, 1420, 1425, 1476, 1477, 1528, 1802)
    """
    
    df_resultado = consulta_a_df(consulta_ultimo_dia, servidor=72, database="CLARO")
    
    if df_resultado is not None and not df_resultado.empty:
        ultimo_dia = pd.to_datetime(df_resultado.loc[0, 'ultimo_dia'])
        return ultimo_dia.strftime("%Y%m%d")
    else:
        debug_print("No se encontró ningún día de gestión.")
        return None

def extraccion_tablas_homologacion():
    debug_print(f"Extraccion de Tablas de Homologacion...", end="\r")
    inicio = time.time()
    
    df_tbgescod = obtener_tabla_gestiones("TBGESCOD")
    df_tbgestion2 = obtener_tabla_gestiones("TBGESTION2")
    df_tbgestion3 = obtener_tabla_gestiones("TBGESTION3")

    fin = time.time()
    duracion = fin - inicio    
    debug_print(f"Tablas de Homologacion obtenidas: {duracion:.2f} segundos                    ")
    
    return df_tbgescod, df_tbgestion2, df_tbgestion3

def combinar_datos(df_gestiones:pd.DataFrame,
                   df_rut_homologados:pd.DataFrame,
                   df_tbgescod:pd.DataFrame,
                   df_tbgestion2:pd.DataFrame,
                   df_tbgestion3:pd.DataFrame):

    # ========================================================================
    #                          Tabla Homologacion
    # ========================================================================

    debug_print(f"Combinando Tabla Homologacion RUT...", end="\r")
    inicio = time.time()
    
    df = df_gestiones.merge(df_rut_homologados, how='left', left_on='CLIRUT', right_on='RUT_DEUDOR')

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Tabla Homologacion RUT Combinada: {duracion:.2f} segundos                   ")

    # ========================================================================
    #                              Tabla TBGESCOD
    # ========================================================================
        
    debug_print(f"Combinando Tabla TBGESCOD...", end="\r")
    inicio = time.time()
    
    df = df.merge(df_tbgescod, how='left', left_on='GESCOD', right_on='GESCOD')

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Tabla TBGESCOD Combinada: {duracion:.2f} segundos                   ")

    # ========================================================================
    #                              Tabla TBGESTION2
    # ========================================================================

    debug_print(f"Combinando Tabla TBGESTION2...", end="\r")
    inicio = time.time()  
        
    df = df.merge(df_tbgestion2, how='left', on=['GESCOD1N', 'GESCOD2N'])

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Tabla TBGESTION2 Combinada: {duracion:.2f} segundos                   ")

    # ========================================================================
    #                              Tabla TBGESTION3
    # ========================================================================
    
    debug_print(f"Combinando Tabla TBGESTION3...", end="\r")
    inicio = time.time()
    
    df = df.merge(df_tbgestion3, how='left', on=['GESCOD1N', 'GESCOD2N', 'GESCOD3N'])

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Tabla TBGESTION3 Combinada: {duracion:.2f} segundos                   ")

    return df

def añade_columna_detalle(df: pd.DataFrame):
    debug_print(f"Añadiendo columna detalle...", end="\r")
    inicio = time.time()  
        
    df['detalle'] = np.where(
    df['DESCRIP'].notna(), df['DESCRIP'],
    np.where(
        df['DESCRIP_y'].notna(), df['DESCRIP_y'],
        df['DESCRIP_x']
    ))

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Columna detalle añadida: {duracion:.2f} segundos                   ")
    
    return df

def recategoriza_bot(df: pd.DataFrame):
    debug_print(f"Recategorizando BOT...", end="\r")
    inicio = time.time()      
    
    df.loc[df['detalle'].str.contains(r'\b(bot|robot)\b', case=False, na=False), 'tipo_gestion'] = 'Bot'

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"BOT Recategorizado: {duracion:.2f} segundos                   ")
    
    return df
 
def reclasifica_contacto_excepcion(df:pd.DataFrame):
    debug_print(f"Clasificando Contactos Excepcionales...", end="\r")
    inicio = time.time()  

    for excepcion in CONTACTO_EXCEPCION:
        gescod1n = excepcion[0]
        gescod2n = excepcion[1] 
        gescod3n = excepcion[2] 
        valor = CONTACTO_EXCEPCION[excepcion]
        df.loc[(df['GESCOD1N'] == gescod1n) & (df['GESCOD2N'] == gescod2n) & (df['GESCOD3N'] == gescod3n), 'CONTACTO'] = valor
        

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Contactos Excepcionales reclasificados: {duracion:.2f} segundos                   ")    
    
    return df
 
def crea_columna_respuesta(df: pd.DataFrame):
    debug_print(f"Creando columna respuesta...", end="\r")
    inicio = time.time()  

    # Mapea tipo_gestion al diccionario RESPUESTAS
    respuesta_dict = df['tipo_gestion'].map(RESPUESTAS)

    df['respuesta'] = [
        respuesta_dict[i].get(contacto, "No Determinado") 
        if isinstance(respuesta_dict[i], dict) else "No Determinado"
        for i, contacto in enumerate(df['CONTACTO'])
    ]
    

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Columna respuesta creada: {duracion:.2f} segundos                   ")

    return df

def añade_columna_tipo_contacto(df: pd.DataFrame):
    debug_print(f"Añadiendo column tipo_contacto...", end="\r")
    inicio = time.time()     
    
    df["tipo_contacto"] = df["GESCOD"].map(TIPOS_CONTACTO).fillna("Desconocido")
    
    # Aplicar la condición
    df.loc[df["CONTACTO"] == 0, "tipo_contacto"] = "Sin Contacto"

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Columna tipo_contacto añadida: {duracion:.2f} segundos                   ")    
    
    return df

def reclasifica_tipo_contacto_excepcion(df:pd.DataFrame):
    debug_print(f"Reclasificando Tipo de Contacto Excepcionales...", end="\r")
    inicio = time.time()  

    for excepcion in TIPO_CONTACTO_EXCEPCION:
        gescod1n = excepcion[0]
        gescod2n = excepcion[1] 
        gescod3n = excepcion[2] 
        valor = TIPO_CONTACTO_EXCEPCION[excepcion]
        df.loc[(df['GESCOD1N'] == gescod1n) & (df['GESCOD2N'] == gescod2n) & (df['GESCOD3N'] == gescod3n), 'tipo_contacto'] = valor
        

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Tipo de Contactos Excepcionales reclasificados: {duracion:.2f} segundos                   ")    
    
    return df

def crea_estructura_archivo(df: pd.DataFrame):
    debug_print(f"Creando estructura final...", end="\r")
    inicio = time.time()  
    
    columnas_finales = {
        "ID_DEUDOR": "fld_cli",
        "periodo": "periodo",
        "HORA": "hora_gestion",
        "GESFEG": "fecha_gestion",
        "tipo_gestion": "tipo_gestion",
        "tipo_contacto": "tipo_contacto",
        "respuesta": "respuesta",
        "detalle": "detalle",
    }

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"DF Estructurado: {duracion:.2f} segundos                   ")  
    
    return df[columnas_finales.keys()].rename(columns=columnas_finales)

def guardar_resultados(df_resultado: pd.DataFrame, dia_gestion:str):
    debug_print(f"\nGuardando Archivo...", end="\r") 
    inicio = time.time()
    
    nombre_archivo = f'gestiones_{dia_gestion}.csv'
    ruta_salida = Path(output_dir/nombre_archivo)
        
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)
    
    df_resultado.to_csv(ruta_salida, sep=SEPARADOR, index=False, encoding=CODIFICACION)
    
    print(f"Archivo {ruta_salida.name} generado.")

    fin = time.time()
    duracion = fin - inicio     
    debug_print(f"Archivo guardado: {duracion:.2f} segundos                      ")
    return ruta_salida
    
def sube_archivo(ruta_archivo:Path):
    nombre_archivo = ruta_archivo.name   
    cargar_archivo_en_s3(nombre_archivo, ruta_archivo)   

# ========================================================================
#                           Procesamiento Principal
# ========================================================================

def generar_archivo_gestiones(dia_gestion:str = None):
    # ========================================================================
    #                             Extrae Gestiones
    # ========================================================================
    if dia_gestion is None:
        dia_gestion = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    df_gestiones =  extrae_gestiones(dia_gestion)
    
    if df_gestiones.empty:
        periodo = (datetime.now() - timedelta(days=1)).strftime("%Y%m")
        dia_gestion = obtener_ultimo_dia_gestion_periodo(periodo)
        df_gestiones =  extrae_gestiones(dia_gestion)
    # ========================================================================
    #                       Extrae Base de Anonimizacion
    # ========================================================================

    lista_rut = df_gestiones["CLIRUT"].unique().tolist()

    df_rut_homologados = extrae_base_anonimizacion(lista_rut)

    # ========================================================================
    #                     Extracción de Tablas de Homologación
    # ========================================================================

    df_tbgescod, df_tbgestion2, df_tbgestion3 = extraccion_tablas_homologacion()

    # ========================================================================
    #                                Combina Tablas
    # ========================================================================
    
    df = combinar_datos(
        df_gestiones, df_rut_homologados, df_tbgescod, df_tbgestion2, df_tbgestion3
    )

    # ========================================================================
    #                             Añade Columna Detalle    
    # ========================================================================

    df = añade_columna_detalle(df)

    # ========================================================================
    #                             Recategorizando BOT    
    # ========================================================================
    
    df = recategoriza_bot(df)

    # ========================================================================
    #                       Clasifica Contactos Excepcionales    
    # ========================================================================

    df = reclasifica_contacto_excepcion(df)

    # ========================================================================
    #                             Clasifica Respuestas  
    # ========================================================================

    df = crea_columna_respuesta(df)

    # ========================================================================
    #                          Añade columna tipo_contacto
    # ========================================================================

    df = añade_columna_tipo_contacto(df)

    # ========================================================================
    #                       Clasifica Tipo Contactos Excepcionales
    # ========================================================================
    
    df = reclasifica_tipo_contacto_excepcion(df)

    # ========================================================================
    #                          Crea Estructura de Archivo
    # ========================================================================

    df = crea_estructura_archivo(df)

    # ========================================================================
    #                              Guarda Archivo
    # ========================================================================
                
    ruta_archivo = guardar_resultados(df, dia_gestion)

    # ========================================================================
    #                              Subir Archivo
    # ========================================================================
    
    sube_archivo(ruta_archivo)
        

if __name__ == "__main__":
    clear_screen()
    generar_archivo_gestiones()
    
