# ========================================================================
#                         Imports y Configuracion Inicial
# ========================================================================

import yaml
import pandas as pd
from pathlib import Path
from funciones_estandar import consulta_a_df
from anonimizacion_rut import extrae_rut_a_ingresar, carga_rut, extrae_info_bd
from cargar_archivos_bucket import cargar_archivo_en_s3
from datetime import datetime, timedelta


with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

output_dir = Path(config['paths']['output_dir'])

# ========================================================================
#                          Funciones Auxiliares
# ========================================================================

def construye_consulta_pagos(cartera:str, fecha_inicio:str, fecha_fin:str):
    base = "CLARO"
    tabla = f"TABPAGO_{cartera}"

    consulta = f"""
    SELECT  
      CAST(SUBSTRING_INDEX(RUT,'-',1) AS INT) AS fld_cli
    , NUMDOCUMENTO as numero_boleta
    , FECHA_PAGO AS fecha_pago
    , CAST(CAPITAL AS INT) AS monto_pago
    , FECHA_CARGA as fecha_carga
    , CASE 
        WHEN FECHA_PAGO = '000000000' THEN 'NOTA DE CREDITO'
        ELSE 'PAGO' END AS tipo 
    , '{cartera}' AS cartera
    FROM {base}.{tabla}
    WHERE
        CAST(SUBSTRING_INDEX(RUT,'-',1) AS INT) > 0  
        AND CAST(CAPITAL AS INT) > 0 
        AND FECHA_CARGA BETWEEN CAST('{fecha_inicio}' AS DATE) AND CAST('{fecha_fin}' AS DATE)
    ;
    """  
    
    return consulta
   
# ========================================================================
#                           Funciones Principales
# ======================d==================================================

def obtener_rango_fechas(fecha_entrada:str = None):
    # Si se ingresó un parámetro de fecha, se tiene que corroborar que ese parametro sea realmente una fecha (Se tiene que retornar un datetime)
    if fecha_entrada is not None:
        
        año = int(fecha_entrada[:4])
        mes = int(fecha_entrada[4:6])
        dia = int(fecha_entrada[6:])

        fecha_fin = datetime(año, mes, dia)
   
    # Si no se ingresó ninguna fecha, eso quiere decir que se va a extraer la data hasta el ultimo dia del periodo actual (Hoy)
    
    else:
        fecha_fin = datetime.now()
    
    
    # Lo previo establece la fecha_fin
    fecha_inicio = fecha_fin.replace(day = 1)
    
    fecha_inicio_str = fecha_inicio.strftime('%Y%m%d')
    fecha_fin_str = fecha_fin.strftime('%Y%m%d')
    
    return fecha_inicio_str, fecha_fin_str
    
def extrae_df_pagos(fecha_inicio:str, fecha_fin:str):
    
    CARTERAS = ["PREVENTIVA", "MORA", "CASTIGO"]
    dfs = []
        
    for cartera in CARTERAS:
        consulta = construye_consulta_pagos(cartera, fecha_inicio, fecha_fin)
        df = consulta_a_df(consulta, servidor=72, database="CLARO")    
        dfs.append(df)
    
    # DF Consolidado
    df_pagos = pd.concat(dfs, ignore_index=True)
    
    return df_pagos
    
def anonimizacion_rut(df:pd.DataFrame):
    # Anonimizar RUTs
    lista_rut_gestion:list = df["fld_cli"].unique().tolist()
    lista_rut_ingresar = extrae_rut_a_ingresar(lista_rut_gestion)
    
    if lista_rut_ingresar:
        carga_rut(lista_rut_ingresar)
        
    df_homologacion_rut = extrae_info_bd(lista_rut_gestion)

    resultado_df = df.merge(
        df_homologacion_rut,
        how='left',
        left_on=['fld_cli'],
        right_on=['RUT_DEUDOR']
    )
    
    return resultado_df
    
def genera_estructura_archivo(df:pd.DataFrame):
    # Redefinir estructura de columnas
    nueva_estructura = {
        "ID_DEUDOR": "fld_cli",
        "numero_boleta": "numero_boleta",
        "fecha_pago": "fecha_pago",
        "monto_pago": "monto_pago",
        "fecha_carga": "fecha_carga",
        "tipo":"tipo",
        "cartera":"cartera",
    }
    
    df = df[list(nueva_estructura.keys())].rename(columns=nueva_estructura)
    
    return df
    
def guarda_archivo(fecha_carga:str, df:pd.DataFrame):
    # Separar y guardar registros   
    nombre_archivo = f"pagos_{fecha_carga}.csv"
    
    ruta_pago = Path(output_dir / nombre_archivo)
    ruta_pago.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(ruta_pago, sep=";", index=False)
    print(f"Archivo {nombre_archivo} generado.")
    
    return ruta_pago

def sube_archivo(ruta_archivo:Path):
    nombre_archivo = ruta_archivo.name   
    cargar_archivo_en_s3(nombre_archivo, ruta_archivo)    
    
# ========================================================================
#                           Procesamiento Principal
# ========================================================================

def crea_archivo_pagos(fecha_carga:str = None):
    # ========================================================================
    #                        Valida y Extrae Fechas
    # ========================================================================
    
    fecha_inicio, fecha_fin = obtener_rango_fechas(fecha_carga)

    # ========================================================================
    #                             Extrae DF Pagos
    # ======================================================================== 

    df = extrae_df_pagos(fecha_inicio, fecha_fin )
    
    if not df.empty:

        # ========================================================================
        #                            Anonimización
        # ========================================================================

        df = anonimizacion_rut(df)

        # ========================================================================
        #                            Nueva Estructura
        # ========================================================================
        
        df = genera_estructura_archivo(df)
        
        # ========================================================================
        #                           Guardar Archivo en CSV
        # ========================================================================
        
        ruta_archivo = guarda_archivo(fecha_fin, df)

        # ========================================================================
        #                           Sube Archivo a Bucket
        # ========================================================================
        
        sube_archivo(ruta_archivo)

# ========================================================================
#                             Ejecutable
# ========================================================================
    
if __name__ == "__main__":
    crea_archivo_pagos()

