# ========================================================================
#                         Imports y Configuracion Inicial
# ========================================================================

import yaml
import pandas as pd
from pathlib import Path
from datetime import datetime
from funciones_estandar import clear_screen
from funciones_estandar import consulta_a_df
from anonimizacion_rut import extrae_rut_a_ingresar, carga_rut, extrae_info_bd
from cargar_archivos_bucket import cargar_archivo_en_s3

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

output_dir = Path(config['paths']['output_dir'])

# ========================================================================
#                          Funciones Auxiliares
# ========================================================================

# ========================================================================
#                           Funciones Principales
# ========================================================================

def extrae_df ():
    
    consulta = f"""
        SELECT DISTINCT
          CLIRUT
        , CLIDV AS DV
        FROM COBVEC
        WHERE
            CLIEST2 IN (1, 3, 11, 20, 47, 56)
    """
    
    df = consulta_a_df(consulta, servidor = 72, database = "CLARO")
    
    return df
    
def anonimizacion_rut(df:pd.DataFrame):
    # Anonimizar RUTs
    lista_rut_gestion = df["CLIRUT"].to_list()
    lista_rut_ingresar = extrae_rut_a_ingresar(lista_rut_gestion)
    
    if lista_rut_ingresar:
        carga_rut(lista_rut_ingresar)
        
        
    df_homologacion_rut = extrae_info_bd()

    resultado_df = df.merge(
        df_homologacion_rut,
        how='left',
        left_on=['CLIRUT'],
        right_on=['RUT_DEUDOR']
    )
    
    return resultado_df
    
def genera_estructura_archivo(df:pd.DataFrame):
    # Redefinir estructura de columnas
    nueva_estructura = {
        "ID_DEUDOR": "fld_cli",
        "DV": "DV" 
    }
    
    df = df[list(nueva_estructura.keys())].rename(columns=nueva_estructura)
    
    return df
    
def guarda_archivo(df:pd.DataFrame, fecha_carga:str):   
    # Separar y guardar registros   
    nombre_archivo = f"id_activos_{fecha_carga}.csv"
    
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

def crea_archivo_id_activos(fecha_proceso:str = None):
    # ========================================================================
    #                             Extrae DF
    # ========================================================================
    if fecha_proceso is None:
        fecha_proceso = datetime.now().strftime("%Y%m%d")  
    
    df = extrae_df()
    
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
        
        ruta_archivo = guarda_archivo(df, fecha_proceso)

        # ========================================================================
        #                           Sube Archivo a Bucket
        # ========================================================================
        
        sube_archivo(ruta_archivo)

# ========================================================================
#                             Ejecutable
# ========================================================================

if __name__ == "__main__":
    clear_screen()
    crea_archivo_id_activos()
