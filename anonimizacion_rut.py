# ========================================================================
#                                 Imports
# ========================================================================

import pandas as pd
from pathlib import Path
from datetime import datetime
from funciones_estandar import clear_screen
from funciones_estandar import consulta_a_df
from funciones_estandar import carga_tabla_desde_df

# ========================================================================
#                               Constantes
# ========================================================================

RESPALDO_PATH = Path("").resolve()
TABLA_HOMOLOGACION = "ML_Claro_Homologacion"
CODIFICACION = "latin1"
SEPARADOR = ";"

# ========================================================================
#                               Funciones
# ========================================================================

def extrae_info_bd() -> pd.DataFrame:
    """
    Extrae toda la información de la tabla ML_Claro_Homologacion desde la base de datos.
    
    Returns:
        pd.DataFrame: DataFrame con los datos extraídos de la base de datos.
    """
    consulta = f"SELECT * FROM {TABLA_HOMOLOGACION}"
    return consulta_a_df(consulta)

def extrae_rut_a_ingresar(lista_rut: list) -> list:
    """
    Determina los RUTs que no están en la base de datos y que deben ser ingresados.
    
    Args:
        lista_rut (list): Lista de RUTs proporcionados para verificar.
    
    Returns:
        list: Lista de RUTs que deben ser ingresados.
    """
    df_bd = extrae_info_bd()
    
    if df_bd.empty:
        return list(set(lista_rut))
    
    lista_bd = set(df_bd["RUT_DEUDOR"].to_list())
    return list(set(lista_rut) - lista_bd)

def carga_rut(lista_rut_carga: list):
    """
    Carga una lista de RUTs a la tabla ML_Claro_Homologacion y genera un respaldo en formato CSV.
    
    Args:
        lista_rut_carga (list): Lista de RUTs a cargar en la tabla.
    """
    # Crear DataFrame a partir de la lista de RUTs
    df_carga = pd.DataFrame(lista_rut_carga, columns=["RUT_DEUDOR"])
    
    # Cargar datos a la tabla
    carga_tabla_desde_df(df_carga, TABLA_HOMOLOGACION)
        
    if len(lista_rut_carga) > 0:
        print(f"RUT nuevos sin anonimizar: {len(lista_rut_carga)}")

        # Consultar datos actualizados de la tabla
        consulta = f"SELECT * FROM {TABLA_HOMOLOGACION}"
        
        df_respaldo = consulta_a_df(consulta)
        
        # Generar ruta y nombre para el archivo de respaldo
        fecha_ahora = datetime.now().strftime("%Y%m%d%H%M")
        nombre_archivo_respaldo = f"homologacion_rut_{fecha_ahora}.csv"
        ruta_archivo_respaldo = RESPALDO_PATH / nombre_archivo_respaldo
        
        # Guardar respaldo en CSV
        df_respaldo.to_csv(ruta_archivo_respaldo, sep=SEPARADOR, index=False, encoding=CODIFICACION)
        print(f"Respaldo guardado en: {ruta_archivo_respaldo}")

# ========================================================================
#                                  Main
# ========================================================================

def main():
    """
    Funcion principal para ejecutar las operaciones necesarias.
    """
    clear_screen()
    lista = extrae_rut_a_ingresar([1, 2, 2])
    print(f"Largo: {len(lista)}")

if __name__ == "__main__":
    main()
