import os
import shutil
import pandas as pd
from pathlib import Path
from datetime import datetime
from core.gestor_bd import carga_datos_desde_csv

def carga_tabla_desde_df(df: pd.DataFrame, tabla_destino: str, rutas: dict = {}, limpiar: bool = False, servidor_destino: int = 23, base_destino: str = "report_cartera", mensaje_dev: bool = False):
    try:
        pd.set_option('display.float_format', lambda x: '%.f' % x)
        
        if rutas:        
            ruta_base_deposito = Path(f"{rutas['BASE']}/temp/")
        else:
            ruta_ejecucion = Path("").resolve()
            ruta_base_deposito = Path(f"{ruta_ejecucion}/temp/")
            
        ruta_base_deposito.mkdir(parents=True, exist_ok=True)
        
        os.system(f'attrib +h "{ruta_base_deposito}"')

        fecha_hora_actual = datetime.now()
        fecha_hora_formateada = fecha_hora_actual.strftime("%Y%m%d%H%M%S")

        nombre_archivo_final = f"temp_{fecha_hora_formateada}.csv"
        ruta_final = Path(f"{ruta_base_deposito}/{nombre_archivo_final}")

        df.to_csv(ruta_final, index=False, sep=";", encoding='utf-8-sig')

        # Verificar si el archivo fue creado
        if ruta_final.exists():
            carga_datos_desde_csv(ruta_archivo=ruta_final, servidor=servidor_destino, database=base_destino, tabla=tabla_destino, limpiar=limpiar, mensaje=False)
        else:
            print(f"Error: el archivo temporal {ruta_final} no fue creado.")
            
        shutil.rmtree(ruta_base_deposito)
            
        if mensaje_dev:
            print(f"Dataframe cargado...\nDestino: \nTabla: {tabla_destino}  \nBase: {base_destino}  \nServidor: 172.16.10.{servidor_destino} ")
             
    except Exception as e:
        print(f"No se pudo crear el archivo temporal: {e}")
   