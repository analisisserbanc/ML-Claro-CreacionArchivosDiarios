# ========================================================================
#                                 Imports
# ========================================================================

import yaml
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from funciones_estandar import clear_screen
from funciones_estandar import consulta_a_df
from funciones_estandar import carga_tabla_desde_df
from anonimizacion_rut import extrae_rut_a_ingresar, carga_rut, extrae_info_bd
from cargar_archivos_bucket import cargar_archivo_en_s3

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

output_dir = Path(config['paths']['output_dir'])

# ========================================================================
#                           Funcion Carga Archivo
# ========================================================================

def sube_archivo(ruta_archivo:Path):
    nombre_archivo = ruta_archivo.name   
    cargar_archivo_en_s3(nombre_archivo, ruta_archivo)   

# ========================================================================
#                      Funciones Actualiza Tramo Edad
# ========================================================================

def actualiza_tramo_edad():
    base = f"CLARO"
    tabla = f"CUBO_CLARO"
    
    
    consulta_cubo = f"""
    SELECT DISTINCT
      IDDEUDOR AS RUT
    , TRAMO_EDAD
    FROM {base}.{tabla}
    """
    
    df_cubo = consulta_a_df(consulta_cubo, servidor = 72, database = base)
        
    if df_cubo is not None:    
        lista_rut_stock = set(df_cubo["RUT"].to_list())
        
        consulta_base = """
        SELECT RUT_DEUDOR 
        FROM ML_Claro_Homologacion_TramoEdad
        """
        lista_base = set(consulta_a_df(consulta_base)["RUT_DEUDOR"].to_list())
        
        lista_a_actualizar = list(set(lista_rut_stock) - lista_base)
        
        df_actualizar  = df_cubo[df_cubo["RUT"].isin(lista_a_actualizar)]
        registros_actualizar = df_actualizar.shape[0]
        
        if registros_actualizar > 0:
            registros_formato = f"{registros_actualizar:,}"
            print(f"Se actualizarán {registros_formato.replace(",",".")} registros")
            
            carga_tabla_desde_df(df_actualizar, "ML_Claro_Homologacion_TramoEdad", limpiar = False)
            
            print("Se actualizó la tabla ML_Claro_Homologacion_TramoEdad")
    
# ========================================================================
#                           Funciones Principales
# ========================================================================



def extrae_asignacion(fecha_proceso:str):
    base = "CLARO"
    tabla = "STOCK"

    
    consulta = f"""
    SELECT  
    FECHA_REPORTE AS fecha_asignacion
    , CLIRUT AS fld_cli
    , CUENTA
    , LINEA_DE_NEGOCIO AS nombre_servicio
    , TIPO_DE_SERVICIO AS tipo_servicio
    , CAST(DEUDA_EN_DIAS AS INT) AS dias_mora
    , VENCIMIENTO AS fecha_vencimiento
    , CASE 
      WHEN CARTERA IN ('PREVENTIVA') THEN SCMO_SALDO_NO_VENCIDO 
      WHEN CARTERA IN ('MORA', 'CASTIGO') THEN SCMO_SALDO_VENCIDO
      END AS monto_deuda
    , 'PESOS' AS moneda
    , SCMO_DIR_LEGAL1 AS direccion
    
    , SCMO_NUM_FACTURA_1 AS boleta_1_numero
    , SCMO_FECHA_EM_FACT1 AS boleta_1_fecha_emision
    , SCMO_FECHA_VENC_FACT1 AS boleta_1_fecha_vencimiento
    , SCMO_MONTO_FACT_1 AS boleta_1_monto_boleta
    , SCMO_SALDO_PEND_FACT_1 as boleta_1_saldo_pendiente
    
    , SCMO_NUM_FACTURA_2 AS boleta_2_numero
    , SCMO_FECHA_EM_FACT2 AS boleta_2_fecha_emision
    , SCMO_FECHA_VENC_FACT2 AS boleta_2_fecha_vencimiento
    , SCMO_MONTO_FACT_2 AS boleta_2_monto_boleta
    , SCMO_SALDO_PEND_FACT_2 as boleta_2_saldo_pendiente    

    , SCMO_NUM_FACTURA_3 AS boleta_3_numero
    , SCMO_FECHA_EM_FACT3 AS boleta_3_fecha_emision
    , SCMO_FECHA_VENC_FACT3 AS boleta_3_fecha_vencimiento
    , SCMO_MONTO_FACT_3 AS boleta_3_monto_boleta
    , SCMO_SALDO_PEND_FACT_3 as boleta_3_saldo_pendiente

    , SCMO_NUM_FACTURA_4 AS boleta_4_numero
    , SCMO_FECHA_EM_FACT4 AS boleta_4_fecha_emision
    , SCMO_FECHA_VENC_FACT4 AS boleta_4_fecha_vencimiento
    , SCMO_MONTO_FACT_4 AS boleta_4_monto_boleta
    , SCMO_SALDO_PEND_FACT_4 as boleta_4_saldo_pendiente

    , SCMO_NUM_FACTURA_5 AS boleta_5_numero
    , SCMO_FECHA_EM_FACT5 AS boleta_5_fecha_emision
    , SCMO_FECHA_VENC_FACT5 AS boleta_5_fecha_vencimiento
    , SCMO_MONTO_FACT_5 AS boleta_5_monto_boleta
    , SCMO_SALDO_PEND_FACT_5 as boleta_5_saldo_pendiente

    , SCMO_NUM_FACTURA_6 AS boleta_6_numero
    , SCMO_FECHA_EM_FACT6 AS boleta_6_fecha_emision
    , SCMO_FECHA_VENC_FACT6 AS boleta_6_fecha_vencimiento
    , SCMO_MONTO_FACT_6 AS boleta_6_monto_boleta
    , SCMO_SALDO_PEND_FACT_6 as boleta_6_saldo_pendiente
    
    , SEGMENTACION AS tipo_cli
    , CASE WHEN SCMO_PLAN_F IN (NULL, 'NULL') THEN '' ELSE SCMO_PLAN_F END AS plan
    , SCMO_ESTADO AS estado_cuenta
    , SCMO_FECHA_INI_SERVICIO AS fecha_inicio_servicio
    , SCMO_FECHA_FIN_SERVICIO AS fecha_fin_servicio
    , UPPER(CAMPANA_ASIGNACION) AS region
    , UPPER(CARGO_ACTIVACION_EQUIPO) AS comuna
    , CARTERA AS cartera
    , FECHA_PROCESO AS fecha_proceso
    FROM {base}.{tabla}
    WHERE 
        CLIRUT > 0 AND
        CARTERA IN ('CASTIGO', 'MORA', 'PREVENTIVA') AND
        FECHA_PROCESO = CAST('{fecha_proceso}' AS DATE)
    ORDER BY CLIRUT
    """
    df = consulta_a_df(consulta, servidor=72, database=base)
    
    return df

def extrae_campana_vista():
    base = f"CLARO"
    tabla = f"CAMPANA_VISTA"        

    consulta = f"""
    SELECT * 
    FROM {base}.{tabla};
    """
    df = consulta_a_df(consulta, servidor=72, database=base)
    
    return df

def extrae_homologacion_tramo():
    actualiza_tramo_edad()
    
    consulta = """
    SELECT * 
    FROM ML_Claro_Homologacion_TramoEdad
    """
    
    df = consulta_a_df(consulta)
    
    return df

def anonimizacion_rut(df_asignacion:pd.DataFrame):
    lista_rut_gestion = df_asignacion["fld_cli"].to_list()
    lista_rut_ingresar = extrae_rut_a_ingresar(lista_rut_gestion)
    if lista_rut_ingresar:
        carga_rut(lista_rut_ingresar)
    df_homologacion_rut = extrae_info_bd() 
    
    return df_homologacion_rut

def combinar_dataframes(df_asignacion:pd.DataFrame, df_campana_vista:pd.DataFrame, df_tramo:pd.DataFrame, df_homologacion_rut:pd.DataFrame):
    resultado_df = df_asignacion.merge(
        df_campana_vista,
        how='left',
        left_on=['fld_cli', 'CUENTA'],
        right_on=['CLIRUT', 'CUENTA']
    )

    resultado_df = resultado_df.merge(
        df_tramo,
        how='left',
        left_on=['fld_cli'],
        right_on=['RUT_DEUDOR']
    )


    resultado_df = resultado_df.merge(
        df_homologacion_rut,
        how='left',
        left_on=['fld_cli'],
        right_on=['RUT_DEUDOR']
    )
    
    return resultado_df
 
def genera_estructura_final(resultado_df:pd.DataFrame):
    nueva_estructura = { 
        "fecha_asignacion": "fecha_asignacion",
        "ID_DEUDOR": "fld_cli",
        "nombre_servicio": "nombre_servicio",
        "tipo_servicio": "tipo_servicio",
        "dias_mora": "dias_mora",
        "fecha_vencimiento": "fecha_vencimiento",
        "monto_deuda": "monto_deuda",
        "moneda": "moneda",
        "direccion": "direccion",
        "boleta_1_numero": "boleta_1_numero",
        "boleta_1_fecha_emision": "boleta_1_fecha_emision",
        "boleta_1_fecha_vencimiento": "boleta_1_fecha_vencimiento",
        "boleta_1_monto_boleta": "boleta_1_monto_boleta",
        "boleta_1_saldo_pendiente": "boleta_1_saldo_pendiente",
        "boleta_2_numero": "boleta_2_numero",
        "boleta_2_fecha_emision": "boleta_2_fecha_emision",
        "boleta_2_fecha_vencimiento": "boleta_2_fecha_vencimiento",
        "boleta_2_monto_boleta": "boleta_2_monto_boleta",
        "boleta_2_saldo_pendiente": "boleta_2_saldo_pendiente",
        "boleta_3_numero": "boleta_3_numero",
        "boleta_3_fecha_emision": "boleta_3_fecha_emision",
        "boleta_3_fecha_vencimiento": "boleta_3_fecha_vencimiento",
        "boleta_3_monto_boleta": "boleta_3_monto_boleta",
        "boleta_3_saldo_pendiente": "boleta_3_saldo_pendiente",
        "boleta_4_numero": "boleta_4_numero",
        "boleta_4_fecha_emision": "boleta_4_fecha_emision",
        "boleta_4_fecha_vencimiento": "boleta_4_fecha_vencimiento",
        "boleta_4_monto_boleta": "boleta_4_monto_boleta",
        "boleta_4_saldo_pendiente": "boleta_4_saldo_pendiente",
        "boleta_5_numero": "boleta_5_numero",
        "boleta_5_fecha_emision": "boleta_5_fecha_emision",
        "boleta_5_fecha_vencimiento": "boleta_5_fecha_vencimiento",
        "boleta_5_monto_boleta": "boleta_5_monto_boleta",
        "boleta_5_saldo_pendiente": "boleta_5_saldo_pendiente",
        "boleta_6_numero": "boleta_6_numero",
        "boleta_6_fecha_emision": "boleta_6_fecha_emision",
        "boleta_6_fecha_vencimiento": "boleta_6_fecha_vencimiento",
        "boleta_6_monto_boleta": "boleta_6_monto_boleta",
        "boleta_6_saldo_pendiente": "boleta_6_saldo_pendiente",
        "tipo_cli": "tipo_cli",
        "plan": "plan",
        "OBSERVACION_CAMPANA": "marca_CHURN",
        "estado_cuenta": "estado_cuenta",
        "fecha_inicio_servicio": "fecha_inicio_servicio",
        "fecha_fin_servicio": "fecha_fin_servicio",
        "TRAMO_EDAD": "tramo_edad",
        "region": "region",
        "comuna": "comuna",
        "cartera": "cartera"
    }
   
    resultado_df = resultado_df[list(nueva_estructura.keys())].rename(columns=nueva_estructura)  
    
    resultado_df["marca_CHURN"] = resultado_df["marca_CHURN"].fillna("NO")
    
    return resultado_df  

def guardar_archivo(fecha_proceso:str, resultado_df:pd.DataFrame):
    ruta = Path("").resolve()
    ruta_sub = Path(f"{ruta}/Salidas/")
    nombre_archivo = f"asignaciones_{fecha_proceso}.csv"
    
    ruta_final = ruta_sub / nombre_archivo
    ruta_final.parent.mkdir(parents=True, exist_ok=True)
    
    
    resultado_df.to_csv(ruta_final, sep="|", index=False)
    print(f"Archivo {nombre_archivo} generado.")
    
    return ruta_final

# ========================================================================
#                             Main
# ========================================================================

def crea_archivo_asignacion(fecha_proceso:str = None):     
    # ========================================================================
    #                           Fecha de Proceso
    # ========================================================================
    if fecha_proceso is None:
        fecha_proceso = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
         
    # ========================================================================
    #                           Extracción de Asignación
    # ========================================================================

    df_asignacion = extrae_asignacion(fecha_proceso)
    
    if not df_asignacion.empty:

        # ========================================================================
        #                            Extracción de Campaña Vista
        # ========================================================================
        
        df_campana_vista = extrae_campana_vista()

        # ========================================================================
        #                           Actualiza Tabla Tramo Edad
        # ========================================================================

        df_tramo = extrae_homologacion_tramo()
        
        # ========================================================================
        #                               Anonimización
        # ========================================================================
    
        df_homologacion_rut = anonimizacion_rut(df_asignacion)

        # ========================================================================
        #                               Combina Consultas
        # ========================================================================

        resultado_df = combinar_dataframes(df_asignacion, df_campana_vista, df_tramo, df_homologacion_rut)

        # ========================================================================
        #                       Estructura Final del Archivo
        # ========================================================================  

        resultado_df = genera_estructura_final(resultado_df)
            
        # ========================================================================
        #                              Guarda el Archivo
        # ========================================================================  
        
        ruta_archivo = guardar_archivo(fecha_proceso, resultado_df)

        # ========================================================================
        #                              Subir Archivo
        # ========================================================================
        
        sube_archivo(ruta_archivo)
    

if __name__ == "__main__":
    clear_screen()
    crea_archivo_asignacion()
    
