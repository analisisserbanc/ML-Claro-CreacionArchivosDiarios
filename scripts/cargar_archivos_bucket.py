# ========================================================================
#                          Configuración Inicial
# ========================================================================

import boto3 
import json
import yaml
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE_PATH = BASE_DIR / 'config' / 'config.yaml'



with open(CONFIG_FILE_PATH, 'r') as file:
    config = yaml.safe_load(file)

cred_file = config['credentials']['file']
prefix = config['credentials']['prefix']
bucket_name = config['storage']['bucket_name']
output_dir = Path(config['paths']['output_dir'])

# ========================================================================
#                                JSON / S3 
# ========================================================================

def extrae_keys_json(nombre_archivo:str):
    with open(nombre_archivo) as config_file:
        config = json.load(config_file)
        
    prod_key_id = config['prod']['key_id']
    prod_secret_key = config['prod']['secret_key']
    
    return prod_key_id, prod_secret_key

def configura_cliente_s3(nombre_archivo_json:str):
    
    prod_key_id, prod_secret_key = extrae_keys_json(nombre_archivo_json)
    
    # Configurar el cliente de S3
    s3 = boto3.client('s3',
                    aws_access_key_id     = prod_key_id,
                    aws_secret_access_key = prod_secret_key)
    
    return s3

# ========================================================================
#                          Funciones Auxiliares
# ========================================================================

def cargar_archivo_en_s3(nombre_archivo, ruta_local):
       
    s3 = configura_cliente_s3(cred_file)

    object_key = prefix + nombre_archivo # Construir la clave del objeto (ruta completa en S3)
    
    try:
        # Cargar el archivo en S3
        s3.upload_file(ruta_local, bucket_name, object_key)
        print(f"Archivo {nombre_archivo} cargado exitosamente en {bucket_name}/{object_key}\n")
    
    except Exception as e:
        print(f"Error al cargar el archivo: {str(e)}\n")

if __name__ == "__main__":
    ...

    