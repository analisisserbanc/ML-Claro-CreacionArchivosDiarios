import MySQLdb
import pandas as pd

def connect_to_database(servidor:int = 23, database:str = None) -> MySQLdb:
    credenciales = {23 : "33+O4O$*9=oV", 72 : 'JO.,2024me'}

    if database is None:
        lista_databases = {23: "report_cartera", 72: "CLARO"}
        database = lista_databases[servidor]

    contraseña = credenciales[servidor]

    config = {
        'host': f'172.16.10.{servidor}',
        'user': 'jriquelme',
        'password': contraseña,
        'database': database,
        'port': 3306,
        'charset': 'utf8mb4',
        'connect_timeout': 1800,
        'local_infile':1 
    }
    return MySQLdb.connect(**config)    

def consulta_a_df(consulta:str, servidor:int = 23, database:str = 'report_cartera'):
    consulta = consulta.replace("\n"," ").replace("          "," ").replace("     ", " ").replace("     ", " ").replace("    "," ").replace("   ", " ")
    
    try:
        conexion = connect_to_database(servidor, database)
        df = pd.read_sql_query(consulta, conexion)
        return df
    except MySQLdb.Error as error:
        print(f"Error al conectar con la base de datos: {error}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conexion' in locals() and conexion.open:
            conexion.close()

def limpiar_tabla(tabla, servidor:int = 23):
    try:        
        consulta = f"TRUNCATE TABLE {tabla}"        
        ejecutar_consulta(consulta, servidor = servidor)
        
    except MySQLdb.Error as error:
        print(f"Error al limpiar la tabla: {error}")
        
def ejecutar_consulta(consulta, servidor:int = 23):
    try:        
        conn = connect_to_database(servidor=servidor)
        
        cursor = conn.cursor()
        
        # Preparar la sentencia SQL dinámica
        sql = consulta
        
        # Ejecutar la sentencia
        cursor.execute(sql)
        
        # Confirmar los cambios
        conn.commit()
        
    except MySQLdb.Error as error:
        print(f"Error al ejecutar la consulta: {error}")
        
    finally:
        cursor.close()
        conn.close()

def carga_datos_desde_csv(ruta_archivo, tabla, limpiar: bool = False, servidor: int = 23, database: str = None, mensaje=True):
    try:
        conn = connect_to_database(servidor=servidor, database=database)
        cursor = conn.cursor()

        ruta_archivo_str = str(ruta_archivo).replace('\\', '/')
        
        if limpiar:
            limpiar_tabla(tabla)        
        
        # Cargar los datos inicialmente sin modificar las columnas
        sql = f"""
        LOAD DATA LOCAL INFILE '{ruta_archivo_str}' INTO TABLE {tabla}
        FIELDS TERMINATED BY ';' 
        LINES TERMINATED BY '\\n' 
        IGNORE 1 LINES;
        """
        
        cursor.execute(sql)
        conn.commit()

        # Identificar columnas tipo VARCHAR en la tabla
        cursor.execute(f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{tabla}' AND TABLE_SCHEMA = '{database}' AND DATA_TYPE = 'varchar';
        """)
        
        columnas_varchar = cursor.fetchall()
        
        # Limpiar el \r de las columnas tipo VARCHAR
        for (columna,) in columnas_varchar:
            cursor.execute(f"UPDATE {tabla} SET `{columna}` = REPLACE(`{columna}`, '\\r', '');")
            conn.commit()

        if mensaje:
            print(f"Archivo {ruta_archivo_str} cargado exitosamente en {tabla} y columnas VARCHAR limpiadas.")
        return True

    except MySQLdb.Error as error:
        print(f"Error al cargar datos: {error}")
        return False

    finally:
        cursor.close()
        conn.close()
