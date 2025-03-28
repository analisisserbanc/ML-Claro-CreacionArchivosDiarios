import os 
from datetime import datetime

def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


def es_fecha_valida(fecha_entrada: str | int) -> bool:
    """Valida si un string tiene formato YYYYMMDD y representa una fecha válida."""
    fecha_entrada = str(fecha_entrada)
    
    if len(fecha_entrada) != 8 or not fecha_entrada.isdigit():
        return False

    año = int(fecha_entrada[:4])
    mes = int(fecha_entrada[4:6])
    dia = int(fecha_entrada[6:])

    try:
        datetime(año, mes, dia)
        return True
    except ValueError:
        return False        