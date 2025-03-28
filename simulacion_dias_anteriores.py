from scripts.crea_archivo_gestiones_diario import generar_archivo_gestiones
from scripts.crea_archivo_pagos_dia import crea_archivo_pagos
from scripts.crea_archivo_asignacion_diario import crea_archivo_asignacion
from scripts.crea_archivo_id_activos_diario import crea_archivo_id_activos
from core.funciones_estandar import clear_screen
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import argparse

def generar_dias_mes_periodo(periodo: int) -> list[str]:
    año = int(str(periodo)[:4])
    mes = int(str(periodo)[-2:])
    
    primer_dia = datetime(año, mes, 1)
    ultimo_dia_mes = primer_dia + relativedelta(months=1) - timedelta(days=1)
    
    # Si el periodo corresponde al mes actual, ajustar al día actual si es necesario
    hoy = datetime.today()
    ultimo_dia = min(ultimo_dia_mes, hoy) if hoy.year == año and hoy.month == mes else ultimo_dia_mes

    dias = []
    dia = primer_dia

    while dia <= ultimo_dia:
        dias.append(dia.strftime("%Y%m%d"))
        dia += timedelta(days=1)
        
    return dias

def procesa_periodo(periodo: int, archivo: str = "todos"):
    clear_screen()
    lista_dias = generar_dias_mes_periodo(periodo)
  
    for dia in lista_dias:
        print(dia)
        if archivo in ["gestiones", "todos"]:
            generar_archivo_gestiones(dia)
        if archivo in ["pagos", "todos"]:
            crea_archivo_pagos(dia)
        if archivo in ["asignacion", "todos"]:
            crea_archivo_asignacion(dia)
        if archivo in ["id_activos", "todos"]:
            crea_archivo_id_activos(dia)
    
def validar_periodo(valor):
    """Verifica que el período tenga exactamente 6 dígitos y sea un número entero en formato yyyymm."""
    if not valor.isdigit() or len(valor) != 6:
        raise argparse.ArgumentTypeError("El período debe ser un número entero de 6 dígitos en formato yyyymm.")
    
    return int(valor)  # Convertimos a entero si pasa la validación
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesa un período y genera archivos.")
    
    parser.add_argument("periodo", type=int, help="Período en formato yyyymm (ejemplo: 202402)")
    parser.add_argument("--archivo", choices=["gestiones", "pagos", "asignacion", "id_activos", "todos"],
                        default="todos", help="Tipo de archivo a generar (por defecto genera todos)")

    args = parser.parse_args()

    procesa_periodo(args.periodo, args.archivo)
