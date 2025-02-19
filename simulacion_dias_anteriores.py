from crea_archivo_gestiones_diario import generar_archivo_gestiones
from crea_archivo_pagos_dia import crea_archivo_pagos
from crea_archivo_asignacion_diario import crea_archivo_asignacion
from crea_archivo_id_activos_diario import crea_archivo_id_activos
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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

def procesa_periodo(periodo:int):
    lista_dias = generar_dias_mes_periodo(periodo)
    
    for dia in lista_dias:   
        generar_archivo_gestiones(dia)
        crea_archivo_pagos(dia)
        crea_archivo_asignacion(dia)
        crea_archivo_id_activos(dia)
    
def main():
    procesa_periodo(202502)



if __name__ == "__main__":
    main()