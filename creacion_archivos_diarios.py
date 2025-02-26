from funciones_estandar import clear_screen
from crea_archivo_pagos_dia import crea_archivo_pagos
from crea_archivo_gestiones_diario import generar_archivo_gestiones
from crea_archivo_asignacion_diario import crea_archivo_asignacion
from crea_archivo_id_activos_diario import crea_archivo_id_activos


def main(fecha_proceso:str|int = None):
    clear_screen()
    crea_archivo_pagos(fecha_proceso)
    generar_archivo_gestiones(fecha_proceso)
    crea_archivo_asignacion(fecha_proceso)
    crea_archivo_id_activos(fecha_proceso)
    
    
if __name__ == "__main__":
    main()