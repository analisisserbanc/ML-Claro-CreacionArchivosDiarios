# ML-Claro-CreacionArchivosDiarios

Este repositorio contiene una colección de scripts en Python diseñados para la generación diaria de archivos específicos para el proyecto ML-Claro. El proceso principal se lleva a cabo mediante el script `creacion_archivos_diarios.py`, que debe ejecutarse diariamente. Los archivos generados se cargan automáticamente en un bucket de Amazon S3, utilizando las credenciales proporcionadas en el archivo `config_credenciales.yaml`.

## Estructura del Repositorio

A continuación, se detallan los archivos y directorios principales del repositorio:

* `anonimizacion_rut.py`: Contiene funciones para anonimizar los RUTs presentes en los datos.
* `cargar_archivos_bucket.py`: Maneja la carga de los archivos generados al bucket de S3.
* `config.yaml`: Archivo de configuración con parámetros utilizados por los scripts.
* `config_credenciales.yaml`: Archivo que almacena las credenciales necesarias para la conexión con S3.
* `crea_archivo_asignacion_diario.py`: Genera el archivo diario de asignaciones.
* `crea_archivo_gestiones_diario.py`: Genera el archivo diario de gestiones.
* `crea_archivo_id_activos_diario.py`: Genera el archivo diario de IDs activos.
* `crea_archivo_pagos_dia.py`: Genera el archivo diario de pagos.
* `creacion_archivos_diarios.py`: Script principal que coordina la creación diaria de los archivos necesarios.
* `funciones_estandar.py`: Contiene funciones auxiliares utilizadas en varios scripts.
* `gestor_archivos.py`: Gestiona las operaciones relacionadas con archivos, como lectura y escritura.
* `gestor_bd.py`: Maneja las conexiones y operaciones con la base de datos.
* `simulacion_dias_anteriores.py`: Permite la simulación de la generación de archivos para días anteriores.
* `utilidades.py`: Incluye funciones utilitarias adicionales.

## Requisitos Previos

Antes de ejecutar los scripts, asegúrese de tener instaladas las siguientes dependencias:

* Python 3.x
* Bibliotecas adicionales listadas en `requirements.txt` (si corresponde)

Además, es necesario contar con el archivo `config_credenciales.yaml` en el directorio raíz del proyecto. Este archivo debe contener las credenciales necesarias para cargar los archivos generados en el bucket de S3.