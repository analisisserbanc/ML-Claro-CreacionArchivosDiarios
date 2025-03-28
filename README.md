# 📦 ML-Claro-CreacionArchivosDiarios

Este repositorio contiene un conjunto de scripts en Python diseñados para automatizar la generación diaria de archivos utilizados en la operación de *Claro*, incluyendo asignaciones, gestiones, pagos e IDs activos. También contempla el anonimizado de datos sensibles y la carga automática a un bucket de S3.

---

## 🧽 Tabla de contenidos

- [📦 Requisitos](#-requisitos)
- [⚙️ Configuración](#⚙️-configuración)
- [🚀 Ejecución](#-ejecución)
- [📂 Estructura del repositorio](#-estructura-del-repositorio)
- [📌 Notas adicionales](#-notas-adicionales)

---

## 📦 Requisitos

- Python 3.9 o superior
- Las dependencias listadas en `requirements.txt`  
  Puedes instalarlas con:

```bash
pip install -r requirements.txt
```

---

## ⚙️ Configuración

Antes de ejecutar los scripts, asegúrate de tener configurados los siguientes archivos:

### `config/config.yaml`
Contiene parámetros como rutas de entrada/salida, fechas y opciones de ejecución.

### `config/config_credenciales.yaml`
Incluye credenciales para conexión con la base de datos y el bucket S3.  
**⚠️ Este archivo no debe subirse al repositorio (agregado en `.gitignore`).**

---

## 🚀 Ejecución

El flujo principal se ejecuta mediante el script:

```bash
python creacion_archivos_diarios.py
```

Este script:
- Genera los archivos diarios (asignaciones, gestiones, pagos, IDs activos)
- Anonimiza RUTs si corresponde
- Sube los archivos generados a S3

También puedes correr los scripts por separado desde la carpeta `scripts/` para tareas específicas o pruebas.

---

## 📂 Estructura del repositorio

```bash
ML-Claro-CreacionArchivosDiarios/
│
├── config/                         # Archivos de configuración
│   ├── config.yaml
│   ├── config_credenciales.yaml
│   └── config_template.yaml       # Ejemplo de configuración
│
├── scripts/                        # Scripts ejecutables diarios
│   ├── crea_archivo_asignacion_diario.py
│   ├── crea_archivo_gestiones_diario.py
│   ├── crea_archivo_pagos_dia.py
│   ├── crea_archivo_id_activos_diario.py
│   └── cargar_archivos_bucket.py
│
├── core/                           # Módulos reutilizables
│   ├── gestor_archivos.py
│   ├── gestor_bd.py
│   ├── funciones_estandar.py
│   └── utilidades.py
│
├── anonimizacion_rut.py           # Función de anonimización de RUTs
├── creacion_archivos_diarios.py   # Script principal de orquestación
├── simulacion_dias_anteriores.py  # Simulación de ejecución para fechas pasadas
├── requirements.txt               # Dependencias del proyecto
└── README.md                      # Este archivo
```

---

## 📌 Notas adicionales

- El proyecto está diseñado para ejecutarse una vez al día de forma automatizada (por ejemplo, mediante `cron` o `task scheduler`).
- Puedes simular días anteriores con el script `simulacion_dias_anteriores.py`. (Solo en caso de emergencias, o situaciones puntuales)
- La seguridad y anonimización de los datos sensibles está contemplada en `anonimizacion_rut.py`.