# Guía de Ejecución en PySpark

Este documento detalla el paso a paso para ejecutar los comandos en **PySpark** basados en el flujo de trabajo de las 4 capturas, adaptados a los dos datasets ubicados en la carpeta `DM_SPARK`:

1. **Dataset 1 (Préstamos Aceptados):** `accepted_2007_to_2018q4.csv/accepted_2007_to_2018Q4.csv`
2. **Dataset 2 (Préstamos Rechazados):** `rejected_2007_to_2018q4.csv/rejected_2007_to_2018Q4.csv`

---

## Modos de Ejecución

Existen dos formas principales de ejecutar estos comandos:

### Opción A: En la Consola Interactiva de PySpark (Recomendado para ir línea por línea como en las capturas)
1. Abre tu terminal (PowerShell, CMD o Bash) y colócate en la carpeta del proyecto:
   ```bash
   cd c:\Users\julio\Downloads\INF_2_2026\DM\spark\expo\DM_SPARK
   ```
2. Inicia la consola de PySpark (se recomienda asignar al menos 4 GB de memoria de driver por el tamaño de los datasets):
   ```bash
   pyspark --driver-memory 4g
   ```
   *(En este modo, la variable `spark` ya se encuentra creada automáticamente).*
3. Copia y pega los comandos de cada sección.

### Opción B: Ejecutar como Script Python Completo
Se han preparado dos scripts listos para su ejecución directa:
```bash
python trabajo_accepted.py
python trabajo_rejected.py
```
O usando `spark-submit`:
```bash
spark-submit --driver-memory 4g trabajo_accepted.py
spark-submit --driver-memory 4g trabajo_rejected.py
```

---

## 1. Dataset 1: Préstamos Aceptados (`accepted`)

### Paso 1: Carga, conteo y esquema (Captura 1)
```python
# Carga del CSV infiriendo el esquema y leyendo encabezados
df = spark.read.options(header='True', inferSchema='True').csv('accepted_2007_to_2018q4.csv/*.csv')

# Conteo total de filas
df.count()

# Impresión de la estructura y tipos de datos
df.printSchema()
```

### Paso 2: Selección y filtrado de registros (Captura 2)
```python
# Filtrar registros por estado del préstamo ('loan_status') y mostrar
df.select(["id"]).filter("loan_status='Fully Paid'").show()

# Obtener el primer registro coincidente
df.select(["id"]).filter("loan_status='Fully Paid'").first()

# Filtro compuesto con operador AND (préstamos pagados para consolidación de deuda)
sesions = df.select(["id", "loan_amnt", "term"]).filter("loan_status='Fully Paid' AND purpose='debt_consolidation'")
```

### Paso 3: Selección de columna, valores únicos y exportación a CSV (Captura 3)
```python
# Seleccionar la columna 'purpose' (propósito del préstamo)
products = df.select("purpose")

# Conteo total de elementos
products.select("purpose").count()

# Obtener registros únicos (distinct)
products = products.select("purpose").distinct()

# Conteo tras aplicar distinct
products.select("purpose").count()

# Guardar el resultado en formato CSV sobrescribiendo si ya existe
products.write.mode("overwrite").csv('result_accepted')
```

### Paso 4: Creación de vista temporal y consultas Spark SQL (Captura 4)
```python
# Registrar DataFrame como vista SQL temporal
df.createOrReplaceTempView("data")

# Ejecución de la consulta SQL retornando el DataFrame
spark.sql("select * from data limit 3")

# Segunda llamada
spark.sql("select * from data limit 3")

# Visualizar los resultados tabulares en consola
spark.sql("select * from data limit 3").show()
```

---

## 2. Dataset 2: Préstamos Rechazados (`rejected`)

> **Nota sobre nombres de columnas:** En este dataset, algunas columnas contienen espacios (por ejemplo: `Amount Requested`, `Loan Title`, `Risk_Score`, `State`). En PySpark SQL y expresiones de filtro se usan acentos graves (``` `Columna` ```) para referenciarlas correctamente.

### Paso 1: Carga, conteo y esquema (Captura 1)
```python
# Carga del CSV de préstamos rechazados
df = spark.read.options(header='True', inferSchema='True').csv('rejected_2007_to_2018q4.csv/*.csv')

# Conteo total de registros
df.count()

# Impresión del esquema
df.printSchema()
```

### Paso 2: Selección y filtrado de registros (Captura 2)
```python
# Filtrar solicitudes correspondientes al estado de California ('CA')
df.select(["State"]).filter("`State`='CA'").show()

# Obtener el primer registro coincidente
df.select(["State"]).filter("`State`='CA'").first()

# Filtro compuesto con operador AND (solicitudes de CA con Risk_Score > 650)
sesions = df.select(["State", "Risk_Score", "Amount Requested"]).filter("`State`='CA' AND `Risk_Score` > 650")
```

### Paso 3: Selección de columna, valores únicos y exportación a CSV (Captura 3)
```python
# Seleccionar la columna 'State'
products = df.select("State")

# Conteo total antes del distinct
products.select("State").count()

# Obtener estados únicos
products = products.select("State").distinct()

# Conteo de estados únicos
products.select("State").count()

# Guardar el resultado en formato CSV sobrescribiendo
products.write.mode("overwrite").csv('result_rejected')
```

### Paso 4: Creación de vista temporal y consultas Spark SQL (Captura 4)
```python
# Registrar vista SQL temporal
df.createOrReplaceTempView("data")

# Consulta SQL retornando el DataFrame
spark.sql("select * from data limit 3")

# Segunda consulta
spark.sql("select * from data limit 3")

# Visualizar los primeros 3 registros tabulares
spark.sql("select * from data limit 3").show()
```

---

## Archivos Generados en el Repositorio

- [trabajo_accepted.py](file:///c:/Users/julio/Downloads/INF_2_2026/DM/spark/expo/DM_SPARK/trabajo_accepted.py): Script en Python con el flujo completo para el dataset de aceptados.
- [trabajo_rejected.py](file:///c:/Users/julio/Downloads/INF_2_2026/DM/spark/expo/DM_SPARK/trabajo_rejected.py): Script en Python con el flujo completo para el dataset de rechazados.
- [PASOS_EJECUCION_PYSPARK.md](file:///c:/Users/julio/Downloads/INF_2_2026/DM/spark/expo/DM_SPARK/PASOS_EJECUCION_PYSPARK.md): Esta guía detallada con los pasos y comandos para cada entorno.
