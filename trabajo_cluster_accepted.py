"""
================================================================================
PROCESAMIENTO DISTRIBUIDO CON WORKERS EN PYSPARK - DATASET: PRÉSTAMOS ACEPTADOS
================================================================================

PASOS PREVIOS EN CONSOLA LINUX:
--------------------------------------------------------------------------------
1. Iniciar Master de Spark (desde cualquier carpeta):
   $SPARK_HOME/sbin/start-master.sh

2. Iniciar Worker(s) conectándolos al Master:
   $SPARK_HOME/sbin/start-worker.sh spark://usuario-VirtualBox:7077
   (o con recursos: $SPARK_HOME/sbin/start-worker.sh -c 2 -m 2G spark://usuario-VirtualBox:7077)

3. Verificar en el navegador:
   http://localhost:8080  -> Web UI del Master (verificar workers en estado ALIVE)

4. Ejecución del script:
   spark-submit --master spark://usuario-VirtualBox:7077 trabajo_cluster_accepted.py

5. Detener el cluster al finalizar:
   $SPARK_HOME/sbin/stop-worker.sh
   $SPARK_HOME/sbin/stop-master.sh
--------------------------------------------------------------------------------
"""

import sys
from pyspark.sql import SparkSession

# 1. Configuración de SparkSession conectada al Cluster de Workers
# Permitimos que Spark adapte la memoria y núcleos dinámicamente según la máquina virtual
spark = SparkSession.builder \
    .appName("Cluster_Workers_Accepted_Loans") \
    .config("spark.master", "spark://usuario-VirtualBox:7077") \
    .getOrCreate()

print("\n>>> Conexión exitosa al Master de Spark. Spark UI activa en http://localhost:4040\n")

# 2. Ruta al archivo de datos (en la carpeta archive)
path_accepted = "archive/accepted_2007_to_2018Q4.csv.gz"

# --- CAPTURA 1: Carga distribuida, conteo y esquema ---
print(">>> [Paso 1] Cargando dataset distribuido en los workers...")
df = spark.read.options(header='True', inferSchema='True').csv(path_accepted)

# Inspección de particiones (cómo se divide el trabajo entre los workers)
num_particiones_inicial = df.rdd.getNumPartitions()
print(f">>> Número de particiones asignadas automáticamente: {num_particiones_inicial}")

# Conteo total distribuido
total_filas = df.count()
print(f">>> Total de registros procesados por los workers: {total_filas}")

# Impresión del esquema
df.printSchema()


# --- OPTIMIZACIÓN Y REPARTICIÓN PARA LOS WORKERS ---
# Reparticionamos los datos para balancear la carga uniformemente entre los núcleos de los workers
num_workers_cores = 4  # Ajustar según la cantidad de núcleos disponibles
df_reparticionado = df.repartition(num_workers_cores)
print(f">>> Dataset balanceado en {df_reparticionado.rdd.getNumPartitions()} particiones para distribución.")


# --- CAPTURA 2: Selección y filtrado paralelo en los workers ---
print("\n>>> [Paso 2] Ejecutando filtros en paralelo en los workers...")
# Filtrar préstamos pagados por completo
df_reparticionado.select(["id"]).filter("loan_status='Fully Paid'").show(5)

primer_registro = df_reparticionado.select(["id"]).filter("loan_status='Fully Paid'").first()
print(f">>> Primer registro encontrado: {primer_registro}")

# Filtro compuesto distribuido
sesions = df_reparticionado.select(["id", "loan_amnt", "term"]).filter("loan_status='Fully Paid' AND purpose='debt_consolidation'")
print(">>> Muestra de préstamos 'Fully Paid' y 'debt_consolidation':")
sesions.show(5)


# --- CAPTURA 3: Conteo, Distinct y Escritura particionada ---
print("\n>>> [Paso 3] Agrupación distribuida (Distinct) y exportación...")
products = df_reparticionado.select("purpose")
print(f">>> Total de registros de 'purpose': {products.count()}")

# Los workers ejecutan el shuffle y distinct en paralelo
products_distinct = products.distinct()
print(f">>> Total de propósitos únicos: {products_distinct.count()}")

# Escritura del resultado: cada worker escribe una parte del archivo en paralelo
products_distinct.write.mode("overwrite").csv("result_cluster_accepted")
print(">>> Resultado guardado exitosamente en 'result_cluster_accepted/'")


# --- CAPTURA 4: Spark SQL sobre el Cluster ---
print("\n>>> [Paso 4] Consultas Spark SQL distribuidas...")
df_reparticionado.createOrReplaceTempView("data")

query = "select id, loan_amnt, term, int_rate, grade, loan_status, purpose, addr_state from data limit 5"
sql_df = spark.sql(query)
sql_df.show()

print("\n>>> Procesamiento completado con éxito en el cluster.")
# spark.stop()
