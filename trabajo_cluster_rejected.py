"""
================================================================================
PROCESAMIENTO DISTRIBUIDO CON WORKERS EN PYSPARK - DATASET: PRÉSTAMOS RECHAZADOS
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
   spark-submit --master spark://usuario-VirtualBox:7077 trabajo_cluster_rejected.py

5. Detener el cluster al finalizar:
   $SPARK_HOME/sbin/stop-worker.sh
   $SPARK_HOME/sbin/stop-master.sh
--------------------------------------------------------------------------------
"""

import sys
from pyspark.sql import SparkSession

# 1. Configuración de SparkSession conectada al Cluster de Workers
spark = SparkSession.builder \
    .appName("Cluster_Workers_Rejected_Loans") \
    .config("spark.master", "spark://usuario-VirtualBox:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

print("\n>>> Conexión exitosa al Master de Spark. Spark UI activa en http://localhost:4040\n")

# 2. Ruta al archivo de datos (en la carpeta archive)
path_rejected = "archive/rejected_2007_to_2018Q4.csv.gz"

# --- CAPTURA 1: Carga distribuida, conteo y esquema ---
print(">>> [Paso 1] Cargando dataset de rechazados en los workers...")
df = spark.read.options(header='True', inferSchema='True').csv(path_rejected)

# Inspección de particiones iniciales
num_particiones = df.rdd.getNumPartitions()
print(f">>> Número de particiones asignadas automáticamente: {num_particiones}")

# Conteo total distribuido
total_filas = df.count()
print(f">>> Total de registros procesados por los workers: {total_filas}")

# Impresión del esquema
df.printSchema()


# --- OPTIMIZACIÓN Y REPARTICIÓN PARA LOS WORKERS ---
# Repartición equitativa entre los ejecutores
df_reparticionado = df.repartition(4)
print(f">>> Dataset balanceado en {df_reparticionado.rdd.getNumPartitions()} particiones para distribución.")


# --- CAPTURA 2: Selección y filtrado paralelo en los workers ---
print("\n>>> [Paso 2] Ejecutando filtros en paralelo en los workers...")
# Filtrar solicitudes en California ('CA')
df_reparticionado.select(["State"]).filter("`State`='CA'").show(5)

primer_registro = df_reparticionado.select(["State"]).filter("`State`='CA'").first()
print(f">>> Primer registro encontrado: {primer_registro}")

# Filtro compuesto distribuido (Estado CA y Risk_Score > 650)
sesions = df_reparticionado.select(["State", "Risk_Score", "Amount Requested"]).filter("`State`='CA' AND `Risk_Score` > 650")
print(">>> Muestra de solicitudes en 'CA' con Risk_Score > 650:")
sesions.show(5)


# --- CAPTURA 3: Conteo, Distinct y Escritura particionada ---
print("\n>>> [Paso 3] Agrupación distribuida (Distinct) y exportación...")
products = df_reparticionado.select("State")
print(f">>> Total de registros de 'State': {products.count()}")

# Los workers ejecutan el distinct en paralelo
products_distinct = products.distinct()
print(f">>> Total de estados únicos: {products_distinct.count()}")

# Escritura del resultado
products_distinct.write.mode("overwrite").csv("result_cluster_rejected")
print(">>> Resultado guardado exitosamente en 'result_cluster_rejected/'")


# --- CAPTURA 4: Spark SQL sobre el Cluster ---
print("\n>>> [Paso 4] Consultas Spark SQL distribuidas...")
df_reparticionado.createOrReplaceTempView("data")

query = "select `Amount Requested`, `Application Date`, `Loan Title`, `Risk_Score`, `State`, `Employment Length` from data limit 5"
sql_df = spark.sql(query)
sql_df.show()

print("\n>>> Procesamiento completado con éxito en el cluster.")
# spark.stop()
