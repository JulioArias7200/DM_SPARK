from pyspark.sql import SparkSession

# ==============================================================================
# TRABAJO PYSPARK - DATASET 1: PRÉSTAMOS ACEPTADOS (accepted_2007_to_2018Q4)
# Código adaptado fielmente a partir de las 4 capturas proporcionadas
# ==============================================================================

# 0. Inicialización de SparkSession (necesario si se ejecuta como script .py)
spark = SparkSession.builder \
    .appName("Trabajo_Spark_Accepted_Loans") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Ruta al archivo (puede ser la carpeta con el csv o el archivo directo)
path_accepted = "accepted_2007_to_2018q4.csv/*.csv"

# --- Captura 1: Carga de datos, conteo y esquema ---
df = spark.read.options(header='True', inferSchema='True').csv(path_accepted)

# Conteo total de registros
df.count()

# Impresión del esquema de columnas y tipos de datos
df.printSchema()


# --- Captura 2: Selección y filtrado de registros ---
# Filtrado equivalente a: event_type='cart'
# Usamos 'loan_status' y seleccionamos 'id'
df.select(["id"]).filter("loan_status='Fully Paid'").show()

# Obtención del primer registro coincidente
primer_registro = df.select(["id"]).filter("loan_status='Fully Paid'").first()
print(primer_registro)

# Filtro compuesto con operador AND
# Equivalente a: filter("event_type='cart' AND product_id=5844305")
# Filtramos préstamos pagados por completo ('Fully Paid') cuyo propósito sea 'debt_consolidation'
sesions = df.select(["id", "loan_amnt", "term"]).filter("loan_status='Fully Paid' AND purpose='debt_consolidation'")
sesions.show(5)


# --- Captura 3: Distinct, conteos y exportación a CSV ---
# Extraemos una columna categórica (en el ejemplo original era 'product_id', aquí usamos 'purpose')
products = df.select("purpose")

# Conteo total antes del distinct
products.select("purpose").count()

# Obtención de valores únicos
products = products.select("purpose").distinct()

# Conteo de valores únicos
products.select("purpose").count()

# Escritura del resultado en formato CSV sobrescribiendo si ya existe
products.write.mode("overwrite").csv("result_accepted")


# --- Captura 4: Vista temporal y consultas Spark SQL ---
# Registro de vista temporal en memoria llamada 'data'
df.createOrReplaceTempView("data")

# Consulta SQL retornando el DataFrame (como en la captura)
sql_df = spark.sql("select * from data limit 3")

# Segunda consulta (DataFrame generado)
spark.sql("select * from data limit 3")

# Muestra del resultado tabular con .show()
spark.sql("select * from data limit 3").show()

# Detener sesión si se corre como script
# spark.stop()
