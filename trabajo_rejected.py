from pyspark.sql import SparkSession

# ==============================================================================
# TRABAJO PYSPARK - DATASET 2: PRÉSTAMOS RECHAZADOS (rejected_2007_to_2018Q4)
# Código adaptado fielmente a partir de las 4 capturas proporcionadas
# ==============================================================================

# 0. Inicialización de SparkSession (necesario si se ejecuta como script .py)
spark = SparkSession.builder \
    .appName("Trabajo_Spark_Rejected_Loans") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Ruta al archivo original comprimido
path_rejected = "rejected_2007_to_2018Q4.csv.gz"

# --- Captura 1: Carga de datos, conteo y esquema ---
df = spark.read.options(header='True', inferSchema='True').csv(path_rejected)

# Conteo total de registros
df.count()

# Impresión del esquema de columnas y tipos de datos
df.printSchema()


# --- Captura 2: Selección y filtrado de registros ---
# Filtrado equivalente a: event_type='cart'
# Usamos la columna 'State' para filtrar los de California ('CA')
# (Nota: Se usan acentos graves ` ` si el nombre de columna contiene espacios o caracteres especiales)
df.select(["State"]).filter("`State`='CA'").show()

# Obtención del primer registro coincidente
primer_registro = df.select(["State"]).filter("`State`='CA'").first()
print(primer_registro)

# Filtro compuesto con operador AND
# Equivalente a: filter("event_type='cart' AND product_id=5844305")
# Filtramos solicitudes en el estado 'CA' con un Risk_Score superior a 650
sesions = df.select(["State", "Risk_Score", "Amount Requested"]).filter("`State`='CA' AND `Risk_Score` > 650")
sesions.show(5)


# --- Captura 3: Distinct, conteos y exportación a CSV ---
# Extraemos una columna categórica (en el ejemplo original era 'product_id', aquí usamos 'State')
products = df.select("State")

# Conteo total antes del distinct
products.select("State").count()

# Obtención de valores únicos
products = products.select("State").distinct()

# Conteo de valores únicos
products.select("State").count()

# Escritura del resultado en formato CSV sobrescribiendo si ya existe
products.write.mode("overwrite").csv("result_rejected")


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
