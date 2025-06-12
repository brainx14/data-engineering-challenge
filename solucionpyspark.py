from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, year, quarter, count, avg, to_timestamp

# 1. Define los esquemas según el enunciado
departments_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("department", StringType(), True)
])

jobs_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("job", StringType(), True)
])

hired_employees_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("datetime", StringType(), True),  # Primero como string
    StructField("department_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True)
])

# 2. Inicia Spark
spark = SparkSession.builder.appName("Globant Data Engineering Challenge").getOrCreate()

# 3. Carga los datos usando los esquemas definidos
departments = spark.read.csv("departments.csv", header=False, schema=departments_schema)
jobs = spark.read.csv("jobs.csv", header=False, schema=jobs_schema)
employees = spark.read.csv("hired_employees.csv", header=False, schema=hired_employees_schema)

# 4. Convierte datetime a Timestamp
employees = employees.withColumn("datetime", to_timestamp("datetime"))

# 5. Filtra empleados contratados en 2021
emp_2021 = employees.filter(year(col("datetime")) == 2021)

# 6. Agrega columna de trimestre
emp_2021 = emp_2021.withColumn("quarter", quarter(col("datetime")))

# 7. Renombra columnas id para evitar ambigüedad al hacer join
departments_renamed = departments.withColumnRenamed("id", "department_id_2")
jobs_renamed = jobs.withColumnRenamed("id", "job_id_2")

# 8. JOINs usando los nuevos nombres
emp_full = emp_2021.join(departments_renamed, emp_2021.department_id == departments_renamed.department_id_2, "left") \
                   .join(jobs_renamed, emp_2021.job_id == jobs_renamed.job_id_2, "left")

# 9. Primer requerimiento: empleados contratados por trabajo y departamento en 2021 dividido por trimestre
hired_by_q = emp_full.groupBy("department", "job", "quarter").agg(count(emp_2021["id"]).alias("count"))
result_q = hired_by_q.groupBy("department", "job") \
    .pivot("quarter", [1, 2, 3, 4]) \
    .sum("count") \
    .orderBy("department", "job") \
    .na.fill(0)

print("Empleados contratados por trabajo y departamento en 2021 por trimestre:")
result_q.show(truncate=False)

# 10. Segundo requerimiento: departamentos que contrataron más que el promedio anual en 2021
dept_hired = emp_2021.groupBy("department_id").agg(count("id").alias("hired"))
mean_hired = dept_hired.agg(avg("hired")).first()[0]
dept_above_mean = dept_hired.filter(col("hired") > mean_hired)
dept_above_mean = dept_above_mean.join(departments_renamed, dept_above_mean.department_id == departments_renamed.department_id_2, "left") \
    .select(dept_above_mean.department_id.alias("id"), "department", "hired") \
    .orderBy(col("hired").desc())

print("Departamentos que contrataron más que el promedio anual en 2021:")
dept_above_mean.show(truncate=False)

# 11. (Opcional) Guarda los resultados a CSV
result_q.coalesce(1).write.csv("reporte_por_trimestre", header=True, mode='overwrite')
dept_above_mean.coalesce(1).write.csv("departamentos_sobresalientes", header=True, mode='overwrite')

spark.stop()
