"""
PySpark Distributed Pipeline (Docker Version)
Processes 10,000 employee records across Docker containers
This version is pre-configured for Docker networking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, max, min, sum as spark_sum, when
from pyspark.sql.types import StringType
from datetime import datetime

def get_salary_level(salary):
    """Categorize salary into levels"""
    if salary < 100000:
        return "Junior"
    elif salary < 200000:
        return "Mid-Level"
    elif salary < 300000:
        return "Senior"
    else:
        return "Executive"

# ============================================================================
# CONFIGURATION FOR DOCKER
# ============================================================================

# With Docker, we use container name "spark-master" instead of IP
# Docker automatically handles networking between containers
MASTER_HOST = "spark-master"
MASTER_PORT = "7077"
APP_NAME = "Employee_Data_Pipeline_Docker"

# ============================================================================
# STEP 1: CREATE SPARK SESSION (CONNECTION TO CLUSTER)
# ============================================================================

print("\n" + "="*70)
print("🚀 STARTING PYSPARK DISTRIBUTED PIPELINE (Docker Version)")
print("="*70)

print(f"\n📡 Connecting to Spark cluster at spark://{MASTER_HOST}:{MASTER_PORT}...")

spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master(f"spark://{MASTER_HOST}:{MASTER_PORT}") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "8") \
    .config("spark.default.parallelism", "8") \
    .getOrCreate()

print("✓ Spark Session created successfully!")
print(f"✓ App Name: {APP_NAME}")
print(f"✓ Master: spark://{MASTER_HOST}:{MASTER_PORT}")
print("✓ Running inside Docker containers!")

# ============================================================================
# STEP 2: READ DATA FROM CSV FILE
# ============================================================================

print("\n" + "-"*70)
print("STEP 1: LOADING DATA")
print("-"*70)

print(f"\n📂 Reading data from: /app/data/data.csv")

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/app/data/data.csv")

total_rows = df.count()
print(f"✓ Data loaded successfully!")
print(f"✓ Total rows: {total_rows:,}")
print(f"✓ Columns: {', '.join(df.columns)}")

# ============================================================================
# STEP 3: PARTITION DATA FOR PARALLEL PROCESSING
# ============================================================================

print("\n" + "-"*70)
print("STEP 2: PARTITIONING DATA FOR PARALLEL PROCESSING")
print("-"*70)

# Repartition into 4 partitions
# Each partition will be processed by a different worker
df_partitioned = df.repartition(4)

num_partitions = df_partitioned.rdd.getNumPartitions()
print(f"\n✓ Data split into {num_partitions} partitions")
print(f"✓ Each partition ≈ {total_rows // num_partitions:,} rows")
print(f"✓ Workers will process these partitions SIMULTANEOUSLY")

# Show sample data
print("\n📋 Sample of Original Data (First 5 rows):")
df_partitioned.show(5, truncate=False)

# ============================================================================
# STEP 4: TRANSFORMATION 1 - ADD SALARY LEVEL COLUMN
# ============================================================================

print("\n" + "-"*70)
print("STEP 3: TRANSFORMATION - ADD SALARY LEVEL COLUMN")
print("-"*70)

from pyspark.sql.functions import udf

# Create UDF (User Defined Function) for salary level
salary_level_udf = udf(get_salary_level, StringType())

df_with_level = df_partitioned.withColumn(
    "salary_level", 
    salary_level_udf(col("salary"))
)

print("\n✓ Added 'salary_level' column based on salary")
print("✓ Salary levels: Junior (<100k), Mid-Level (100k-200k), Senior (200k-300k), Executive (300k+)")

print("\n📋 Data with Salary Levels (First 8 rows):")
df_with_level.select("id", "name", "salary", "salary_level").show(8, truncate=False)

# ============================================================================
# STEP 5: TRANSFORMATION 2 - FILTER HIGH EARNERS
# ============================================================================

print("\n" + "-"*70)
print("STEP 4: TRANSFORMATION - FILTER HIGH EARNERS")
print("-"*70)

# Find all employees earning more than 500,000
high_earners = df_with_level.filter(col("salary") > 500000)
high_earner_count = high_earners.count()

print(f"\n✓ Employees with salary > $500,000: {high_earner_count:,}")

if high_earner_count > 0:
    print("\n📋 High Earners (Sample):")
    high_earners.select("name", "salary", "department", "salary_level").show(5, truncate=False)
else:
    print("   (No employees in this salary range)")

# ============================================================================
# STEP 6: TRANSFORMATION 3 - CALCULATE STATISTICS BY DEPARTMENT
# ============================================================================

print("\n" + "-"*70)
print("STEP 5: TRANSFORMATION - DEPARTMENT STATISTICS")
print("-"*70)

# Group by department and calculate statistics
dept_stats = df_with_level.groupBy("department").agg(
    count("*").alias("employee_count"),
    avg("salary").alias("avg_salary"),
    min("salary").alias("min_salary"),
    max("salary").alias("max_salary"),
    spark_sum("salary").alias("total_salary")
).orderBy("department")

print("\n✓ Calculated statistics grouped by department")
print("📊 Department Statistics:")
dept_stats.show(truncate=False)

# ============================================================================
# STEP 7: TRANSFORMATION 4 - SALARY LEVEL DISTRIBUTION
# ============================================================================

print("\n" + "-"*70)
print("STEP 6: TRANSFORMATION - SALARY LEVEL DISTRIBUTION")
print("-"*70)

level_distribution = df_with_level.groupBy("salary_level").agg(
    count("*").alias("count"),
    avg("salary").alias("avg_salary")
).orderBy(
    when(col("salary_level") == "Junior", 1)
    .when(col("salary_level") == "Mid-Level", 2)
    .when(col("salary_level") == "Senior", 3)
    .otherwise(4)
)

print("\n✓ Calculated salary level distribution")
print("📊 Distribution of Salary Levels:")
level_distribution.show(truncate=False)

# ============================================================================
# STEP 8: TRANSFORMATION 5 - RECENT HIRES (Last 5 years)
# ============================================================================

print("\n" + "-"*70)
print("STEP 7: TRANSFORMATION - RECENT HIRES")
print("-"*70)

current_year = 2024  # You can change this
recent_hires = df_with_level.filter(col("join_year") >= (current_year - 5))
recent_hire_count = recent_hires.count()

print(f"\n✓ Employees hired in last 5 years (2019-2024): {recent_hire_count:,}")

# Recent hires by department
recent_by_dept = recent_hires.groupBy("department").agg(
    count("*").alias("recent_hires")
).orderBy("recent_hires")

print("\n📊 Recent Hires by Department:")
recent_by_dept.show(truncate=False)

# ============================================================================
# STEP 9: SAVE RESULTS TO OUTPUT FOLDER
# ============================================================================

print("\n" + "-"*70)
print("STEP 8: SAVING RESULTS")
print("-"*70)

# Define output paths (inside Docker container)
output_paths = {
    "enriched_data": "/app/output/enriched_employees",
    "dept_stats": "/app/output/department_statistics",
    "level_dist": "/app/output/salary_level_distribution"
}

print("\n📁 Saving results to output folders...\n")

# Save enriched data
df_with_level.coalesce(1).write.mode("overwrite").csv(
    output_paths["enriched_data"], 
    header=True
)
print(f"✓ Saved enriched employee data to: {output_paths['enriched_data']}/")

# Save department statistics
dept_stats.coalesce(1).write.mode("overwrite").csv(
    output_paths["dept_stats"], 
    header=True
)
print(f"✓ Saved department statistics to: {output_paths['dept_stats']}/")

# Save salary level distribution
level_distribution.coalesce(1).write.mode("overwrite").csv(
    output_paths["level_dist"], 
    header=True
)
print(f"✓ Saved salary level distribution to: {output_paths['level_dist']}/")

# ============================================================================
# STEP 10: FINAL SUMMARY
# ============================================================================

print("\n" + "="*70)
print("📊 FINAL SUMMARY")
print("="*70)

total_salary_expense = df_with_level.agg(spark_sum("salary")).collect()[0][0]
avg_salary = df_with_level.agg(avg("salary")).collect()[0][0]
max_salary = df_with_level.agg(max("salary")).collect()[0][0]
min_salary = df_with_level.agg(min("salary")).collect()[0][0]

print(f"\n👥 Total Employees: {total_rows:,}")
print(f"💰 Total Salary Expense: ${total_salary_expense:,.0f}")
print(f"📈 Average Salary: ${avg_salary:,.0f}")
print(f"📊 Max Salary: ${max_salary:,.0f}")
print(f"📉 Min Salary: ${min_salary:,.0f}")
print(f"\n🎯 Processing completed on {num_partitions} partitions")
print(f"⚡ Data processed SIMULTANEOUSLY inside Docker containers!")

# ============================================================================
# CLOSE SPARK SESSION
# ============================================================================

spark.stop()

print("\n" + "="*70)
print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
print("="*70)
print(f"\n📂 Results saved in Docker container at: /app/output/")
print(f"📂 On Windows, check: C:\\pyspark-project\\output\\")
print(f"🔗 Master Dashboard: http://localhost:8080")
print(f"📊 Job Status: http://localhost:4040 (while running)\n")