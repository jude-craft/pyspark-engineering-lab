# =============================================================================
# MILESTONE 01 — Spark Fundamentals
# =============================================================================
# Topics Covered:
#   - Reading data from a Spark table (JSON / CSV)
#   - Flattening nested JSON structures
#   - Inspecting a DataFrame (count, columns, schema)
#   - Defining schemas with DDL strings and StructType / StructField
#   - Loading CSV with a custom schema
#
# Dataset: dbacademy.default.big_mart_sales  |  dbacademy.default.drivers
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)
from pyspark.sql.functions import col, concat_ws


# -----------------------------------------------------------------------------
# 1. Data Reading — JSON (nested structure)
# -----------------------------------------------------------------------------
# Raw file: data/raw/drivers.json  (853 F1 driver records, newline-delimited JSON)
#
# Upload options:
#   A) Unity Catalog Volume  → /Volumes/<catalog>/<schema>/raw_data/drivers.json
#   B) DBFS FileStore        → /FileStore/tables/drivers.json
#   C) Databricks Repos      → file:<repo_root>/data/raw/drivers.json
#
# Choose ONE of the read approaches below and comment out the others.

# --- Option A: Unity Catalog Volume (matches lesson default) ---
df_json = spark.read.format("json") \
    .option("multiline", "false") \
    .load("/Volumes/dbacademy/default/raw_data/drivers.json")

# --- Option B: DBFS FileStore ---
# df_json = spark.read.format("json") \
#     .option("multiline", "false") \
#     .load("/FileStore/tables/drivers.json")

# --- Option C: Databricks Repos (Git-synced) ---
# import os
# repo_root = os.path.dirname(os.path.abspath(__file__))
# df_json = spark.read.format("json") \
#     .option("multiline", "false") \
#     .load(f"file:{repo_root}/../../data/raw/drivers.json")

display(df_json)


# -----------------------------------------------------------------------------
# 2. Flattening Nested Names in JSON
# -----------------------------------------------------------------------------
# The 'name' column contains nested fields: name.forename and name.surname.
# concat_ws combines them with a space separator into a single 'full_name' column.

df_clean = df_json.withColumn(
    "full_name", concat_ws(" ", col("name.forename"), col("name.surname"))
)
df_clean.display()


# -----------------------------------------------------------------------------
# 3. Data Reading — CSV (Big Mart Sales)
# -----------------------------------------------------------------------------
# Raw file: data/raw/BigMart Sales.csv  (8,523 rows, 12 columns)
#
# Upload options:
#   A) Unity Catalog Volume  → /Volumes/<catalog>/<schema>/raw_data/BigMart Sales.csv
#   B) DBFS FileStore        → /FileStore/tables/BigMart Sales.csv
#   C) Databricks Repos      → file:<repo_root>/data/raw/BigMart Sales.csv
#
# Choose ONE of the read approaches below and comment out the others.

# --- Option A: Unity Catalog Volume (matches lesson default) ---
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/dbacademy/default/raw_data/BigMart Sales.csv")

# --- Option B: DBFS FileStore ---
# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("/FileStore/tables/BigMart Sales.csv")

# --- Option C: Databricks Repos (Git-synced) ---
# import os
# repo_root = os.path.dirname(os.path.abspath(__file__))
# df = spark.read.format("csv") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load(f"file:{repo_root}/../../data/raw/BigMart Sales.csv")

display(df)

# Row and column count
total_rows = df.count()
total_columns = len(df.columns)
print(f"Total Rows: {total_rows}")
print(f"Total Columns: {total_columns}")


# -----------------------------------------------------------------------------
# 4. Schema Inspection
# -----------------------------------------------------------------------------

df.printSchema()


# -----------------------------------------------------------------------------
# 5. DDL Schema Definition
# -----------------------------------------------------------------------------
# A DDL schema is a compact, SQL-style string that maps column names to types.

my_ddl_schema = """
               Item_Identifier STRING,
               Item_Weight STRING,
               Item_Fat_Content STRING,
               Item_Visibility DOUBLE,
               Item_Type STRING,
               Item_MRP DOUBLE,
               Outlet_Identifier STRING,
               Outlet_Establishment_Year LONG,
               Outlet_Size STRING,
               Outlet_Location_Type STRING,
               Outlet_Type STRING,
               Item_Outlet_Sales DOUBLE
               """

# Swap the .load() path to match your upload option (A / B / C above)
df_custom = (
    spark.read.format("csv")
    .schema(my_ddl_schema)
    .option("header", "true")
    .load("/Volumes/dbacademy/default/raw_data/BigMart Sales.csv")  # ← adjust path
)
df_custom.printSchema()


# -----------------------------------------------------------------------------
# 6. StructType Schema Definition
# -----------------------------------------------------------------------------
# StructType gives programmatic control over schema enforcement —
# useful when schemas are built dynamically at runtime.

struct_schema = StructType(
    [
        StructField("Item_Identifier", StringType(), True),
        StructField("Item_Weight", StringType(), True),
        StructField("Item_Fat_Content", StringType(), True),
        StructField("Item_Visibility", DoubleType(), True),
        StructField("Item_Type", StringType(), True),
        StructField("Item_MRP", DoubleType(), True),
        StructField("Outlet_Identifier", StringType(), True),
        StructField("Outlet_Establishment_Year", LongType(), True),
        StructField("Outlet_Size", StringType(), True),
        StructField("Outlet_Location_Type", StringType(), True),
        StructField("Outlet_Type", StringType(), True),
        StructField("Item_Outlet_Sales", DoubleType(), True),
    ]
)

# Swap the .load() path to match your upload option (A / B / C above)
df_custom2 = (
    spark.read.format("csv")
    .schema(struct_schema)
    .option("header", "true")
    .load("/Volumes/dbacademy/default/raw_data/BigMart Sales.csv")  # ← adjust path
)
df_custom2.printSchema()
