# =============================================================================
# MILESTONE 07 — User-Defined Functions (UDFs)
# =============================================================================
# Topics Covered:
#   - Defining a Python UDF with udf()
#   - Registering return types using Spark SQL types
#   - Applying UDFs to DataFrame columns via withColumn()
#   - When to use UDFs vs. built-in functions
#
# Note: UDFs are row-level Python functions registered with the Spark driver.
#       They bypass Catalyst optimisation — prefer built-in functions
#       (pyspark.sql.functions) whenever possible for performance.
#       Use UDFs only when no built-in equivalent exists.
#
# Dataset: dbacademy.default.big_mart_sales  |  inline dataset
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

df = spark.table("dbacademy.default.big_mart_sales")


# =============================================================================
# EXAMPLE 1 — Fat Content Standardisation UDF
# =============================================================================
# The Item_Fat_Content column has inconsistent values:
#   "LF", "low fat"  → should be "Low Fat"
#   "reg"            → should be "Regular"
# A UDF normalises these to a canonical form.

def standardise_fat_content(value: str) -> str:
    """Normalise inconsistent fat-content labels to canonical values."""
    if value in ("LF", "low fat"):
        return "Low Fat"
    elif value == "reg":
        return "Regular"
    return value   # already canonical — return as-is


standardise_fat_udf = udf(standardise_fat_content, StringType())

df_clean = df.withColumn(
    "Item_Fat_Content", standardise_fat_udf(col("Item_Fat_Content"))
)
df_clean.select("Item_Identifier", "Item_Fat_Content").display()


# =============================================================================
# EXAMPLE 2 — MRP Tier Classification UDF
# =============================================================================
# Classifies each item into a price tier based on its MRP value.

def classify_mrp(mrp: float) -> str:
    """Bucket items into price tiers based on Maximum Retail Price."""
    if mrp is None:
        return "Unknown"
    if mrp < 50:
        return "Budget"
    elif mrp < 150:
        return "Mid-Range"
    else:
        return "Premium"


classify_mrp_udf = udf(classify_mrp, StringType())

df_tiered = df.withColumn(
    "MRP_Tier", classify_mrp_udf(col("Item_MRP"))
)
df_tiered.select("Item_Identifier", "Item_MRP", "MRP_Tier").display()


# =============================================================================
# EXAMPLE 3 — Inline Lambda UDF (concise single-expression UDFs)
# =============================================================================

double_sales_udf = udf(lambda x: x * 2 if x is not None else None, DoubleType())

df.withColumn(
    "Doubled_Sales", double_sales_udf(col("Item_Outlet_Sales"))
).select("Item_Identifier", "Item_Outlet_Sales", "Doubled_Sales").display()
