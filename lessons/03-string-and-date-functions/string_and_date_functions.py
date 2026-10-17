# =============================================================================
# MILESTONE 03 — String & Date Functions
# =============================================================================
# Topics Covered:
#   - initcap()  — capitalize first letter of each word
#   - upper()    — full uppercase conversion
#   - lower()    — full lowercase conversion
#   - length()   — character count of a string column
#   - split()    — tokenize a string column into an array
#   - Indexing   — extract a specific element from a split array
#
# Dataset: dbacademy.default.big_mart_sales
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import col, initcap, upper, lower, length, split

df = spark.table("dbacademy.default.big_mart_sales")


# -----------------------------------------------------------------------------
# 1. initcap() — Title-Case Formatting
# -----------------------------------------------------------------------------
# Capitalises the first letter of every word in the column value.
# Useful for normalising inconsistently-cased categorical fields.

df.select(initcap("Item_Type")).display()


# -----------------------------------------------------------------------------
# 2. upper() / lower() / length()
# -----------------------------------------------------------------------------
# Standard string normalisation functions used before joins or comparisons.

df.select(
    col("Item_Type"),
    upper(col("Item_Type")).alias("Item_Type_Upper"),
    lower(col("Item_Type")).alias("Item_Type_Lower"),
    length(col("Item_Type")).alias("Item_Type_Length"),
).display()


# -----------------------------------------------------------------------------
# 3. split() — Tokenise a Column into an Array
# -----------------------------------------------------------------------------
# Converts a space-delimited string column into an ArrayType column.
# e.g. "Supermarket Type1" → ["Supermarket", "Type1"]

df.withColumn("Outlet_Type", split("Outlet_Type", " ")).display()


# -----------------------------------------------------------------------------
# 4. Indexing — Extract a Specific Token from a Split Array
# -----------------------------------------------------------------------------
# Access individual elements from the resulting array using bracket notation.
# Index [0] = first token, [1] = second token, etc.

df.withColumn("Outlet_Type", split("Outlet_Type", " ")[1]).display()
