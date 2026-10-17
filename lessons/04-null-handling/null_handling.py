# =============================================================================
# MILESTONE 04 — Null Handling
# =============================================================================
# Topics Covered:
#   - isNull() / isNotNull() — detecting nulls via filter
#   - Null audit             — count nulls across ALL columns at once
#   - dropna()               — drop rows based on null thresholds
#   - fillna()               — fill nulls with a constant or per-column map
#
# Dataset: dbacademy.default.big_mart_sales
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import col, sum

df = spark.table("dbacademy.default.big_mart_sales")


# -----------------------------------------------------------------------------
# 1. Detecting Nulls — isNull() / isNotNull()
# -----------------------------------------------------------------------------

# Rows where Outlet_Size is missing
df.filter(col("Outlet_Size").isNull()).display()

# Rows where Outlet_Size is present
df.filter(col("Outlet_Size").isNotNull()).display()


# -----------------------------------------------------------------------------
# 2. Null Audit — Count Nulls Across All Columns
# -----------------------------------------------------------------------------
# Cast the boolean result of isNull() to integer (0/1) and sum per column.
# Returns a single-row summary showing the null count for every field.

df.select(
    [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
).display()


# -----------------------------------------------------------------------------
# 3. Dropping Nulls — dropna()
# -----------------------------------------------------------------------------

# Drop rows where EVERY column is null
df.dropna("all").display()

# Drop rows that contain ANY null value
df.dropna("any").display()

# Drop rows where a SPECIFIC column is null
df.dropna(subset=["Outlet_Size"]).display()

# Full dataset for reference
df.display()


# -----------------------------------------------------------------------------
# 4. Filling Nulls — fillna()
# -----------------------------------------------------------------------------

# Replace ALL string nulls with a placeholder
df.fillna("NotAvailable").display()

# Replace nulls in a specific column only
df.fillna("NotAvailable", subset=["Outlet_Size"]).display()

# Replace nulls using a column-to-value mapping (most precise approach)
df.fillna({"Outlet_Size": "NotAvailable"}).display()
