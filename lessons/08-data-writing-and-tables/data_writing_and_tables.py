# =============================================================================
# MILESTONE 08 — Data Writing & Tables
# =============================================================================
# Topics Covered:
#   - Writing a DataFrame to CSV
#   - Write modes: append | overwrite | error | ignore
#   - saveAsTable() — writing directly to a Delta/Hive catalog table
#     (industry standard on Databricks Lakehouse)
#
# Dataset: dbacademy.default.big_mart_sales
# Platform: Databricks (Apache Spark)
# =============================================================================

df = spark.table("dbacademy.default.big_mart_sales")


# =============================================================================
# PART A — Writing to Files
# =============================================================================

# ── Basic CSV write (default mode = error if output path exists) ──────────
df.write.format("csv").save("/Filestore/tables/CSV/data.csv")


# =============================================================================
# PART B — Write Modes
# =============================================================================
# mode("append")    → adds new data to an existing output location
# mode("overwrite") → replaces ALL existing data at the output location
# mode("error")     → raises an error if the output location already exists
# mode("ignore")    → silently skips the write if the output location exists

# 1. APPEND — safe for incremental loads
df.write.format("csv").mode("append").option("header", "true").save(
    "/Volumes/dbacademy/default/raw_data/cleaned_sales_output"
)

# Save as a managed Delta table (industry standard on Databricks)
df.write.mode("append").saveAsTable("dbacademy.default.cleaned_sales_data")


# 2. OVERWRITE — full refresh / reprocessing
df.write.format("csv").mode("overwrite").option("header", "true").save(
    "/Volumes/dbacademy/default/raw_data/cleaned_sales_output"
)


# 3. ERROR — safe guard: fails if output already exists (default behaviour)
df.write.format("csv").mode("error").option("header", "true").save(
    "/Volumes/dbacademy/default/raw_data/cleaned_sales_output"
)


# 4. IGNORE — idempotent writes; silently skips if output exists
df.write.format("csv").mode("ignore").option("header", "true").save(
    "/Volumes/dbacademy/default/raw_data/cleaned_sales_output"
)


# =============================================================================
# PART C — Industry Best Practice: saveAsTable() → Delta Format
# =============================================================================
# On the Databricks Lakehouse, writing to a Unity Catalog managed table
# automatically persists data as Delta format — enabling:
#   • ACID transactions
#   • Time-travel (version history)
#   • Schema enforcement
#   • Efficient upserts (MERGE INTO)

df.write.mode("overwrite").saveAsTable("dbacademy.default.big_mart_clean")
