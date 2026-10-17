# =============================================================================
# MILESTONE 05 — Aggregations & Arrays
# =============================================================================
# Topics Covered:
#   - groupBy() + agg()    — single and multi-column aggregations
#   - sum(), avg(), count() — common aggregation functions
#   - collect_list()        — aggregate values into an array per group
#   - pivot()               — reshape rows into columns
#   - when() / otherwise()  — conditional column derivation (SQL CASE WHEN)
#   - explode()             — flatten an array column into multiple rows
#   - array_contains()      — membership check on array-type columns
#
# Dataset: dbacademy.default.big_mart_sales  |  inline datasets
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import (
    col, sum, avg, count,
    collect_list, explode, array_contains,
    split, when,
)

df = spark.table("dbacademy.default.big_mart_sales")


# =============================================================================
# PART A — GroupBy Aggregations
# =============================================================================

# Scenario 1: Sum of MRP per item type
df_sum = df.groupBy("Item_Type").agg(sum("Item_MRP"))
df_sum.display()

# Scenario 2: Average MRP per item type
df_avg = df.groupBy("Item_Type").agg(avg("Item_MRP"))
df_avg.display()

# Scenario 3: Multi-column groupBy with aliased aggregations
df_multiple = df.groupBy("Item_Type", "Outlet_Size").agg(
    sum("Item_MRP").alias("Total_MRP")
)
df_multiple.display()

# Scenario 4: Multiple aggregation functions in one pass
df_multiple = df.groupBy("Item_Type", "Outlet_Size").agg(
    sum("Item_MRP").alias("Total_MRP"),
    avg("Item_MRP").alias("Avg_MRP"),
    count("Item_MRP").alias("Count_MRP"),
)
df_multiple.display()


# =============================================================================
# PART B — collect_list(): Aggregate Values into an Array
# =============================================================================
# Real-world use: group all IP addresses accessed by each user into a list.

data = [
    ("user1", "192.168.1.1"),
    ("user1", "192.168.2.1"),
    ("user2", "192.168.2.1"),
    ("user3", "192.168.3.1"),
    ("user2", "192.168.4.1"),
    ("user5", "192.168.1.1"),
]
schema = "user string, ip string"
df_ip = spark.createDataFrame(data, schema)
df_ip.display()

output = df_ip.groupBy("user").agg(collect_list("ip").alias("ips"))
display(output)


# =============================================================================
# PART C — PIVOT: Reshape Data (Rows → Columns)
# =============================================================================
# Pivot creates one column per unique value in the pivot column.
# Here we get avg Item_MRP for each Outlet_Size as separate columns.

result = df.groupBy("Item_Type").pivot("Outlet_Size").agg(avg("Item_MRP"))
display(result)


# =============================================================================
# PART D — when() / otherwise(): Conditional Column (CASE WHEN)
# =============================================================================

result = df.withColumn(
    "veg_flag", when(col("Item_Type") == "Meat", "Non-Veg").otherwise("Veg")
)
display(result)


# =============================================================================
# PART E — explode() and array_contains()
# =============================================================================

# Step 1: Convert Outlet_Type string into an array by splitting on space
df_exp = df.withColumn("Outlet_Type", split("Outlet_Type", " "))
df_exp.display()

# Step 2: explode() — one row per array element
df_explode = df_exp.select(col("Item_Identifier"), explode(col("Outlet_Type")))
df_explode.display()

# Step 3: array_contains() — flag rows where "Supermarket" is in the array
df_arrc = df_exp.withColumn(
    "In_Supermarket", array_contains(col("Outlet_Type"), "Supermarket")
)
df_arrc.display()
