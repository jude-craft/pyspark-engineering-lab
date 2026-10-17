# =============================================================================
# MILESTONE 02 — DataFrame Transformations
# =============================================================================
# Topics Covered:
#   - select()          — column projection
#   - filter() / where()— row-level filtering (single & compound conditions)
#   - withColumn()      — type casting & computed columns
#   - sort() / orderBy()— single and multi-column sorting (asc / desc)
#   - drop()            — removing columns
#   - dropDuplicates()  — deduplication (full-row & subset)
#   - union()           — position-based DataFrame merging
#   - unionByName()     — name-based DataFrame merging
#
# Dataset: dbacademy.default.big_mart_sales
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import col

df = spark.table("dbacademy.default.big_mart_sales")


# -----------------------------------------------------------------------------
# 1. SELECT — Column Projection
# -----------------------------------------------------------------------------

# String-based column selection
df.select("Item_Identifier", "Item_Weight").display()

# col()-based selection (preferred for complex expressions)
df.select(
    col("Item_Identifier"), col("Item_Weight"),
    col("Item_Type"), col("Item_Visibility")
).show()


# -----------------------------------------------------------------------------
# 2. FILTER — Row-Level Filtering
# -----------------------------------------------------------------------------

# Scenario 1: Single equality condition
df.filter(col("Item_Fat_Content") == "Regular").display()

# Scenario 2: Compound AND condition
df.filter(
    (col("Item_Type") == "Soft Drinks") & (col("Item_Weight") < 10)
).display()

# Scenario 3: isin() + isNull() — Tier 1 or Tier 2 outlets with unknown size
df.filter(
    (col("Outlet_Location_Type").isin("Tier 1", "Tier 2"))
    & (col("Outlet_Size").isNull())
).display()


# -----------------------------------------------------------------------------
# 3. TYPE CASTING — withColumn()
# -----------------------------------------------------------------------------
# Item_Weight was loaded as STRING; cast to DOUBLE for numeric operations.

df_cast = df.withColumn("Item_Weight", col("Item_Weight").cast("double"))
df_cast.printSchema()


# -----------------------------------------------------------------------------
# 4. SORT / orderBy
# -----------------------------------------------------------------------------

# Scenario 1: Single column descending
df.sort(col("Item_Weight").desc()).display()

# Scenario 2: Single column ascending (default)
df.sort(col("Item_Visibility")).display()

# Scenario 3: Multi-column — both descending
df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 0]).display()

# Scenario 4: Multi-column — mixed order
df.sort(["Item_Weight", "Item_Visibility"], ascending=[0, 1]).display()


# -----------------------------------------------------------------------------
# 5. DROP — Remove Columns
# -----------------------------------------------------------------------------

# Remove a single column
df.drop("Item_Visibility")

# Remove multiple columns
df.drop("Item_Visibility", "Item_MRP")


# -----------------------------------------------------------------------------
# 6. DROP DUPLICATES — Deduplication
# -----------------------------------------------------------------------------

# Drop fully duplicate rows
df.dropDuplicates().display()

# Deduplicate based on a subset of columns
df.drop_duplicates(subset=["Item_Type"]).display()


# -----------------------------------------------------------------------------
# 7. UNION & UNION BY NAME — Combining DataFrames
# -----------------------------------------------------------------------------

# --- Prepare two DataFrames with matching schema ---
data1 = [("1", "kad"), ("2", "sid")]
schema1 = "id STRING, name STRING"
df1 = spark.createDataFrame(data1, schema1)

data2 = [("3", "rahul"), ("4", "jas")]
schema2 = "id STRING, name STRING"
df2 = spark.createDataFrame(data2, schema2)

df1.display()
df2.display()

# Position-based union
df1.union(df2).display()

# --- Demonstrate why unionByName matters (column order differs) ---
data1_messy = [("kad", "1"), ("sid", "2")]
schema1_messy = "name STRING, id STRING"
df1_messy = spark.createDataFrame(data1_messy, schema1_messy)
df1_messy.display()

# Name-based union — safe when column order differs across DataFrames
df1_messy.unionByName(df2).display()
