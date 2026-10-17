# =============================================================================
# MILESTONE 06 — Joins & Window Functions
# =============================================================================
# Topics Covered:
#   JOINS
#   - inner join    — matching rows from both DataFrames
#   - left join     — all rows from left + matches from right
#   - right join    — all rows from right + matches from left
#   - anti join     — rows in left with NO match in right
#
#   WINDOW FUNCTIONS
#   - row_number()  — unique sequential number per partition
#   - rank()        — rank with gaps for ties
#   - dense_rank()  — rank without gaps for ties
#   - cumulative sum — running total using rowsBetween
#
# Dataset: inline datasets  |  dbacademy.default.big_mart_sales
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum

df = spark.table("dbacademy.default.big_mart_sales")


# =============================================================================
# PART A — Joins
# =============================================================================

# ── Sample DataFrames ──────────────────────────────────────────────────────
data1 = [
    ("1", "jude",  "d01"),
    ("2", "bill",  "d02"),
    ("3", "elon",  "d03"),
    ("4", "steve", "d04"),
    ("5", "john",  "d04"),
    ("6", "jane",  "d06"),
    ("7", "jill",  "d07"),   # d07 has no matching department
]
schema1 = "emp_id string, emp_name string, dept_id string"
df1 = spark.createDataFrame(data1, schema1)

data2 = [
    ("d01", "HR"),
    ("d02", "IT"),
    ("d03", "FINANCE"),
    ("d04", "MARKETING"),
    ("d05", "SALES"),        # d05 has no matching employee
    ("d06", "SOFTWARE"),
]
schema2 = "dept_id string, department string"
df2 = spark.createDataFrame(data2, schema2)

display(df1)
display(df2)

# ── Inner Join — only rows with a match on both sides ──────────────────────
result1 = df1.join(df2, "dept_id", "inner")
display(result1)

# ── Left Join — all employees; null department for unmatched ───────────────
result2 = df1.join(df2, on="dept_id", how="left")
display(result2)

# ── Right Join — all departments; null employees for unmatched ─────────────
result3 = df1.join(df2, on="dept_id", how="right")
display(result3)

# ── Anti Join — employees with NO matching department ─────────────────────
result4 = df1.join(df2, on="dept_id", how="left_anti")
display(result4)


# =============================================================================
# PART B — Window Functions
# =============================================================================
# Window functions compute values over a partition (group) of rows
# without collapsing them — unlike groupBy().

# ── row_number(): Unique sequential number within each partition ───────────
windowSpec = Window.partitionBy("Item_Type").orderBy("Item_Identifier")
result = df.withColumn("rowCol", row_number().over(windowSpec))
display(result)

# ── rank(): Sequential rank with GAPS for tied values ─────────────────────
# e.g. two rows tied at rank 2 → next rank is 4 (skips 3)
window_spec = Window.partitionBy("Item_Type").orderBy("Item_Identifier")
result = df.withColumn("rank", rank().over(window_spec))
display(result)

# ── dense_rank(): Sequential rank WITHOUT gaps for tied values ─────────────
# e.g. two rows tied at rank 2 → next rank is 3 (no skip)
window_spec = Window.partitionBy("Item_Type").orderBy("Item_Identifier")
result = df.withColumn("dense_rank", dense_rank().over(window_spec))
display(result)

# ── Cumulative Sum: Running total from window start to current row ─────────
# rowsBetween(unboundedPreceding, currentRow) defines the window frame.
window_spec_boundaries = (
    Window.partitionBy("Item_Identifier")
    .orderBy("Outlet_Establishment_Year")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
result = df.withColumn(
    "Cumulative_sum", sum("Item_MRP").over(window_spec_boundaries)
)
display(result)
