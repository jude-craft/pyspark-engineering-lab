<div align="center">

<img src="asset/spark.jpg" alt="Apache Spark Logo" width="700"/>

# PySpark Engineering Lab

**A structured, hands-on learning journey through Apache Spark & PySpark**

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)

</div>

---

## 📌 Overview

This repository documents a progressive, milestone-based learning journey into **Apache Spark** and **PySpark** — one of the most critical tools in the modern Data Engineering stack. The lab was conducted on the **Databricks** platform using real-world retail datasets and covers everything from first principles (reading data, schema definition) through to production patterns (Delta tables, window functions, Spark SQL, and UDFs).

Each lesson is an isolated, runnable `.py` file that can be executed directly in a Databricks notebook or imported as a Databricks Workspace notebook.

> **Why `.py` files?**
> In production Data Engineering environments — especially on Databricks with CI/CD pipelines, version control, and code review workflows — `.py` files are the **industry standard**. They are diff-friendly, lintable, testable with `pytest`, and deploy-ready via Databricks Repos or the Asset Bundles CLI (`databricks bundle`). Jupyter notebooks (`.ipynb`) are valuable for exploration but introduce JSON diff noise and are harder to maintain at scale.

---

## 🗂️ Repository Structure

```
pyspark-engineering-lab/
│
├── README.md                                  ← You are here
│
├── asset/
│   └── spark.jpg                              ← Project banner image
│
├── data/
│   └── raw/
│       ├── BigMart Sales.csv                  ← Retail sales dataset (Milestones 01–08)
│       └── drivers.json                       ← F1 drivers nested JSON dataset (Milestone 01)
│
└── lessons/
    ├── 01-spark-fundamentals/
    │   └── spark_fundamentals.py
    ├── 02-dataframe-transformations/
    │   └── dataframe_transformations.py
    ├── 03-string-and-date-functions/
    │   └── string_and_date_functions.py
    ├── 04-null-handling/
    │   └── null_handling.py
    ├── 05-aggregations-and-arrays/
    │   └── aggregations_and_arrays.py
    ├── 06-joins-and-window-functions/
    │   └── joins_and_window_functions.py
    ├── 07-user-defined-functions/
    │   └── user_defined_functions.py
    ├── 08-data-writing-and-tables/
    │   └── data_writing_and_tables.py
    └── 09-spark-sql/
        └── spark_sql.py
```

---

## 📂 Datasets & Databricks Setup

Both raw datasets live in [`data/raw/`](data/raw/) and must be uploaded to your Databricks workspace before running any lesson. Follow the steps below exactly once — all lessons will then work out of the box.

---

### Dataset 1 — `BigMart Sales.csv`
**Path in repo:** [`data/raw/BigMart Sales.csv`](data/raw/BigMart%20Sales.csv)  
**Used in:** Milestones 01 through 08 (primary practice dataset)

**Schema:**

| Column | Type | Description |
|---|---|---|
| `Item_Identifier` | STRING | Unique product ID |
| `Item_Weight` | STRING / DOUBLE | Product weight (cast to DOUBLE in lessons) |
| `Item_Fat_Content` | STRING | Fat label — `Low Fat`, `Regular`, `LF`, `low fat`, `reg` |
| `Item_Visibility` | DOUBLE | Display area fraction in the store |
| `Item_Type` | STRING | Product category (e.g. `Dairy`, `Soft Drinks`, `Meat`) |
| `Item_MRP` | DOUBLE | Maximum Retail Price |
| `Outlet_Identifier` | STRING | Unique store ID |
| `Outlet_Establishment_Year` | LONG | Year the outlet was established |
| `Outlet_Size` | STRING | Store size (`Small`, `Medium`, `High`) — **contains nulls** |
| `Outlet_Location_Type` | STRING | Tier classification (`Tier 1`, `Tier 2`, `Tier 3`) |
| `Outlet_Type` | STRING | Store type (e.g. `Supermarket Type1`, `Grocery Store`) |
| `Item_Outlet_Sales` | DOUBLE | Sales revenue for the item at that outlet |

> ⚠️ `Outlet_Size` intentionally contains null values — this is the dataset used for the Null Handling milestone.

---

### Dataset 2 — `drivers.json`
**Path in repo:** [`data/raw/drivers.json`](data/raw/drivers.json)  
**Used in:** Milestone 01 (nested JSON & flattening)

This is an F1 drivers dataset in **newline-delimited JSON** format (one JSON object per line). Each record contains a nested `name` field:

```json
{
  "driverId": 1,
  "driverRef": "hamilton",
  "number": 44,
  "code": "HAM",
  "name": { "forename": "Lewis", "surname": "Hamilton" },
  "dob": "1985-01-07",
  "nationality": "British",
  "url": "http://en.wikipedia.org/wiki/Lewis_Hamilton"
}
```

| Column | Type | Description |
|---|---|---|
| `driverId` | INT | Unique driver ID |
| `driverRef` | STRING | URL-safe driver reference slug |
| `number` | INT / STRING | Racing number (`"\\N"` = not assigned) |
| `code` | STRING | 3-letter driver abbreviation (e.g. `HAM`) |
| `name.forename` | STRING | First name — **nested field** |
| `name.surname` | STRING | Last name — **nested field** |
| `dob` | STRING | Date of birth |
| `nationality` | STRING | Driver nationality |

> The `name` field is a **nested struct** — Milestone 01 demonstrates how to access `name.forename` and `name.surname` using dot notation and flatten them into a single `full_name` column.

---

### ⬆️ Uploading Data to Databricks

There are two approaches depending on your Databricks tier.

#### Option A — Unity Catalog Volume *(Recommended — Databricks with Unity Catalog)*

> This is the approach used in the lesson code (`/Volumes/dbacademy/default/raw_data/...`).

1. Open your Databricks workspace
2. In the left sidebar, go to **Catalog** → navigate to your catalog (e.g. `dbacademy`) → `default` schema
3. Click **Volumes** → select or create a volume named `raw_data`
4. Click **Upload to this Volume**
5. Upload both files:
   - `data/raw/BigMart Sales.csv` → will be accessible at `/Volumes/dbacademy/default/raw_data/BigMart Sales.csv`
   - `data/raw/drivers.json` → will be accessible at `/Volumes/dbacademy/default/raw_data/drivers.json`
6. Update the lesson file `spark_fundamentals.py` if your catalog/schema names differ:
   ```python
   # Change these two paths to match your Volume location
   .load("/Volumes/<your-catalog>/<your-schema>/raw_data/BigMart Sales.csv")
   .load("/Volumes/<your-catalog>/<your-schema>/raw_data/drivers.json")
   ```

#### Option B — DBFS FileStore *(Classic Databricks without Unity Catalog)*

1. In the top menu, click **Data** → **Add Data** → **Upload File**
2. Upload both files — they will land in `/FileStore/tables/`
3. Update your read paths in the lesson:
   ```python
   # BigMart Sales
   df = spark.read.format("csv") \
       .option("header", "true") \
       .load("/FileStore/tables/BigMart Sales.csv")

   # Drivers (newline-delimited JSON)
   df_json = spark.read.format("json") \
       .option("multiline", "false") \
       .load("/FileStore/tables/drivers.json")
   ```

#### Option C — Databricks Repos (Git-synced, zero upload)

If you cloned this repo via **Databricks Repos**, the `data/raw/` files are already on the cluster filesystem at the repo path. Read them directly:

```python
import os

# Resolve the repo-relative path at runtime
repo_root = os.path.dirname(os.path.abspath(__file__))

df = spark.read.format("csv") \
    .option("header", "true") \
    .load(f"file:{repo_root}/data/raw/BigMart Sales.csv")

df_json = spark.read.format("json") \
    .option("multiline", "false") \
    .load(f"file:{repo_root}/data/raw/drivers.json")
```

---

### ✅ Verify Your Data is Ready

Run this quick sanity check in your first notebook cell before starting Milestone 01:

```python
# -- BigMart Sales --
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/Volumes/dbacademy/default/raw_data/BigMart Sales.csv")
print(f"BigMart rows: {df.count()}, columns: {len(df.columns)}")
# Expected → BigMart rows: 8523, columns: 12

# -- Drivers JSON --
df_json = spark.read.format("json") \
    .option("multiline", "false") \
    .load("/Volumes/dbacademy/default/raw_data/drivers.json")
print(f"Drivers rows: {df_json.count()}")
# Expected → Drivers rows: 853
```

---

## 🏁 Learning Milestones

### Milestone 01 — Spark Fundamentals
**File:** [`lessons/01-spark-fundamentals/spark_fundamentals.py`](lessons/01-spark-fundamentals/spark_fundamentals.py)

The foundation of every Spark pipeline — getting data in and understanding its structure.

| Concept | Description |
|---|---|
| `spark.table()` | Reading data from a Databricks Unity Catalog table |
| `spark.read.format("csv")` | Loading a raw CSV file from a Volume path |
| Nested JSON | Flattening nested fields using `col("name.forename")` |
| `concat_ws()` | Combining nested fields into a single flat column |
| `df.printSchema()` | Inspecting column names and inferred data types |
| `df.count()` / `len(df.columns)` | Row and column profiling |
| DDL Schema | Defining schema as a compact SQL-style string |
| `StructType` / `StructField` | Programmatic schema enforcement at read time |

> **Key insight:** Always define a schema explicitly at read time. Spark's schema inference triggers a full data scan, which is expensive on large datasets and can produce incorrect types (e.g., inferring `Long` for a ZIP code column).

---

### Milestone 02 — DataFrame Transformations
**File:** [`lessons/02-dataframe-transformations/dataframe_transformations.py`](lessons/02-dataframe-transformations/dataframe_transformations.py)

The core transformation toolkit that powers 80% of daily Data Engineering work.

| Concept | Description |
|---|---|
| `select()` | Project specific columns; supports both string and `col()` syntax |
| `filter()` / `where()` | Row-level filtering with single and compound boolean conditions |
| `isin()` | Filter for membership in a list of values |
| `isNull()` | Filter for missing values within a filter chain |
| `withColumn()` + `.cast()` | Type-cast a column to a different data type |
| `sort()` / `orderBy()` | Single and multi-column sorting with mixed ASC/DESC control |
| `drop()` | Remove one or more columns from a DataFrame |
| `dropDuplicates()` / `drop_duplicates()` | Full-row or subset-based deduplication |
| `union()` | Position-based row combination of two DataFrames |
| `unionByName()` | Name-based row combination — safe when column order differs |

> **Key insight:** Prefer `col()` over string column references in complex expressions. It gives you early validation, makes chained expressions cleaner, and enables IDE auto-completion.

---

### Milestone 03 — String & Date Functions
**File:** [`lessons/03-string-and-date-functions/string_and_date_functions.py`](lessons/03-string-and-date-functions/string_and_date_functions.py)

Real-world data is messy. String and date manipulation are essential cleaning tools.

| Concept | Description |
|---|---|
| `initcap()` | Title-case each word — useful for normalising product or category names |
| `upper()` / `lower()` | Standardise casing before joins or comparisons |
| `length()` | Character count — useful for data quality checks and pattern detection |
| `split()` | Tokenise a string column into an `ArrayType` column |
| Array indexing `[n]` | Extract a single element from a split array by zero-based index |

> **Key insight:** `split()` returns an `ArrayType` column. You can either index into it directly (`split("col", " ")[0]`) or pass it to `explode()` to expand the array into individual rows — a key pattern for handling multi-valued attributes.

---

### Milestone 04 — Null Handling
**File:** [`lessons/04-null-handling/null_handling.py`](lessons/04-null-handling/null_handling.py)

Null management is one of the most important data quality disciplines in Data Engineering. Unhandled nulls silently corrupt aggregations and joins.

| Concept | Description |
|---|---|
| `isNull()` / `isNotNull()` | Detect null / non-null values in a filter expression |
| Null audit | `sum(col(c).isNull().cast("int"))` — count nulls across ALL columns at once |
| `dropna("all")` | Drop rows where every column is null |
| `dropna("any")` | Drop rows containing any null value |
| `dropna(subset=[...])` | Drop rows where a specific column is null |
| `fillna("value")` | Replace all string nulls with a placeholder |
| `fillna(value, subset=[...])` | Replace nulls in a targeted column only |
| `fillna({col: value})` | Dictionary-based per-column null replacement (most precise) |

> **Key insight:** Run the null audit (`select([sum(col(c).isNull().cast("int"))...])`) as the very first step of any data exploration. It gives a complete picture of data completeness in a single query.

---

### Milestone 05 — Aggregations & Arrays
**File:** [`lessons/05-aggregations-and-arrays/aggregations_and_arrays.py`](lessons/05-aggregations-and-arrays/aggregations_and_arrays.py)

Aggregations summarise data for reporting and analysis. Array functions unlock advanced data modelling patterns.

| Concept | Description |
|---|---|
| `groupBy().agg()` | Grouped aggregation with one or more functions |
| `sum()` / `avg()` / `count()` | Standard aggregation functions with `.alias()` for clean column names |
| Multi-column `groupBy` | Group across multiple dimensions simultaneously |
| `collect_list()` | Aggregate individual values into a list array per group |
| `pivot()` | Rotate unique column values into separate columns (crosstab) |
| `when()` / `otherwise()` | SQL `CASE WHEN` equivalent — conditional column derivation |
| `explode()` | Flatten an `ArrayType` column into one row per element |
| `array_contains()` | Check whether a specific value exists in an array column |

> **Key insight:** `pivot()` is a powerful analytical tool but can be expensive on high-cardinality columns. Always specify the expected pivot values explicitly as a list — e.g. `.pivot("Outlet_Size", ["Small", "Medium", "High"])` — to avoid a full scan to discover unique values.

---

### Milestone 06 — Joins & Window Functions
**File:** [`lessons/06-joins-and-window-functions/joins_and_window_functions.py`](lessons/06-joins-and-window-functions/joins_and_window_functions.py)

Joins combine datasets. Window functions compute ordered analytics without collapsing rows — together they cover almost every reporting use-case.

#### Joins

| Join Type | Keyword | Behaviour |
|---|---|---|
| Inner | `"inner"` | Returns only matching rows from both DataFrames |
| Left | `"left"` | All rows from the left + matched rows from the right |
| Right | `"right"` | All rows from the right + matched rows from the left |
| Anti | `"left_anti"` | Rows in the left DataFrame that have **no** match in the right |

#### Window Functions

| Function | Description |
|---|---|
| `row_number()` | Unique sequential integer per row within a partition |
| `rank()` | Rank with gaps — tied rows get the same rank, next rank skips |
| `dense_rank()` | Rank without gaps — tied rows get the same rank, next rank is consecutive |
| Cumulative Sum | Running total using `rowsBetween(unboundedPreceding, currentRow)` |

> **Key insight:** Window functions require a `Window.partitionBy()` spec. Without it, the entire dataset becomes one partition — meaning the function runs on the driver, defeating the purpose of distributed processing.

---

### Milestone 07 — User-Defined Functions (UDFs)
**File:** [`lessons/07-user-defined-functions/user_defined_functions.py`](lessons/07-user-defined-functions/user_defined_functions.py)

UDFs allow custom Python logic to operate on DataFrame columns when no built-in function exists.

| Concept | Description |
|---|---|
| `udf(fn, ReturnType)` | Register a Python function as a Spark UDF |
| `StringType()` / `DoubleType()` | Declare the return type for type safety |
| `withColumn()` + UDF | Apply the UDF to produce a new derived column |
| Lambda UDF | Concise single-expression UDFs for simple transformations |

**Examples built in this milestone:**
- **Fat Content Standardiser** — normalises `"LF"`, `"low fat"`, `"reg"` to canonical values
- **MRP Tier Classifier** — buckets items into `Budget`, `Mid-Range`, `Premium` price tiers
- **Inline Lambda UDF** — doubles sales values in a single expression

> ⚠️ **Performance warning:** UDFs break Catalyst query optimisation and require serialising data between the JVM and the Python interpreter row-by-row. Always prefer built-in `pyspark.sql.functions` equivalents. Use UDFs only when no built-in alternative exists. For high-throughput workloads, consider **Pandas UDFs** (`@pandas_udf`) — they operate on batches and avoid per-row serialisation overhead.

---

### Milestone 08 — Data Writing & Tables
**File:** [`lessons/08-data-writing-and-tables/data_writing_and_tables.py`](lessons/08-data-writing-and-tables/data_writing_and_tables.py)

Persisting transformed data is the final step of every pipeline. Understanding write modes prevents accidental data loss or duplication.

| Write Mode | Behaviour | Use Case |
|---|---|---|
| `append` | Adds new data to existing output | Incremental / streaming loads |
| `overwrite` | Replaces all existing data | Full refresh / reprocessing |
| `error` | Raises error if output exists (default) | Safe one-time writes |
| `ignore` | Skips write silently if output exists | Idempotent pipeline steps |

| Method | Description |
|---|---|
| `.write.format("csv").save(path)` | Write to file storage as CSV |
| `.write.mode(...).option("header", "true")` | Write CSV with column header row |
| `.write.mode(...).saveAsTable("catalog.db.table")` | Write directly to a managed Delta table |

> **Industry standard:** On Databricks, always prefer `saveAsTable()` over writing raw files. It persists data in **Delta Lake format**, giving you ACID transactions, time-travel, schema enforcement, and efficient `MERGE INTO` upserts — all critical capabilities for production Lakehouse architectures.

---

### Milestone 09 — Spark SQL
**File:** [`lessons/09-spark-sql/spark_sql.py`](lessons/09-spark-sql/spark_sql.py)

Spark SQL lets you query DataFrames using standard ANSI SQL. It is especially powerful for analysts transitioning to Spark and for writing complex analytical queries.

| Concept | Description |
|---|---|
| `createOrReplaceTempView("name")` | Register a DataFrame as a queryable SQL view in the session |
| `spark.sql("SELECT ...")` | Execute a SQL string and return a DataFrame |
| `%sql` magic | Databricks-native SQL cell (renders interactive tables) |
| `UPPER()` / `LOWER()` / `LENGTH()` | Standard SQL string manipulation functions |
| `YEAR()` / `MONTH()` / `DATE_ADD()` | SQL date extraction and arithmetic functions |
| `AVG()` / `SUM()` / `COUNT()` / `ROUND()` | SQL aggregation functions with `GROUP BY` and `ORDER BY` |
| CTEs (`WITH ... AS (...)`) | Named sub-queries for modular, readable SQL logic |

**Dataset:** A Kenya county weather dataset covering temperature and rainfall observations across Meru, Nakuru, Kitale, Turkana, and Mombasa.

> **Key insight:** `createOrReplaceTempView()` creates a session-scoped view — it only exists for the lifetime of the active Spark session. For persistent, shareable views, use `spark.catalog.createTable()` or `saveAsTable()`. CTEs are preferred over nested subqueries for readability and maintainability.

---

## 🛠️ Tech Stack

| Technology | Role |
|---|---|
| **Apache Spark 3.x** | Distributed data processing engine |
| **PySpark** | Python API for Apache Spark |
| **Databricks** | Managed Spark platform (notebooks, clusters, Unity Catalog) |
| **Delta Lake** | Open-source storage layer — ACID transactions on data lakes |
| **Unity Catalog** | Databricks data governance and catalog layer |
| **Python 3.x** | Application language |

---

## 📦 Datasets Used

| Dataset | Repo Location | Description | Rows |
|---|---|---|---|
| `BigMart Sales.csv` | [`data/raw/BigMart Sales.csv`](data/raw/BigMart%20Sales.csv) | Retail sales data — outlet types, locations, product categories | 8,523 |
| `drivers.json` | [`data/raw/drivers.json`](data/raw/drivers.json) | F1 drivers — newline-delimited JSON with nested `name` struct | 853 |
| Kenya Weather Dataset | Inline (hand-coded in lesson) | Temperature & rainfall observations across Kenyan counties | 10 |
| Employee / Department | Inline (hand-coded in lesson) | Join demo: employees mapped to departments | 13 |
| User / IP | Inline (hand-coded in lesson) | `collect_list()` demo — user session IP addresses | 6 |

---

## 🚀 How to Run

### Option 1 — Databricks Repos (Recommended)

1. In your Databricks workspace, go to **Repos → Add Repo**
2. Paste the repository URL and click **Create Repo**
3. Navigate to any `lessons/` file
4. Open it — Databricks will render it as an interactive notebook
5. Attach a cluster and run cell by cell

### Option 2 — Databricks CLI / Asset Bundles

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Sync repo to your workspace
databricks workspace import_dir lessons/ /Users/<your-email>/pyspark-lab/
```

### Option 3 — Local PySpark (Without Databricks)

Some cells use Databricks-specific APIs (`display()`, `spark.table()`, `%sql` magic, Unity Catalog paths). For local execution, substitute:

```python
# Replace:
df = spark.table("dbacademy.default.big_mart_sales")
# With:
df = spark.read.option("header", "true").csv("path/to/BigMart Sales.csv")

# Replace display(df) with:
df.show()
```

---

## 📈 Learning Path

```
01 Fundamentals  →  02 Transformations  →  03 Strings & Dates
                                                    ↓
06 Joins & Windows  ←  05 Aggregations  ←  04 Null Handling
        ↓
07 UDFs  →  08 Data Writing  →  09 Spark SQL
```

Each milestone builds on the previous. It is recommended to follow the numbered order, as later lessons assume familiarity with concepts introduced earlier.

---

## 💡 Key Takeaways

- **Schema-first thinking** — always define schemas explicitly; never rely on Spark's schema inference in production
- **Built-ins over UDFs** — Catalyst-optimised built-in functions outperform Python UDFs by orders of magnitude
- **Delta > raw files** — `saveAsTable()` gives you ACID, time-travel, and schema enforcement for free
- **Window functions are not groupBy** — they add analytical columns without aggregating rows
- **`unionByName` is safer than `union`** — position-based unions silently produce wrong results when column order differs
- **Null audit first** — always profile null counts before transforming or joining data

---

## 📚 Further Reading

- [PySpark Official Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Databricks Learning Academy](https://www.databricks.com/learn/training)
- [Delta Lake Documentation](https://docs.delta.io/latest/index.html)
- [Spark: The Definitive Guide — Bill Chambers & Matei Zaharia](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)

---

<div align="center">

*Built as part of a hands-on Data Engineering learning journey.*
*Apache Spark · PySpark · Databricks · Delta Lake*

</div>
