# =============================================================================
# MILESTONE 09 — Spark SQL
# =============================================================================
# Topics Covered:
#   - Creating a temporary view from a DataFrame (createOrReplaceTempView)
#   - Running SQL queries with spark.sql() or the %sql magic in Databricks
#   - String functions in SQL: UPPER, LOWER, LENGTH
#   - Date functions in SQL:   YEAR, MONTH, DATE_ADD
#   - Aggregation functions:   AVG, SUM, COUNT, ROUND, GROUP BY, ORDER BY
#   - Common Table Expressions (CTEs) for modular query design
#
# Dataset: Inline Kenya Weather Dataset (Kenyan counties)
# Platform: Databricks (Apache Spark)
# =============================================================================

from pyspark.sql.functions import to_date


# =============================================================================
# 1. Create the Practice Dataset
# =============================================================================

weather_data = [
    (1,  "Meru",    "2026-01-01", 24.5, 12.0),
    (2,  "Meru",    "2026-01-02", 26.0,  0.0),
    (3,  "Nakuru",  "2026-01-01", 22.0,  8.5),
    (4,  "Nakuru",  "2026-01-02", 28.0,  0.0),
    (5,  "Kitale",  "2026-01-01", 21.5, 15.0),
    (6,  "Kitale",  "2026-01-02", 23.0, None),
    (7,  "Turkana", "2026-01-01", 34.0,  0.0),
    (8,  "Turkana", "2026-01-02", 36.5,  0.0),
    (9,  "Mombasa", "2026-01-01", 31.0,  5.0),
    (10, "Mombasa", "2026-01-02", 32.0, 18.0),
]

columns = ["record_id", "county", "observation_date", "temperature", "rainfall_mm"]

weather_df = spark.createDataFrame(weather_data, columns)

# Cast the date string to a proper DateType
weather_df = weather_df.withColumn("observation_date", to_date("observation_date"))

# Register as a temporary view so it can be queried with SQL
weather_df.createOrReplaceTempView("weather")

display(weather_df)


# =============================================================================
# 2. Basic SELECT
# =============================================================================

# Programmatic API approach (equivalent to %sql SELECT * FROM weather)
spark.sql("SELECT * FROM weather").display()


# =============================================================================
# 3. String Functions in SQL
# =============================================================================

spark.sql("""
  SELECT
    county,
    UPPER(county)  AS uppercase_county,
    LOWER(county)  AS lowercase_county,
    LENGTH(county) AS name_length
  FROM weather
""").display()

# ── Databricks Magic Cell equivalent ─────────────────────────────────────
# %sql
# SELECT
#   county,
#   UPPER(county)  AS uppercase_county,
#   LOWER(county)  AS lowercase_county,
#   LENGTH(county) AS name_length
# FROM weather;


# =============================================================================
# 4. Date Functions in SQL
# =============================================================================

spark.sql("""
  SELECT
    county,
    observation_date,
    YEAR(observation_date)         AS year,
    MONTH(observation_date)        AS month,
    DATE_ADD(observation_date, 7)  AS date_after_week
  FROM weather
""").display()

# ── Databricks Magic Cell equivalent ─────────────────────────────────────
# %sql
# SELECT
#   county,
#   observation_date,
#   YEAR(observation_date)        AS year,
#   MONTH(observation_date)       AS month,
#   DATE_ADD(observation_date, 7) AS date_after_week
# FROM weather;


# =============================================================================
# 5. Aggregation Functions
# =============================================================================

spark.sql("""
  SELECT
    county,
    ROUND(AVG(temperature), 2)  AS avg_temperature,
    ROUND(SUM(rainfall_mm), 2)  AS total_rainfall,
    COUNT(*)                    AS observation_count
  FROM weather
  GROUP BY county
  ORDER BY total_rainfall DESC
""").display()

# ── Databricks Magic Cell equivalent ─────────────────────────────────────
# %sql
# SELECT
#   county,
#   ROUND(AVG(temperature), 2) AS avg_temperature,
#   ROUND(SUM(rainfall_mm), 2) AS total_rainfall,
#   COUNT(*) AS observation_count
# FROM weather
# GROUP BY county
# ORDER BY total_rainfall DESC;


# =============================================================================
# 6. Common Table Expressions (CTEs)
# =============================================================================
# CTEs improve readability by naming intermediate result sets.
# Here, county_summary pre-aggregates, then the outer query filters
# counties with an average temperature above 25°C.

spark.sql("""
  WITH county_summary AS (
    SELECT
      county,
      AVG(temperature)  AS avg_temperature,
      SUM(rainfall_mm)  AS total_rainfall
    FROM weather
    GROUP BY county
  )
  SELECT *
  FROM county_summary
  WHERE avg_temperature > 25
""").display()

# ── Databricks Magic Cell equivalent ─────────────────────────────────────
# %sql
# WITH county_summary AS (
#   SELECT
#     county,
#     AVG(temperature)  AS avg_temperature,
#     SUM(rainfall_mm)  AS total_rainfall
#   FROM weather
#   GROUP BY county
# )
# SELECT *
# FROM county_summary
# WHERE avg_temperature > 25;
