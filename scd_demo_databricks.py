# Databricks notebook source
# MAGIC %md
# MAGIC # SCD Type 1, 2 & 3
# MAGIC
# MAGIC **Scenario:** a dimension table `dim_emp` where the attribute `dept` changes over time.
# MAGIC
# MAGIC | emp_id | name | value before | incoming batch | what happened |
# MAGIC |---|---|---|---|---|
# MAGIC | 1 | Alice | Sales | Marketing | **changed** |
# MAGIC | 2 | Bob   | IT    | IT        | unchanged |
# MAGIC | 3 | Chris | (row does not exist yet) | HR | **new row** |
# MAGIC
# MAGIC Run the notebook top-to-bottom. Re-run the **Setup** cell at any time to reset everything.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — create the 3 target tables + the incoming batch

# COMMAND ----------

# --- pick a database (works on Community Edition, hive_metastore or Unity Catalog) ---
DB = "scd_demo"
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DB}")
    spark.sql(f"USE {DB}")
except Exception as e:
    print("Could not use schema", DB, "->", e)
    print("Continuing in the current default schema.")
print("Working database:", spark.sql("SELECT current_database()").first()[0])

# --- existing ("before") state of the three target tables (DROP first so the cell is re-runnable) ---
for t in ["dim_emp_scd1", "dim_emp_scd2", "dim_emp_scd3"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

spark.sql("""
CREATE TABLE dim_emp_scd1 USING DELTA AS
SELECT 1 AS emp_id, 'Alice' AS name, 'Sales' AS dept
UNION ALL
SELECT 2, 'Bob', 'IT'
""")

spark.sql("""
CREATE TABLE dim_emp_scd2 USING DELTA AS
SELECT 1 AS emp_id, 'Alice' AS name, 'Sales' AS dept,
       DATE '2024-01-01' AS eff_start, DATE '9999-12-31' AS eff_end, true AS is_current
UNION ALL
SELECT 2, 'Bob', 'IT', DATE '2024-01-01', DATE '9999-12-31', true
""")

spark.sql("""
CREATE TABLE dim_emp_scd3 USING DELTA AS
SELECT 1 AS emp_id, 'Alice' AS name, 'Sales' AS dept, CAST(NULL AS STRING) AS prev_dept
UNION ALL
SELECT 2, 'Bob', 'IT', CAST(NULL AS STRING)
""")

# --- the incoming batch (source) ---
src = spark.createDataFrame(
    [
        (1, "Alice", "Marketing"),  # CHANGED: Sales -> Marketing
        (2, "Bob", "IT"),           # unchanged
        (3, "Chris", "HR"),         # NEW employee
    ],
    "emp_id INT, name STRING, dept STRING",
)
src.createOrReplaceTempView("src_batch")

print("Target tables created. Incoming batch:")
display(src)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. SCD Type 1 — **overwrite** (no history)
# MAGIC
# MAGIC Latest value simply wins, the old value is **lost**.
# MAGIC Use when history does not matter (e.g. correcting a typo).

# COMMAND ----------

spark.sql("""
MERGE INTO dim_emp_scd1 t
USING src_batch s
ON t.emp_id = s.emp_id
WHEN MATCHED THEN UPDATE SET name = s.name, dept = s.dept
WHEN NOT MATCHED THEN INSERT (emp_id, name, dept) VALUES (s.emp_id, s.name, s.dept)
""")

display(spark.sql("SELECT * FROM dim_emp_scd1 ORDER BY emp_id"))

# MAGIC %md
# MAGIC Alice is now `Marketing` — her previous `Sales` value is **gone forever**. That is Type 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. SCD Type 2 — **new row per change** (full history)
# MAGIC
# MAGIC Every version gets a validity window (`eff_start` / `eff_end`) plus an `is_current` flag.
# MAGIC Implemented with **two simple MERGEs**:
# MAGIC
# MAGIC 1. **close** the current row if the incoming record changed
# MAGIC 2. **insert** the new current row (brand-new keys, or keys whose old row was just closed)

# COMMAND ----------

# Step 1 — close old versions of rows that changed
spark.sql("""
MERGE INTO dim_emp_scd2 t
USING src_batch s
ON t.emp_id = s.emp_id AND t.is_current = true
WHEN MATCHED AND (t.dept <> s.dept OR t.name <> s.name)
  THEN UPDATE SET is_current = false, eff_end = date_sub(current_date(), 1)
""")

# Step 2 — insert the new current version
spark.sql("""
MERGE INTO dim_emp_scd2 t
USING src_batch s
ON t.emp_id = s.emp_id AND t.is_current = true
WHEN NOT MATCHED
  THEN INSERT (emp_id, name, dept, eff_start, eff_end, is_current)
       VALUES (s.emp_id, s.name, s.dept, current_date(), DATE '9999-12-31', true)
""")

print("Full history (Alice now has TWO rows):")
display(spark.sql("SELECT * FROM dim_emp_scd2 ORDER BY eff_start, emp_id"))

print("Current version only (what a join would normally use):")
display(spark.sql("SELECT emp_id, name, dept FROM dim_emp_scd2 WHERE is_current ORDER BY emp_id"))

# MAGIC %md
# MAGIC Both `Sales` (closed) and `Marketing` (open) rows exist — that is Type 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. SCD Type 3 — **extra column** keeps the previous value
# MAGIC
# MAGIC Still one row per entity, but we add a `prev_dept` column that holds the old value
# MAGIC (exactly **one level** of history).

# COMMAND ----------

# The old value on the right-hand side of SET refers to the target row:
#   prev_dept = t.dept  -> the current (old) dept is pushed into the prev_dept column
spark.sql("""
MERGE INTO dim_emp_scd3 t
USING src_batch s
ON t.emp_id = s.emp_id
WHEN MATCHED AND t.dept <> s.dept
  THEN UPDATE SET name = s.name, dept = s.dept, prev_dept = t.dept
WHEN NOT MATCHED
  THEN INSERT (emp_id, name, dept, prev_dept)
       VALUES (s.emp_id, s.name, s.dept, CAST(NULL AS STRING))
""")

display(spark.sql("SELECT * FROM dim_emp_scd3 ORDER BY emp_id"))

# MAGIC %md
# MAGIC Alice shows `dept = Marketing` **and** `prev_dept = Sales` in the same row — that is Type 3.
# MAGIC (If dept changes again tomorrow, `Sales` will be overwritten: only one previous value is kept.)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Side-by-side result

# COMMAND ----------

print("SCD 1 — value overwritten, no history kept")
display(spark.table("dim_emp_scd1").orderBy("emp_id"))

print("SCD 2 — full history, Alice now has 2 rows")
display(spark.table("dim_emp_scd2").orderBy("emp_id", "eff_start"))

print("SCD 3 — current value + previous value in a column")
display(spark.table("dim_emp_scd3").orderBy("emp_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cheat sheet
# MAGIC
# MAGIC | | Type 1 | Type 2 | Type 3 |
# MAGIC |---|---|---|---|
# MAGIC | **What happens** | UPDATE in place | INSERT new row + close old row | UPDATE in place, old value moves to a column |
# MAGIC | **History** | none | full (arbitrary length) | only the previous value |
# MAGIC | **Table growth** | none | grows on every change | none |
# MAGIC | **Cost / complexity** | cheapest | highest | cheap |
# MAGIC | **Typical use** | fix a typo, non-sensitive attributes | audit / slowly changing conformed dimensions | "previous region/segment" style reports |
# MAGIC
# MAGIC **Tips**
# MAGIC - `MERGE` is atomic (Delta ACID): either the whole merge applies or nothing does.
# MAGIC - If a column can be NULL, use the null-safe operator: `NOT (t.dept <=> s.dept)`.
# MAGIC - Re-run the **Setup** cell to reset all three tables, then re-run any section.
