# Databricks notebook source
# MAGIC %md
# MAGIC # Test Notebook
# MAGIC
# MAGIC This is a simple test notebook for the Databricks Asset Bundle project.
# MAGIC
# MAGIC ## Purpose
# MAGIC - Validate deployment configuration
# MAGIC - Test basic Spark functionality
# MAGIC - Demonstrate notebook integration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import sys

print(f"Python version: {sys.version}")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Data

# COMMAND ----------

# Create a simple DataFrame for testing
data = [
    ("Alice", 34, "Engineering"),
    ("Bob", 45, "Sales"),
    ("Charlie", 28, "Engineering"),
    ("Diana", 32, "Marketing"),
    ("Eve", 29, "Engineering")
]

columns = ["name", "age", "department"]

df = spark.createDataFrame(data, columns)

print("Sample DataFrame created:")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Data Transformations

# COMMAND ----------

# Filter engineering department
engineering_df = df.filter(col("department") == "Engineering")

print("Engineering employees:")
engineering_df.show()

# Calculate average age by department
avg_age_df = df.groupBy("department").avg("age")

print("Average age by department:")
avg_age_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Widget Parameters (Optional)

# COMMAND ----------

# Create a widget for parameterized execution
dbutils.widgets.text("environment", "dev", "Environment")
environment = dbutils.widgets.get("environment")

print(f"Running in environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Results

# COMMAND ----------

# Count records
record_count = df.count()
print(f"Total records: {record_count}")

# Assertions for testing
assert record_count == 5, f"Expected 5 records, got {record_count}"
assert engineering_df.count() == 3, "Expected 3 engineering employees"

print("✅ All tests passed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Add any cleanup code here if needed
print("Test notebook execution completed successfully!")
