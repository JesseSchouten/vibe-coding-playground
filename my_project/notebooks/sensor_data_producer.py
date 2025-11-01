# Databricks notebook source
# MAGIC %md
# MAGIC # Sensor Data Producer Notebook
# MAGIC
# MAGIC This notebook generates synthetic sensor event data and writes it to a Databricks volume.
# MAGIC
# MAGIC ## Parameters
# MAGIC - `volume_path`: Path to the Databricks volume (landing zone)
# MAGIC - `num_datapoints`: Number of JSON data points to generate
# MAGIC - `filename_prefix`: Prefix for the output filename (default: "sensor_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Import Libraries

# COMMAND ----------

# Import the data producer module from the centralized library
from my_project.data_producer import generate_and_write_sensor_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

# Widget for volume path
dbutils.widgets.text("volume_path", "/Volumes/catalog/schema/volume/landing_zone", "Volume Path")

# Widget for number of datapoints
dbutils.widgets.text("num_datapoints", "100", "Number of Datapoints")

# Widget for filename prefix
dbutils.widgets.text("filename_prefix", "sensor_data", "Filename Prefix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameter Values

# COMMAND ----------

# Get parameter values from widgets
volume_path = dbutils.widgets.get("volume_path")
num_datapoints = int(dbutils.widgets.get("num_datapoints"))
filename_prefix = dbutils.widgets.get("filename_prefix")

# Display parameters
print(f"Volume Path: {volume_path}")
print(f"Number of Datapoints: {num_datapoints}")
print(f"Filename Prefix: {filename_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate and Write Data

# COMMAND ----------

try:
    # Generate sensor data and write to volume
    output_path = generate_and_write_sensor_data(
        count=num_datapoints,
        volume_path=volume_path,
        filename_prefix=filename_prefix
    )
    
    print(f"\n✓ Successfully generated {num_datapoints} sensor data points")
    print(f"✓ Data written to: {output_path}")
    
    # Return the output path for downstream tasks
    dbutils.notebook.exit(output_path)
    
except Exception as e:
    print(f"✗ Error generating sensor data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Format Reference
# MAGIC
# MAGIC Each data point has the following structure:
# MAGIC ```json
# MAGIC {
# MAGIC   "timestamp": "2025-11-01T12:34:56.789012",
# MAGIC   "event": "sensor1",
# MAGIC   "value": 42
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Where:
# MAGIC - `timestamp`: ISO format datetime of when the data point was generated
# MAGIC - `event`: One of "sensor1", "sensor2", or "sensor3"
# MAGIC - `value`: Integer > 0, or null (with ~1% probability)
