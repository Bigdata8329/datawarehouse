#!/bin/bash

# Set environment variables
export SPARK_HOME=/path/to/spark
export PATH=$SPARK_HOME/bin:$PATH

# Run PySpark script
spark-submit --files config.yaml my_pyspark_script.py
