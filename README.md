# Nakiyah_Assignment10
[![CICD](https://github.com/nogibjj/Nakiyah_Assignment10/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Nakiyah_Assignment10/actions/workflows/cicd.yml)

# Data Processing with PySpark
This project demonstrates fundamental PySpark operations, including loading, describing, querying, and transforming data for scalable data processing.

## Prerequisites
### 1. Setting Up Spark Locally
To run this project locally, you’ll need to install Apache Spark. Follow the official [Apache Spark Installation Guide](https://spark.apache.org/docs/latest/) to download and set up Spark. Make sure to configure environment variables (e.g., `JAVA_HOME` and `SPARK_LOCAL_IP`) as needed for your system.

### 2. Additional Dependencies
Install required Python packages listed in `requirements.txt` by running:
`pip3 install -r requirements.txt`

# `lib.py` - Library Functions for Data Processing and Visualization
This module contains essential functions for data processing, analysis, and visualization using **PySpark** and **Matplotlib**. Below is an overview of each function:

## 1. Logging Functions
- `log_output(operation, output, query=None)`: Logs operations to `pyspark_output.md`, including the operation name, output, and optional SQL query, creating a traceable audit log.

## 2. Spark Session Management
- `start_spark(appName="DataProcessing")`: Starts a Spark session, setting configurations for large data processing.
- `end_spark(spark)`: Stops the Spark session and confirms termination.

## 3. Data Loading
- `readData(spark, filepath)`: Loads CSV data into a Spark DataFrame with schema inference and specified encoding. Logs a data preview.

## 4. Summary Statistics
- `summaryStatistics(df, columns)`: Computes and logs mean and approximate median statistics for specified columns, outputting in markdown format.

## 5. Data Cleaning and Sorting
- `cleanData(df, ColToSort, Columns, RanksRequired)`: Sorts data by a specified column, selects specific columns, and retrieves a limited number of rows, logging a preview.

## 6. SQL Query Execution
- `queryData(spark, df, query)`: Runs SQL queries on the DataFrame, logging the query and its output for easy exploration.

## Last
- Finally the output can be viewed in 'pyspark_output.md' file
Each function supports a modular, structured approach to data processing, ensuring reproducibility and efficient data workflows.
