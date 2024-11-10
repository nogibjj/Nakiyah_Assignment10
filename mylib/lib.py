"""
library functions
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F


# Setting environment variables
import os
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
print(os.environ["JAVA_HOME"])

LOG_FILE = "pyspark_output.md"

def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName="DataProcessing"):
    """Start a Spark session"""
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.conf.set("spark.sql.debug.maxToStringFields", "100")
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def end_spark(spark):
    """Stop the Spark session"""
    spark.stop()
    return "stopped spark session"

# Function 1: Read CSV data into a Spark DataFrame
def readData(spark, filepath):
    """Read CSV data into a Spark DataFrame"""
    df = spark.read.csv(filepath, header=True, inferSchema=True, encoding="ISO-8859-1")
    log_output("read data", df.limit(10).toPandas().to_markdown())
    return df


def summaryStatistics(df, columns):
    # Assume df is the DataFrame and columns is a list of column names
    stats = []

    for col_name in columns:
        # Ensure column names are enclosed in backticks for SQL compatibility
        mean_col = F.avg(col_name).alias("Mean")
        median_col = F.expr(f"percentile_approx(`{col_name}`, 0.5)").alias("Median")
        max_col = F.max(col_name).alias("Max")
        min_col = F.min(col_name).alias("Min")

        stats.append(df.select(mean_col, median_col, max_col, min_col))

    # Combine the results for all columns (if needed)
    result = stats[0]  # Just as an example, you may want to union or join these results
    return result



# Function 3: Clean and sort data, selecting specific columns and ranks
def cleanData(df, ColToSort, Columns, RanksRequired):
    """Clean and sort data by specified column, selecting specific columns and ranks"""
    # Sort by specified column in ascending order and select specified columns
    sorted_df = df.orderBy(col(ColToSort))
    selected_df = sorted_df.select(Columns).limit(RanksRequired)
    
    log_output("clean data", selected_df.limit(10).toPandas().to_markdown())
    return selected_df

def queryData(spark, df, query):
    """Run a sample Spark SQL query"""
    df.createOrReplaceTempView("business_data")
    result = spark.sql(query)
    log_output("SQL query", result.toPandas().to_markdown(), query)
    return result

