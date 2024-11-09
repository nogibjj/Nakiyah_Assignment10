"""
library functions
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc
import matplotlib.pyplot as plt


# # Setting environment variables
# os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
# os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# print(os.environ["JAVA_HOME"])

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

# Function 2: Generate summary statistics for a specific column
def summaryStatistics(df, columns):
    """Generate summary statistics for the specified columns"""
    stats = {}
    for Col in columns:
        selected_column_df = df.select(Col)
        
        # Calculate summary statistics
        SumStats = selected_column_df.describe().toPandas()  # Converts to Pandas for viewing
        Median = selected_column_df.approxQuantile(Col, [0.5], 0.05)[0]  # Approximate median
        Mean = selected_column_df.agg({Col: "mean"}).collect()[0][0]  # Calculate mean

        # Prepare the statistics in a readable format
        stats[Col] = {
            "Summary Statistics": SumStats.to_markdown(),  # Converts to markdown for clarity
            "Median": Median,
            "Mean": Mean
        }

    # Logging the summary statistics with improved formatting
    log_output("summary statistics", "\n".join([f"**{col}**:\n{stats[col]['Summary Statistics']}\nMedian: {stats[col]['Median']}\nMean: {stats[col]['Mean']}\n" for col in stats]))

    return stats

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


# Function 4: Generate a pie chart from a specified column
def PiePlot(df, col, labels_col):
    """Generate a pie chart from a specified column"""
    # Filter out null values and sort data by the specified column
    data = df.select(col, labels_col).na.drop()
    data_sorted = data.orderBy(desc(col))

    # Convert to Pandas for plotting (PySpark doesn’t support direct plotting)
    pandas_df = data_sorted.toPandas()
    values = pandas_df[col].values
    labels = pandas_df[labels_col].values

    # Plot the pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=90, labeldistance=1.05)
    plt.title(f"Breakdown of {col} by {labels_col}", pad=40)
    plt.axis("equal")
    plt.show()

    return "Pie Chart displayed"
