"""
library functions
"""
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

"""
Functions library
"""
# Function 1
def readData(df):
    return pl.read_csv(df, encoding="ISO-8859-1")

# Function 2
def summaryStatistics(df, Col):
    df = df[Col]
    SumStats = df.describe()
    Median = df.median()
    Mean = df.mean()
    return SumStats, Median, Mean

# Function 3
def cleanData(df, ColToSort, Columns, RanksRequired):
    df = df.with_columns(pl.col(ColToSort))
    df1 = df.sort(by=ColToSort)
    df1 = df1.select(Columns).head(RanksRequired)
    
    return df1

def PiePlot(df, col, labels_col):
    data = df.select([col, labels_col]).drop_nulls()
    data = data.sort(col, descending=True)
    values = data[col].to_numpy()
    labels = data[labels_col].to_list()
    
    # Plot the pie chart
    plt.figure(figsize=(8, 8))
    plt.pie(values, labels=labels, autopct="%1.1f%%", startangle=90, labeldistance=1.05)
    plt.title(f"Breakdown of {col} by School", pad=40)
    plt.axis("equal")
    plt.show()
    
    return "Pie Chart displayed"
