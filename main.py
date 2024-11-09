"""
Main CLI or app entry point
"""

from mylib.lib import (
    readData,
    summaryStatistics,
    cleanData,
    PiePlot,
    start_spark,
    end_spark,
)

def main():
    # Define the file path to your CSV data
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"  # Replace this with the actual path to your CSV file
    
    # Start Spark session
    spark = start_spark("DataProcessing")
    
    # Read data into dataframe
    df = readData(spark, file_path)
    
    # Generate summary statistics for a list of columns (you can specify the columns you want here)
    stats = summaryStatistics(df, columns=["YEAR", "GoogleKnowlege_Occupation"])
    print(stats)  # You can log or handle stats however you like
    
    # Clean and sort data (example: sorting by "YEAR" and selecting top 5 entries)
    cleaned_data = cleanData(df, ColToSort="YEAR", Columns=["YEAR", "GoogleKnowlege_Occupation"], RanksRequired=5)
    cleaned_data.show()

    # Generate a pie chart (example: pie chart for the 'GoogleKnowlege_Occupation' column)
    PiePlot(df, col="YEAR", labels_col="GoogleKnowlege_Occupation")
    
    # End Spark session
    end_spark(spark)

if __name__ == "__main__":
    main()
