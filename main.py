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
    stats = summaryStatistics(df, columns=["Value for money rank", "Salary percentage increase", "Overall satisfaction **"])
    print(stats)  # You can log or handle stats however you like
    
    # Clean and sort data (example: sorting by "YEAR" and selecting top 5 entries)
    cleaned_data = cleanData(df, ColToSort="#", Columns=["#", "School Name", "International students (%)", "International faculty (%)",
                                                         "Value for money rank", "Career progress rank", "Careers service rank"], 
                             RanksRequired=5)
    
    cleaned_data.show()

    # Generate a pie chart (example: pie chart for the 'GoogleKnowlege_Occupation' column)
    PiePlot(cleaned_data, col="International students (%)", labels_col="School Name")
    
    # End Spark session
    end_spark(spark)

if __name__ == "__main__":
    main()
