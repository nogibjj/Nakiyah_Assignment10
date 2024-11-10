"""
Main CLI or app entry point
"""

from mylib.lib import (
    readData,
    summaryStatistics,
    cleanData,
    queryData,
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
    stats = summaryStatistics(
        df,
        columns=[
            "Value for money rank",
            "Salary percentage increase",
            "Overall satisfaction **",
        ],
    )
    print(stats)  # You can log or handle stats however you like

    # Clean and sort data (example: sorting by "YEAR" and selecting top 5 entries)
    cleaned_data = cleanData(
        df,
        ColToSort="#",
        Columns=[
            "#",
            "School Name",
            "International students (%)",
            "International faculty (%)",
            "Value for money rank",
            "Career progress rank",
            "Careers service rank",
        ],
        RanksRequired=5,
    )

    cleaned_data.show()

    # Query the top 20 schools based on a ranking column (e.g., "#")
    query = "SELECT * FROM business_data ORDER BY `#` LIMIT 20"
    top_schools = queryData(spark, df, query)
    top_schools.show()  # Display the top 20 schools

    # End Spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
