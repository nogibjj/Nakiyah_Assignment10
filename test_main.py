import pytest
import pyspark
from mylib.lib import (
    readData,
    summaryStatistics,
    cleanData,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_readData(spark):
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"
    df = readData(spark, file_path)
    assert df is not None
    assert df.count() > 0  # Check that the dataframe is not empty


def test_summaryStatistics(spark):
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"
    df = readData(spark, file_path)

    # Call summaryStatistics and get the stats DataFrame
    stats_df = summaryStatistics(
        df,
        columns=[
            "Value for money rank",
            "Salary percentage increase",
            "Overall satisfaction **",
        ],
    )

    # Print the columns to debug the structure
    print(stats_df.columns)

    # Check that the result is a DataFrame
    assert stats_df is not None
    assert isinstance(stats_df, pyspark.sql.dataframe.DataFrame)


def test_cleanData(spark):
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"
    df = readData(spark, file_path)
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
    assert cleaned_data is not None
    assert cleaned_data.count() > 0  # Ensure that the cleaned data has rows


if __name__ == "__main__":
    pytest.main()
