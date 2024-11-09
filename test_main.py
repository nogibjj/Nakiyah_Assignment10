import pytest
from mylib.lib import (
    readData,
    summaryStatistics,
    cleanData,
    PiePlot,
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
    stats = summaryStatistics(df, columns=["Value for money rank", "Salary percentage increase", "Overall satisfaction **"])
    assert stats is not None
    assert isinstance(stats, dict)  # Assuming stats returns a dictionary

def test_cleanData(spark):
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"
    df = readData(spark, file_path)
    cleaned_data = cleanData(df, ColToSort="#", Columns=["#", "School Name", "International students (%)", "International faculty (%)",
                                                         "Value for money rank", "Career progress rank", "Careers service rank"], 
                             RanksRequired=5)
    assert cleaned_data is not None
    assert cleaned_data.count() > 0  # Ensure that the cleaned data has rows

def test_PiePlot(spark):
    file_path = "data/FT Global Business School MBA Ranking 2024.csv"
    df = readData(spark, file_path)
    cleaned_data = cleanData(df, ColToSort="#", Columns=["#", "School Name", "International students (%)", "International faculty (%)",
                                                         "Value for money rank", "Career progress rank", "Careers service rank"], 
                             RanksRequired=5)
    
    # This is just to check if the PiePlot function runs without errors
    try:
        PiePlot(cleaned_data, col="International students (%)", labels_col="School Name")
        plot_successful = True
    except Exception as e:
        plot_successful = False
        print(f"Error during PiePlot: {e}")
    
    assert plot_successful is True

if __name__ == "__main__":
    pytest.main()
