from mylib.lib import (
    readData,
    cleanData,
    summaryStatistics,
    PiePlot,
)

Data = "FT Global Business School MBA Ranking 2024.csv"
ReadData = readData(Data)


ColumnsWantedForSummaryStats = [
    "Value for money rank",
    "Salary percentage increase",
    "Overall satisfaction **",
]
SummaryStatistics, Median, Mean = summaryStatistics(
    ReadData, ColumnsWantedForSummaryStats
)
print(SummaryStatistics)


def save_to_markdown(df, col):
    """save summary report to markdown"""
    markdown_table1, markdown_table2, markdown_table3 = summaryStatistics(df, col)
    markdown_table1 = str(markdown_table1)
    markdown_table2 = str(markdown_table2)
    markdown_table3 = str(markdown_table3)
    # Write the markdown table to a file
    with open("summary_report_generated.md", "w", encoding="utf-8") as file:
        file.write("Describe:\n")
        file.write(markdown_table1)
        file.write("\n\n")  # Add a new line
        file.write("Median:\n")
        file.write(markdown_table2)
        file.write("\n\n")  # Add a new line
        file.write("Mean:\n")
        file.write(markdown_table3)
        file.write("\n\n")  # Add a new line


ColumnsForDataset = [
    "#",
    "School Name",
    "International students (%)",
    "International faculty (%)",
    "Value for money rank",
    "Career progress rank",
    "Careers service rank",
]
Rank = "#"  # Column for sorting
requiredrank = 10
PctIntlStudents = "International students (%)"
PctIntlFaculty = "International faculty (%)"
SchoolName = "School Name"
RankNames = "Value for money rank", "Career progress rank", "Careers service rank"

CleanData = cleanData(ReadData, Rank, ColumnsForDataset, requiredrank)
piePlotStudents = PiePlot(CleanData, PctIntlStudents, SchoolName)
piePlotFaculty = PiePlot(CleanData, PctIntlFaculty, SchoolName)
save_to_markdown(ReadData, ColumnsWantedForSummaryStats)
