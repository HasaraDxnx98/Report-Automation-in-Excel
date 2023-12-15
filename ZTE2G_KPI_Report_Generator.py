# import required libraries
import pandas as pd
import numpy as np
from tqdm import tqdm

# User input of first data file name
file_Name1 = str(input("Enter the Data file 1 name: "))
# User input of second data file name
file_Name2 = str(input("Enter the Data file 2 name: "))
# User input of third data file name
file_Name3 = str(input("Enter the Data file 3 name: "))

# Set no. of steps in progress bar for the process
no_Steps = 10
# Create a progress bar
progress_bar = tqdm(total=no_Steps, desc="Processing")

# Read the first table from the first data file
df_Data1 = pd.read_excel(
    file_Name1,
    engine="openpyxl",
    dtype=object,
    header=None,
)
# Read the second table from the second data file
df_Data2 = pd.read_excel(
    file_Name2,
    engine="openpyxl",
    dtype=object,
    header=None,
)
# Read the third table from the third data file
df_Data3 = pd.read_excel(
    file_Name3,
    engine="openpyxl",
    dtype=object,
    header=None,
)
# set progress bar to 1
progress_bar.update(1)

# Rename the columns of the dataframe df_Data1
df_Data1 = df_Data1.rename(
    columns={
        0: "Index",
        1: "Start_Time",
        2: "End_Time",
        3: "Query_Granularity",
        4: "GBSC",
        5: "SubNetwork_Name",
        6: "ME",
        7: "ME_Name",
        8: "PEAK_TCH_T",
        9: "No_TRX",
    }
)
# Drop the first row of the dataframe df_Data1
df_Data1 = df_Data1.drop(0)
# set progress bar to 2
progress_bar.update(1)

# Rename the columns of the dataframe df_Data2
df_Data2 = df_Data2.rename(
    columns={
        0: "Index",
        1: "Start_Time",
        2: "End_Time",
        3: "Query_Granularity",
        4: "GBSC",
        5: "SubNetwork_Name",
        6: "ME",
        7: "ME_Name",
        8: "SITE",
        9: "SITE_NAME",
        10: "BTS",
        11: "BTS_NAME",
        12: "Location_LAC",
        13: "CI",
        14: "CELL_LAT",
        15: "CELL_LONG",
        16: "ANTENNA_HEIGHT",
        17: "ANTENNA_AZIMUTH",
        18: "CELL_ADDRESS",
        19: "Peak_TCH_T",
    }
)
# Drop the first row of the dataframe df_Data2
df_Data2 = df_Data2.drop(0)
# set progress bar to 3
progress_bar.update(1)

# Rename the columns of the dataframe df_Data3
df_Data3 = df_Data3.rename(
    columns={
        0: "Index",
        1: "Start_Time",
        2: "End_Time",
        3: "Query_Granularity",
        4: "GBSC",
        5: "SubNetwork_Name",
        6: "ME",
        7: "ME_Name",
        8: "SR_IA",
        9: "SR_TCHA_BSC",
        10: "CS_SR_BSC",
        11: "BSC_HO_SR",
        12: "O_HO_SR",
        13: "I_HO_SR",
        14: "SPC_DR",
        15: "TCH_DR_IHO",
        16: "TCH_DR_EHO",
        17: "TCH_DR_RLF",
        18: "TCH_DR_HOL",
        19: "DLRQ",
        20: "ULRQ",
        21: "SPC",
        22: "TCH_CR",
        23: "SDCCH_CR",
        24: "ATCH_TN_CH",
        25: "TCH_T",
        26: "TCH_TV",
        27: "CT_HR",
    }
)
# Drop the first row of the dataframe df_Data3
df_Data3 = df_Data3.drop(0)
# set progress bar to 4
progress_bar.update(1)

# Select the required columns from the dataframe df_Data3 to multiply by 100
columns_to_multiply = [
    "SR_IA",
    "SR_TCHA_BSC",
    "CS_SR_BSC",
    "BSC_HO_SR",
    "O_HO_SR",
    "I_HO_SR",
    "SPC_DR",
    "TCH_DR_IHO",
    "TCH_DR_EHO",
    "TCH_DR_RLF",
    "TCH_DR_HOL",
    "DLRQ",
    "ULRQ",
    "SPC",
    "TCH_CR",
    "SDCCH_CR",
    "ATCH_TN_CH",
    "CT_HR",
]
# Multiply the selected columns by 100
df_Data3[columns_to_multiply] = df_Data3[columns_to_multiply].multiply(100)

# Create a pivot table for the dataframe df_Data1 as PT_df_Data1 using separate aggregation functions for each column
PT_df_Data1 = pd.pivot_table(
    df_Data1,
    index="GBSC",
    values=["PEAK_TCH_T", "No_TRX"],
    aggfunc={"PEAK_TCH_T": np.average, "No_TRX": np.max},
)
# set progress bar to 5
progress_bar.update(1)

# Drop the duplicate rows from the dataframe df_Data2 considering the columns SITE_NAME and BTS_NAME
df_Data2_T1 = df_Data2.drop_duplicates(subset=["SITE_NAME", "BTS_NAME"], keep="first")
# Create a pivot table for the dataframe df_Data2_T1 as PT_df_Data2_T1 using separate aggregation functions for each column
PT_df_Data2_T1 = pd.pivot_table(
    df_Data2_T1,
    index="GBSC",
    values=["SITE_NAME", "BTS_NAME"],
    aggfunc={
        "SITE_NAME": np.count_nonzero,
        "BTS_NAME": np.count_nonzero,
    },
)
# set progress bar to 6
progress_bar.update(1)

# Drop the duplicate rows from the dataframe df_Data2 considering the column SITE_NAME
df_Data2_T2 = df_Data2.drop_duplicates(subset=["SITE_NAME"], keep="first")
# Create a pivot table for the dataframe df_Data2_T2 as PT_df_Data2_T2 using separate aggregation functions for each column
PT_df_Data2_T2 = pd.pivot_table(
    df_Data2_T2,
    index="GBSC",
    values=["SITE_NAME"],
    aggfunc={"SITE_NAME": np.count_nonzero},
)
# set progress bar to 7
progress_bar.update(1)

# Create a pivot table for the dataframe df_Data2 as PT_Data2_Extra using separate aggregation functions for each column
PT_Data2_Extra = pd.pivot_table(
    df_Data2,
    index="BTS_NAME",
    values=["GBSC", "Peak_TCH_T"],
    aggfunc={"GBSC": np.max, "Peak_TCH_T": np.average},
)

# Create a pivot table for the dataframe PT_Data2_Extra as PT_df_Data2_T3 using separate aggregation functions for each column
PT_df_Data2_T3 = pd.pivot_table(
    PT_Data2_Extra, index="GBSC", values=["Peak_TCH_T"], aggfunc={"Peak_TCH_T": np.sum}
)
# Rename the column Peak_TCH_T as Sum_Peak_TCH_T in the dataframe PT_df_Data2_T3
PT_df_Data2_T3 = PT_df_Data2_T3.rename(columns={"Peak_TCH_T": "Sum_Peak_TCH_T"})
# set progress bar to 8
progress_bar.update(1)

# Create a pivot table for the dataframe df_Data3 as PT_df_Data3 using separate aggregation functions for each column
PT_df_Data3 = pd.pivot_table(
    df_Data3,
    index="GBSC",
    values=[
        "SR_IA",
        "SR_TCHA_BSC",
        "CS_SR_BSC",
        "BSC_HO_SR",
        "O_HO_SR",
        "I_HO_SR",
        "SPC_DR",
        "TCH_DR_IHO",
        "TCH_DR_EHO",
        "TCH_DR_RLF",
        "TCH_DR_HOL",
        "DLRQ",
        "ULRQ",
        "SPC",
        "TCH_CR",
        "SDCCH_CR",
        "ATCH_TN_CH",
        "TCH_T",
        "TCH_TV",
        "CT_HR",
    ],
    aggfunc={
        "SR_IA": np.average,
        "SR_TCHA_BSC": np.average,
        "CS_SR_BSC": np.average,
        "BSC_HO_SR": np.average,
        "O_HO_SR": np.average,
        "I_HO_SR": np.average,
        "SPC_DR": np.average,
        "TCH_DR_IHO": np.average,
        "TCH_DR_EHO": np.average,
        "TCH_DR_RLF": np.average,
        "TCH_DR_HOL": np.average,
        "DLRQ": np.average,
        "ULRQ": np.average,
        "SPC": np.average,
        "TCH_CR": np.average,
        "SDCCH_CR": np.average,
        "ATCH_TN_CH": np.average,
        "TCH_T": np.sum,
        "TCH_TV": np.sum,
        "CT_HR": np.average,
    },
)
# set progress bar to 9
progress_bar.update(1)

# transpose the dataframe PT_df_Data3
transposed_PT_df_Data3 = PT_df_Data3.transpose()
# Write the transposed dataframe transposed_PT_df_Data3 to an excel file ZTE2G_Report.xlsx in sheet1
transposed_PT_df_Data3.to_excel("ZTE2G_Report.xlsx", sheet_name="Sheet1", index=True)

# Create a pivot table for the dataframe df_Data3 as PT_df_Data3_AccessT using separate aggregation functions for each column
PT_df_Data3_AccessT = pd.pivot_table(
    df_Data3,
    index="GBSC",
    values=[
        "O_HO_SR",
        "I_HO_SR",
        "SPC_DR",
        "SPC",
        "SDCCH_CR",
        "CT_HR",
    ],
    aggfunc={
        "O_HO_SR": np.average,
        "I_HO_SR": np.average,
        "SPC_DR": np.average,
        "SPC": np.average,
        "SDCCH_CR": np.average,
        "CT_HR": np.average,
    },
)

# mearge the column BTS_NAME in dataframe PT_df_Data2_T1 with the dataframe PT_df_Data3_AccessT on the column GBSC
PT_df_Data32_AccessT = PT_df_Data3_AccessT.merge(
    PT_df_Data2_T1["BTS_NAME"], how="left", on="GBSC"
)
# mearge the column SITE_NAME in dataframe PT_df_Data2_T2 with the dataframe PT_df_Data32_AccessT on the column GBSC
PT_df_Data32_AccessT = PT_df_Data32_AccessT.merge(
    PT_df_Data2_T2["SITE_NAME"], how="left", on="GBSC"
)
# mearge the column Sum_Peak_TCH_T in dataframe PT_df_Data2_T3 with the dataframe PT_df_Data32_AccessT on the column GBSC
PT_df_Data32_AccessT = PT_df_Data32_AccessT.merge(
    PT_df_Data2_T3["Sum_Peak_TCH_T"], how="left", on="GBSC"
)
# mearge the columns in dataframe PT_df_Data1 with the dataframe PT_df_Data32_AccessT on the column GBSC
Table_AccessNetwork = PT_df_Data32_AccessT.merge(PT_df_Data1, how="left", on="GBSC")

# Write the dataframe Table_AccessNetwork to an excel file ZTE2G_Report.xlsx in sheet2
with pd.ExcelWriter("ZTE2G_Report.xlsx", mode="a") as writer:
    Table_AccessNetwork.to_excel(writer, sheet_name="Sheet2", index=True)

# Rename the columns of the dataframe Table_AccessNetwork
Table_AccessNetwork = Table_AccessNetwork.rename(
    columns={
        "GBSC": "GBSC",
        "SITE_NAME": "No_BTSs_BSC",
        "BTS_NAME": "No_Cells_BSC",
        "No_TRX": "No_TRXs_BSC",
        "CT_HR": "CT_HR",
        "I_HO_SR": "I_HO_SR",
        "O_HO_SR": "O_HO_SR",
        "SDCCH_CR": "SDCCH_CR",
        "SPC": "SPC",
        "SPC_DR": "SPC_DR",
        "Sum_Peak_TCH_T": "TCH_T_Max",
        "PEAK_TCH_T": "TCH_T",
    }
)
# write the dataframe Table_AccessNetwork to an excel file ZTE2G_Report.xlsx in sheet Table_AccessNetwork_renamed
with pd.ExcelWriter("ZTE2G_Report.xlsx", mode="a") as writer:
    Table_AccessNetwork.to_excel(
        writer, sheet_name="Table_AccessNetwork_renamed", index=True
    )
# set progress bar to 10
progress_bar.update(1)
# close the progress bar
progress_bar.close()

# Print the message of successful report generation
print("\nZTE report generation is successful!\n")
