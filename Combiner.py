# Importing the required libraries
import pandas as pd
from tqdm import tqdm
import os

# Exucute Huawei2G_KPI_Report_Generator.py
current_directory = os.path.dirname(os.path.abspath(__file__))
file_path_huawei = os.path.join(current_directory, "Huawei2G_KPI_Report_Generator.py")
os.system(f'python "{file_path_huawei}"')

# Execute ZTE2G_KPI_Report_Generator.py
current_directory = os.path.dirname(os.path.abspath(__file__))
file_path_zte = os.path.join(current_directory, "ZTE2G_KPI_Report_Generator.py")
os.system(f'python "{file_path_zte}"')

# Label for Combining Huawei and ZTE 2G KPI reports
print("\nCombining Huawei and ZTE 2G KPI reports...\n")

# Set no. of steps in progress bar for the process
No_Steps = 10
# Create a progress bar
progress_bar = tqdm(total=No_Steps, desc="Processing")

# Read the excel file from Huawei2G_KPI_Report_Generator.py in sheet Table_AccessNetwork_renamed
df_Huawei = pd.read_excel(
    "Huawei2G_Report.xlsx",
    sheet_name="Table_AccessNetwork_renamed",
    engine="openpyxl",
    dtype=object,
    header=None,
)
# Read the excel file from ZTE2G_KPI_Report_Generator.py in sheet Table_AccessNetwork_renamed
progress_bar.update(1)
df_ZTE = pd.read_excel(
    "ZTE2G_Report.xlsx",
    sheet_name="Table_AccessNetwork_renamed",
    engine="openpyxl",
    dtype=object,
    header=None,
)
# set progress bar to 1
progress_bar.update(1)

# Rename the columns of the dataframes df_Huawei
df_Huawei = df_Huawei.rename(
    columns={
        0: "GBSC",
        1: "No_BTSs_BSC",
        2: "No_Cells_BSC",
        3: "No_TRXs_BSC",
        4: "CT_HR",
        5: "I_HO_SR",
        6: "O_HO_SR",
        7: "SDCCH_CR",
        8: "SPC",
        9: "SPC_DR",
        10: "TCH_T_Max",
        11: "TCH_T",
    }
)
# Drop the first row of the dataframe df_Huawei
df_Huawei = df_Huawei.drop(0)
# set progress bar to 2
progress_bar.update(1)

# Rename the columns of the dataframes df_ZTE
df_ZTE = df_ZTE.rename(
    columns={
        0: "GBSC",
        1: "CT_HR",
        2: "I_HO_SR",
        3: "O_HO_SR",
        4: "SDCCH_CR",
        5: "SPC",
        6: "SPC_DR",
        7: "No_Cells_BSC",
        8: "No_BTSs_BSC",
        9: "TCH_T_Max",
        10: "No_TRXs_BSC",
        11: "TCH_T",
    }
)
# Drop the first row of the dataframe df_ZTE
df_ZTE = df_ZTE.drop(0)
# set progress bar to 3
progress_bar.update(1)

# Identify common columns between the two dataframes
common_columns = set(df_Huawei.columns) & set(df_ZTE.columns)

# Combine similar columns separately
df1_common = df_Huawei[list(common_columns)]
df2_common = df_ZTE[list(common_columns)]

# Concatenate the results
combined_common = pd.concat([df1_common, df2_common], ignore_index=True)

# Combine the remaining columns separately
df1_remaining = df_Huawei.drop(columns=common_columns)
df2_remaining = df_ZTE.drop(columns=common_columns)

# Concatenate the remaining columns
combined_remaining = pd.concat(
    [df1_remaining, df2_remaining], ignore_index=True, axis=1
)

# Combine the results
result = pd.concat([combined_common, combined_remaining], axis=1)

# Display the final result
result_end = result[list(common_columns)]
progress_bar.update(1)

# Define the desired column order
desired_columns = [
    "GBSC",
    "TCH_T",
    "TCH_T_Max",
    "No_BTSs_BSC",
    "No_TRXs_BSC",
    "No_Cells_BSC",
    "CT_HR",
    "SPC",
    "SDCCH_CR",
    "SPC_DR",
    "O_HO_SR",
    "I_HO_SR",
]

# Set the progress bar to 4
progress_bar.update(1)
# Rearrange the columns
result_end = result_end.reindex(columns=desired_columns)
# Set the progress bar to 5
progress_bar.update(1)

# Copy the dataframe result_end to result_end_rounded
result_end_rounded = result_end.copy()
# Set columns to float and round neccessary columns to 2 decimal places
result_end_rounded["TCH_T"] = result_end_rounded["TCH_T"].astype(float).round(2)
result_end_rounded["TCH_T_Max"] = result_end_rounded["TCH_T_Max"].astype(float).round(2)
result_end_rounded["No_BTSs_BSC"] = (
    result_end_rounded["No_BTSs_BSC"].astype(float).round(0)
)
result_end_rounded["No_TRXs_BSC"] = (
    result_end_rounded["No_TRXs_BSC"].astype(float).round(0)
)
result_end_rounded["No_Cells_BSC"] = (
    result_end_rounded["No_Cells_BSC"].astype(float).round(0)
)
result_end_rounded["CT_HR"] = result_end_rounded["CT_HR"].astype(float).round(2)
result_end_rounded["SPC"] = result_end_rounded["SPC"].astype(float).round(2)
result_end_rounded["SDCCH_CR"] = result_end_rounded["SDCCH_CR"].astype(float).round(2)
result_end_rounded["SPC_DR"] = result_end_rounded["SPC_DR"].astype(float).round(2)
result_end_rounded["O_HO_SR"] = result_end_rounded["O_HO_SR"].astype(float).round(2)
result_end_rounded["I_HO_SR"] = result_end_rounded["I_HO_SR"].astype(float).round(2)
# Set the progress bar to 6
progress_bar.update(1)
# Set the progress bar to 7
progress_bar.update(1)

# Write the final result to an excel file
result_end_rounded.to_excel("result_Final_Report.xlsx", index=False)

# Set the progress bar to 8
progress_bar.update(1)
# Close the progress bar
progress_bar.close()

# Label for the final report completion
print("\nSuccessfully generated the final report!\n")

# Response = str(input("Exit (y/n): "))
