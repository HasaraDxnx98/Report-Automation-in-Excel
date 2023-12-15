# Import required libraries
import pandas as pd
import numpy as np
from tqdm import tqdm
import warnings

# User input of file name
file_Name = str(input("Enter the file name: "))

# Set no. of steps in progress bar for the process
no_Steps = 10
# Create a progress bar
progress_bar = tqdm(total=no_Steps, desc="Processing")

# Read the excel file, remove the warnings
with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")
    # Read the KPIs sheet of the given excel file
    df_KPI_GSM = pd.read_excel(
        file_Name,
        sheet_name="KPIs",
        engine="openpyxl",
        dtype=object,
        header=None,
    )
    # Read the bts_trx_cell sheet of the given excel file
    df_BTS_TRX_CELL = pd.read_excel(
        file_Name,
        sheet_name="bts_trx_cell",
        engine="openpyxl",
        dtype=object,
        header=None,
    )
# set progress bar to 1
progress_bar.update(1)

# Rename the columns of the dataframe df_KPI_GSM
df_KPI_GSM = df_KPI_GSM.rename(
    columns={
        0: "Date",
        1: "GBSC",
        2: "Intergrity",
        3: "SR_IA",
        4: "SR_TCHA_BSC",
        5: "O_HO_SR",
        6: "I_HO_SR",
        7: "SPC_DR",
        8: "TCHC_DR_IHO",
        9: "TCHC_DR_OHO",
        10: "CDR_SDCCH_BSC",
        11: "SPC",
        12: "TCH_CR",
        13: "SDCCH_CR",
        14: "TCH_T",
        15: "TCH_TV",
        16: "CT_HR",
    }
)
# Drop the first row of the dataframe df_KPI_GSM
df_KPI_GSM = df_KPI_GSM.drop(0)
# set progress bar to 2
progress_bar.update(1)

# Rename the columns of the dataframe df_BTS_TRX_CELL
df_BTS_TRX_CELL = df_BTS_TRX_CELL.rename(
    columns={
        0: "Date",
        1: "GBSC",
        2: "Intergrity",
        3: "No_BTSs_BSC",
        4: "No_TRXs_BSC",
        5: "No_Cells_BSC",
    }
)
# Drop the first row of the dataframe df_BTS_TRX_CELL
df_BTS_TRX_CELL = df_BTS_TRX_CELL.drop(0)
# set progress bar to 3
progress_bar.update(1)

# Calculate the average of SR_IA and SR_TCHA_BSC
df_KPI_GSM["CS_SR_BSC"] = (df_KPI_GSM["SR_IA"] + df_KPI_GSM["SR_TCHA_BSC"]) / 2
# Calculate the average of O_HO_SR and I_HO_SR
df_KPI_GSM["BSC_HO_SR"] = (df_KPI_GSM["O_HO_SR"] + df_KPI_GSM["I_HO_SR"]) / 2

# Create a pivot table for the dataframe df_KPI_GSM as PT_df_KPI_GSM using separate aggregation functions for each column
PT_df_KPI_GSM = pd.pivot_table(
    df_KPI_GSM,
    index="GBSC",
    values=[
        "SR_IA",
        "SR_TCHA_BSC",
        "CS_SR_BSC",
        "O_HO_SR",
        "I_HO_SR",
        "BSC_HO_SR",
        "SPC_DR",
        "TCHC_DR_IHO",
        "TCHC_DR_OHO",
        "CDR_SDCCH_BSC",
        "SPC",
        "TCH_CR",
        "SDCCH_CR",
        "TCH_T",
        "TCH_TV",
        "CT_HR",
    ],
    aggfunc={
        "SR_IA": np.average,
        "SR_TCHA_BSC": np.average,
        "CS_SR_BSC": np.average,
        "O_HO_SR": np.average,
        "I_HO_SR": np.average,
        "BSC_HO_SR": np.average,
        "SPC_DR": np.average,
        "TCHC_DR_IHO": np.average,
        "TCHC_DR_OHO": np.average,
        "CDR_SDCCH_BSC": np.average,
        "SPC": np.average,
        "TCH_CR": np.average,
        "SDCCH_CR": np.average,
        "TCH_T": np.average,
        "TCH_TV": np.average,
        "CT_HR": np.average,
    },
)
# set progress bar to 4
progress_bar.update(1)

# Create a pivot table for the dataframe df_BTS_TRX_CELL as PT_df_BTS_TRX_CELL using separate aggregation functions for each column
PT_df_BTS_TRX_CELL = pd.pivot_table(
    df_BTS_TRX_CELL,
    index="GBSC",
    values=["No_BTSs_BSC", "No_TRXs_BSC", "No_Cells_BSC"],
    aggfunc={
        "No_BTSs_BSC": np.average,
        "No_TRXs_BSC": np.average,
        "No_Cells_BSC": np.average,
    },
)
# set progress bar to 5
progress_bar.update(1)

# Create a pivot table for the dataframe df_KPI_GSM as PT_df_KPI_GSM_Extra using separate aggregation functions for each column
PT_df_KPI_GSM_Extra = pd.pivot_table(
    df_KPI_GSM,
    index="GBSC",
    values=[
        "O_HO_SR",
        "I_HO_SR",
        "SPC_DR",
        "SPC",
        "SDCCH_CR",
        "TCH_T",
        "CT_HR",
    ],
    aggfunc={
        "O_HO_SR": np.average,
        "I_HO_SR": np.average,
        "SPC_DR": np.average,
        "SPC": np.average,
        "SDCCH_CR": np.average,
        "TCH_T": [np.average, np.max],
        "CT_HR": np.average,
    },
)
# set progress bar to 6
progress_bar.update(1)

# Creating the transpose of the pivot table PT_df_KPI_GSM as transposed_PT_df_KPI_GSM
transposed_PT_df_KPI_GSM = PT_df_KPI_GSM.transpose()

# Creating merged dataframe Table_AccessNetwork by merging PT_df_BTS_TRX_CELL and PT_df_KPI_GSM_Extra
with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")
    Table_AccessNetwork = PT_df_BTS_TRX_CELL.merge(
        PT_df_KPI_GSM_Extra, how="left", on="GBSC"
    )
# set progress bar to 7
progress_bar.update(1)

# Create excel sheet for the merged transposed_PT_df_KPI_GSM dataframe, saving it as Huawei2G_Report.xlsx
transposed_PT_df_KPI_GSM.to_excel("Huawei2G_Report.xlsx", sheet_name="KPIs", index=True)
# Write Table_AccessNetwork dataframe to the excel sheet Huawei2G_Report.xlsx in sheet Table_AccessNetwork
with pd.ExcelWriter("Huawei2G_Report.xlsx", mode="a") as writer:
    Table_AccessNetwork.to_excel(writer, sheet_name="Table_AccessNetwork", index=True)
# set progress bar to 8
progress_bar.update(1)

# Rename the columns of the dataframe Table_AccessNetwork
Table_AccessNetwork = Table_AccessNetwork.rename(
    columns={
        "GBSC": "GBSC",
        "No_BTSs_BSC": "No_BTSs_BSC",
        "No_Cells_BSC": "No_Cells_BSC",
        "No_TRXs_BSC": "No_TRXs_BSC",
        ("CT_HR", "average"): "CT_HR",
        ("I_HO_SR", "average"): "I_HO_SR",
        ("O_HO_SR", "average"): "O_HO_SR",
        ("SDCCH_CR", "average"): "SDCCH_CR",
        ("SPC", "average"): "SPC",
        ("SPC_DR", "average"): "SPC_DR",
        ("TCH_T", "amax"): "TCH_T_Max",
        ("TCH_T", "average"): "TCH_T",
    }
)
# set progress bar to 9
progress_bar.update(1)

# Write Table_AccessNetwork dataframe to the excel sheet Huawei2G_Report.xlsx in sheet Table_AccessNetwork_renamed
with pd.ExcelWriter("Huawei2G_Report.xlsx", mode="a") as writer:
    Table_AccessNetwork.to_excel(
        writer, sheet_name="Table_AccessNetwork_renamed", index=True
    )
# set progress bar to 10
progress_bar.update(1)
# Close the progress bar
progress_bar.close()

# Print the success message
print("\nHuawei report generation is successful!\n")