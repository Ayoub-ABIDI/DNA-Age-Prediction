import os
import gzip
import shutil
import pandas as pd

# --- CONFIGURATION ---
input_folder = "DataSet"  # folder containing your .csv.gz files
output_folder = "Cleaned_Dataset"  # folder to store extracted CSVs
merged_file_name = "GSE207605_merged.csv"  # final merged CSV
rename_map = {
    "GSE207605_GSE103657.csv.gz": "BloodAge_103657.csv",
    "GSE207605_GSE105018.csv.gz": "BloodAge_105018.csv",
    "GSE207605_GSE147221.csv.gz": "BloodAge_147221.csv",
    "GSE207605_GSE154566.csv.gz": "BloodAge_154566.csv",
    "GSE207605_GSE157131.csv.gz": "BloodAge_157131.csv",
    "GSE207605_GSE30870.csv.gz":  "BloodAge_30870.csv",
    "GSE207605_GSE36054.csv.gz":  "BloodAge_36054.csv",
    "GSE207605_GSE40279.csv.gz":  "BloodAge_40279.csv",
    "GSE207605_GSE41169.csv.gz":  "BloodAge_41169.csv",
    "GSE207605_GSE42861.csv.gz":  "BloodAge_42861.csv",
    "GSE207605_GSE51032.csv.gz":  "BloodAge_51032.csv",
    "GSE207605_GSE55763.csv.gz":  "BloodAge_55763.csv",
    "GSE207605_GSE64495.csv.gz":  "BloodAge_64495.csv",
    "GSE207605_GSE69270.csv.gz":  "BloodAge_69270.csv",
    "GSE207605_GSE72680.csv.gz":  "BloodAge_72680.csv",
    "GSE207605_GSE72775.csv.gz":  "BloodAge_72775.csv",
    "GSE207605_GSE73103.csv.gz":  "BloodAge_73103.csv",
    "GSE207605_GSE84727.csv.gz":  "BloodAge_84727.csv",
    "GSE207605_GSE87648.csv.gz":  "BloodAge_87648.csv",
}

# --- CREATE OUTPUT FOLDER ---
os.makedirs(output_folder, exist_ok=True)

# --- EXTRACT AND RENAME CSV FILES ---
for file in os.listdir(input_folder):
    if file.endswith(".csv.gz"):
        input_path = os.path.join(input_folder, file)
        output_name = rename_map.get(file, file.replace(".gz", ""))
        output_path = os.path.join(output_folder, output_name)

        with gzip.open(input_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Extracted and renamed: {file} -> {output_name}")

# --- MERGE ALL CSV FILES INTO ONE DATAFRAME ---
all_dfs = []
for file in os.listdir(output_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(output_folder, file)
        df = pd.read_csv(file_path)
        # Optionally add a column to keep track of original dataset
        df["source_file"] = file
        all_dfs.append(df)

merged_df = pd.concat(all_dfs, ignore_index=True)

# --- SAVE MERGED DATASET ---
merged_path = os.path.join(output_folder, merged_file_name)
merged_df.to_csv(merged_path, index=False)
print(f"\nMerged dataset saved as: {merged_path}")
