import os
import pandas as pd

def combine_all_company_data(data_dir='data', output_dir='data/combined_data', output_filename='All_companies_combined_data.csv'):
    """
    Combines all *_combined_analysis.csv files from each subdirectory in `data_dir`
    and saves the concatenated DataFrame to `output_dir/output_filename`.
    """
    combined_files = []
    for subdir in os.listdir(data_dir):
        subdir_path = os.path.join(data_dir, subdir)
        if os.path.isdir(subdir_path):
            for fname in os.listdir(subdir_path):
                if fname.endswith('_combined_analysis.csv'):
                    combined_files.append(os.path.join(subdir_path, fname))

    if not combined_files:
        print("No combined analysis files found.")
        return

    df_list = []
    for file in combined_files:
        try:
            df = pd.read_csv(file)
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if not df_list:
        print("No valid data found for combining.")
        return

    combined_df = pd.concat(df_list, ignore_index=True)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_filename)
    combined_df.to_csv(output_path, index=False)
    print(f"✅ Combined data saved to: {output_path}")

if __name__ == "__main__":
    combine_all_company_data()