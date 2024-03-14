import pandas as pd


def split_csv(input_file, output_prefix, num_files):
    df = pd.read_csv(input_file)
    chunksize = int(len(df) / num_files)
    for i in range(num_files):
        start_index = i * chunksize
        end_index = (i + 1) * chunksize if i < num_files - 1 else len(df)
        output_file = f"{output_prefix}_{i + 1}.csv"
        df.iloc[start_index:end_index].to_csv(output_file, index=False)


input_file = "Airline_Dataset.csv"
output_prefix = "chunk"
num_files = 4

split_csv(input_file, output_prefix, num_files)

print(f"Successfully created {num_files} CSV files!")