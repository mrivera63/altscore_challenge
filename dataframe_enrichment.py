from helper import process_file_with_h3

if __name__ == "__main__":
    input_file = 'data/mobility_data.parquet'
    output_file = 'data/mobility_data_enriched.parquet'

    process_file_with_h3(input_file, output_file)
