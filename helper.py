import duckdb
import h3
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def compute_neighbours_col(row, df, max_distance, col = "cost_of_living"):
    """
    Compute the average value for the especified col for neighboring H3 hexagons within a specified distance.

    Parameters:
    ----------
    row : pandas.Series
        A row from a DataFrame containing the central hexagon's information. Must include the "hex_id" column.
    df : pandas.DataFrame
        A DataFrame containing information about other hexagons. Must include "hex_id" and "cost_of_living" columns.
    max_distance : int
        The maximum grid distance (in H3 terms) to consider a hexagon as a neighbor.
    col : str
        The column used to compute the average

    Returns:
    -------
    float or np.nan
        The average cost of living for hexagons within the specified distance. Returns np.nan if no neighbors are within range.

    Notes:
    -----
    - The function uses the H3 library's `h3_distance` to calculate the grid distance between hexagons.
    - Hexagons at a distance of 0 (the same hexagon as the center) are ignored.

    Example:
    --------
    Suppose you have the following DataFrame `df`:

    | hex_id           | cost_of_living |
    |------------------|----------------|
    | 8928308280fffff  | 100            |
    | 8928308283bffff  | 120            |
    | 89283082807ffff  | 110            |

    And a `row` with `hex_id = "8928308280fffff"`:
    
    ```python
    result = compute_neighbours_col(row, df, max_distance=2)
    ```

    This will compute the average `cost_of_living` for neighboring hexagons within a distance of 2.
    """
    
    costs = []
    center = row["hex_id"]
    
    for instance in df.iterrows():
        comp_to = instance[1]
        candid = comp_to["hex_id"]
        distance = h3.h3_distance(center, candid)
        if distance > 0 and distance <= max_distance:
            costs.append(comp_to[col])
    if len(costs) == 0:
        return np.nan
    else:
        return np.mean(costs)
    
def compute_neighbours_costs_for_row(row, df):
    """
    Compute the average cost of living for neighboring hexagons within 
    distances of 1, 2, 3, and 4 H3 cells from the current hexagon.

    Parameters:
    ----------
    row : pandas.Series
        A row from the DataFrame containing the hexagon's details. 
        Must include the "hex_id" column representing the current H3 hexagon.

    Returns:
    -------
    list
        A list containing four values:
        - The average cost of living for neighbors within distance 1.
        - The average cost of living for neighbors within distance 2.
        - The average cost of living for neighbors within distance 3.
        - The average cost of living for neighbors within distance 4.
        If no neighbors are found within a given distance, the corresponding value will be `np.nan`.

    Notes:
    -----
    - This function uses the `compute_neighbours_col` function to calculate 
      the average cost of living for each distance.
    - The function is designed to be applied to each row of a DataFrame using `pandas.DataFrame.apply`.
    """
    
    return [
        compute_neighbours_col(row, df, 1),
        compute_neighbours_col(row, df, 2),
        compute_neighbours_col(row, df, 3),
        compute_neighbours_col(row, df, 4)]

def compute_h3_index(lat, lon, resolution=8):
    """
    Compute the H3 index for a given geographic coordinate.

    Parameters:
        lat (float): Latitude of the location.
        lon (float): Longitude of the location.
        resolution (int, optional): The H3 resolution level (default is 8).

    Returns:
        str: The H3 index as a string if successful, or "unknown" if an exception occurs.
    """
    try:
        return h3.geo_to_h3(lat, lon, resolution)
    except Exception:
        return "unknown"

def process_file_with_h3(input_file, output_file, chunk_size=1_000_000):
    """
    Processes a large input file by adding an H3 index column and writing the result to an output file in chunks.

    Parameters:
        input_file (str): Path to the input Parquet file containing latitude and longitude columns ('lat', 'lon').
        output_file (str): Path to the output Parquet file to save processed data.
        chunk_size (int, optional): Number of rows to process per chunk (default is 1,000,000).

    Steps:
        1. Connects to the input file using DuckDB.
        2. Reads data in chunks, adds an H3 index column using `compute_h3_index`, and writes the result to the output file.
        3. Ensures proper resource cleanup (DuckDB connection and ParquetWriter).

    Notes:
        - The input file must be in Parquet format with columns 'lat' and 'lon'.
        - The H3 index is computed at the default resolution defined by `compute_h3_index`.

    Returns:
        None
    """

    # Initialize DuckDB connection
    conn = duckdb.connect()

    # Get total number of rows
    total_rows = conn.execute(f"SELECT COUNT(*) FROM '{input_file}'").fetchone()[0]
    print(f"Total rows: {total_rows}")

    # Initialize ParquetWriter before processing
    writer = None

    try:
        for offset in range(0, total_rows, chunk_size):
            # Read a chunk from the input file
            query = f"SELECT * FROM '{input_file}' LIMIT {chunk_size} OFFSET {offset}"
            df_chunk = conn.execute(query).fetchdf()

            # Add the H3 index column
            df_chunk['h3_index'] = df_chunk.apply(
                lambda row: compute_h3_index(row['lat'], row['lon']), axis=1
            )

            # Convert to PyArrow Table
            table = pa.Table.from_pandas(df_chunk)

            # Initialize writer with the schema on the first chunk
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema, use_dictionary=True)

            # Write the chunk
            writer.write_table(table)
            print(f"Processed and written chunk with offset {offset}")

    finally:
        # Ensure the writer is closed properly
        if writer:
            writer.close()
        conn.close()
        print("Processing complete.")
