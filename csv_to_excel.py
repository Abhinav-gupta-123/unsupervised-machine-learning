import pandas as pd
import argparse
import os
import sys
import math

def convert_csv_to_excel(input_csv, output_prefix=None, rows_per_file=500000):
    """
    Converts a CSV file to multiple Excel (.xlsx) files to handle large datasets
    and memory constraints.
    """
    if not os.path.exists(input_csv):
        print(f"Error: The file '{input_csv}' does not exist.")
        sys.exit(1)

    if output_prefix is None:
        # Use filename without extension as prefix
        output_prefix = os.path.splitext(input_csv)[0] 

    # Excel hard limit is 1,048,576. We use a smaller number to be safe with RAM.
    # 500,000 is a good balance for 8GB RAM systems.
    
    print(f"Processing '{input_csv}'...")
    print(f"Splitting into files of {rows_per_file} rows each to save RAM and fit in Excel...")

    try:
        # Calculate total rows approximately (optional, but good for progress)
        # For very large files, counting lines might take a moment but helps context.
        # We skip exact counting to save time and jump straight to chunk processing.
        
        chunk_size = 50000 # Read from CSV in small memory-efficient blocks
        
        # We will accumulate chunks until we hit `rows_per_file` then write to Excel
        current_data = []
        current_rows = 0
        file_counter = 1
        
        # Create an iterator to read the file in chunks so we don't load it all at once
        csv_iterator = pd.read_csv(input_csv, chunksize=chunk_size, low_memory=False)
        
        for chunk in csv_iterator:
            current_data.append(chunk)
            current_rows += len(chunk)
            
            # If we have enough data for one Excel file
            if current_rows >= rows_per_file:
                # Combine accumulated chunks
                df_part = pd.concat(current_data)
                
                # If we overshot the limit significantly, we might want to split exactly, 
                # but for simplicity in this "low memory" script, we just write what we have 
                # (which might be slightly over rows_per_file but under 1M limit if rows_per_file is 500k).
                
                # Check if we are accidentally over Excel's hard limit
                if len(df_part) > 1048576:
                    # Look, if we somehow got > 1M rows in one pass because chunks were huge (unlikely with 50k chunks)
                    # We would need to handle it. With 50k chunks and 500k target, we will be fine.
                    pass 

                output_filename = f"{output_prefix}_part{file_counter}.xlsx"
                print(f"Writing {len(df_part)} rows to '{output_filename}'...")
                df_part.to_excel(output_filename, index=False)
                print(f"Saved {output_filename}")
                
                # Clear memory
                del df_part
                current_data = []
                current_rows = 0
                file_counter += 1
                
        # Write any remaining data
        if current_data:
            df_part = pd.concat(current_data)
            output_filename = f"{output_prefix}_part{file_counter}.xlsx"
            print(f"Writing remaining {len(df_part)} rows to '{output_filename}'...")
            df_part.to_excel(output_filename, index=False)
            print(f"Saved {output_filename}")
            
        print("All conversions successful!")
        
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a Large CSV file to multiple Excel files.")
    parser.add_argument("input_csv", help="Path to the input CSV file")
    parser.add_argument("-r", "--rows", type=int, default=500000, help="Rows per Excel file (default 500,000)")
    
    args = parser.parse_args()
    
    # Validation
    if args.rows > 1000000:
        print("Warning: Excel cannot hold more than 1,048,576 rows per sheet.")
        print("Resetting rows limit to 1,000,000.")
        args.rows = 1000000
        
    convert_csv_to_excel(args.input_csv, None, args.rows)
