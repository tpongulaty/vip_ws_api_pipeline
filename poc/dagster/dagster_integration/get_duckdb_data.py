import duckdb
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv


# this will read .env in your project root
load_dotenv()

def export_duckdb_to_csv(folder,duckdb_file,table_name,filename, output_dir="Monthly"):
    # Connect to DuckDB
    db_path = os.getenv("DB_PATH")
    print(db_path)
    now = datetime.now()
    current_year = now.year
    current_month = now.strftime('%B')
    file_name = f"{filename}_{current_month}_{current_year}.csv"
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(db_path,folder,output_dir, file_name) # get the full path of the file to be stored.

    db_file = rf"{db_path}\{folder}\{duckdb_file}.duckdb" # get the full path of the db_file
    con = duckdb.connect(db_file)
    df = con.execute(f"SELECT * FROM {table_name}").fetch_df()
    df.to_csv(file_path, index=False)
    con.close()

    return file_path, file_name
if __name__ == "__main__":
    filename = "wv_bulk_pipeline_test"
    table_name = os.getenv("WV_TABLE_NAME")
    folder = os.getenv("WV_FOLDER")
    duckdb_file = os.getenv("WV_DUCKDB_FILE")
    export_duckdb_to_csv(folder,duckdb_file,table_name,filename, output_dir="Monthly")