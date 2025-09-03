import logging
import duckdb
import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def scrape_raw_ny() -> pd.DataFrame:
    """ Scrapes the NY City DOT data and returns the raw data as a DataFrame."""
    # Set download directory
    download_dir = os.getenv("DOWNLOAD_DIR_NY")  # Replace with desired path. Ideally where downloads from browser are isolated to avoid conflicts

    # Configure Chrome options
    opts = Options()
    opts.add_experimental_option("prefs", {
        "download.default_directory": rf"{download_dir}",  # Set default download directory
        "download.prompt_for_download": False,  # Disable download prompts
        "directory_upgrade": True,  # Automatically overwrite files
        "safebrowsing.enabled": True  # Enable safe browsing
    })
    # Automatic driver installer
    service = Service(ChromeDriverManager().install())
    # URL of the site
    url = 'https://wwe2.osc.state.ny.us/transparency/contracts/contractsearch.cfm'
           
    opts.add_argument("--headless")

    def get_bulk_file():
        try:
            # Start a new browser session
            driver = webdriver.Chrome(service=service, options=opts)
            driver.get(url)
            driver.set_window_size(1920, 1080)  # adjust window size to avoid elements from overlapping

            def is_download_complete(file_path):
                """
                Check if the file is complete by:
                1. Ensuring it's not a .crdownload file (Chrome's in-progress download extension).
                2. Ensuring it has a non-zero size.
                """
                return os.path.exists(file_path) and not file_path.endswith(".crdownload") and os.path.getsize(file_path) > 0

            def wait_for_download(download_dir, timeout=30):
                """
                Wait for the most recently downloaded file to appear in the download directory.
                Ensures the file is fully downloaded before returning its path.
                :param download_dir: The directory to monitor for downloads.
                :param timeout: Maximum time to wait for a file (in seconds).
                :return: The full path of the downloaded file.
                """
                start_time = time.time()
                while True:
                    # List all files in the download directory
                    files = os.listdir(download_dir)

                    if files:
                        # Identify the most recently created file in the directory
                        latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(download_dir, f)))
                        latest_file_path = os.path.join(download_dir, latest_file)

                        # Check if the download is complete
                        if is_download_complete(latest_file_path):
                            return latest_file_path

                    # Break the loop if timeout is exceeded
                    if time.time() - start_time > timeout:
                        raise TimeoutError("Download did not complete within the timeout period.")
                    
                    time.sleep(1)  # Wait for a short time before checking again

            driver.get(url)
            driver.set_window_size(1920, 1080)  # adjust window size to avoid elements from overlapping
            # wait for agency option to appear
            agency_name = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[3]/div[2]/div/form/div[1]/p[1]/span/span[1]/span/ul/li/input"))
            )
            agency_name.click()
            # select "all state agencies"
            all_state_agencies = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[3]/div[2]/div/form/div[1]/p[1]/select/option[1]"))
            )
            all_state_agencies.click()
            search = driver.find_element(By.ID, 'b')
            # Find the contract number input box and enter the contract number
            search.click()
            # find and download the final report
            download_report = WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.LINK_TEXT, 'Download Summary Contract Information to an Excel Spreadsheet'))
            )
            download_report.click()
            print("Waiting for contract data download to complete...")
            # wait until download is completed
            time.sleep(70)
            try:
                downloaded_file = wait_for_download(download_dir, timeout=60)
                print(f"File successfully downloaded at: {downloaded_file}")
                ny_bulk = pd.read_excel(downloaded_file,skiprows=7)
            except TimeoutError as e:
                print(e)
            download_report_amendment = WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.LINK_TEXT, 'Download Additional Contract and Related Amendment Data for OSC approved transactions'))
            )
            download_report_amendment.click()
            print("Waiting for amendment contract data download to complete...")
            # wait until download is completed
            time.sleep(120)
            try:
                downloaded_file = wait_for_download(download_dir, timeout=60)
                print(f"File successfully downloaded at: {downloaded_file}")
                ny_amendment = pd.read_csv(downloaded_file,skiprows=1)
            except TimeoutError as e:
                print(e)
            
        finally:
            # Close the browser
            driver.quit()
        return ny_bulk, ny_amendment
    # Call the function to get the bulk file
    ny_bulk, ny_amendment = get_bulk_file()
    return ny_bulk, ny_amendment
    

def transform_and_load_ny(ny_bulk: pd.DataFrame, ny_amendment: pd.DataFrame) -> pd.DataFrame:    
    """ Transforms the scraped NY data and loads it into DuckDB."""

    # Read the lookup table for department/facility names and their corresponding agency names
    df_lookup = pd.read_excel(os.getenv("NY_LOOKUP_TABLE"))


    # POST PROCESSING
    # Split into contract type and contract sub type
    ny_bulk[['Contract_Type', 'Contract_Subtype']] = ny_bulk['CONTRACT TYPE'].str.split(' - ', n=1, expand = True)
    del ny_bulk['CONTRACT TYPE'] # use this option if it needs to be deleted
    ny_dot_data = pd.merge(ny_bulk,df_lookup, left_on="DEPARTMENT/FACILITY", right_on="DEPARTMENT/FACILITY", how="left")
    df = ny_dot_data.copy()
    

    df = ny_dot_data.copy()
    df.rename(columns = {'VENDOR NAME':'Contractor_Name','DEPARTMENT/FACILITY': 'Project_Dept_Facility','CONTRACT NUMBER':'Contract_Number','CURRENT CONTRACT AMOUNT':'Project_Cost_Total',
                        'SPENDING TO DATE': 'Payment_Amount_Total',	'CONTRACT START DATE':'Project_Start_Actual','CONTRACT END DATE':'Project_Comp_Est','CONTRACT DESCRIPTION':'Project_Description',
                        'ORIGINAL CONTRACT APPROVED/FILED DATE':'Project_Bid_Awarded'}, inplace=True)
    def parse_money(value):
        # Check if the value is already a float or None/NaN
        if pd.isna(value) or isinstance(value, (float, int)):
            return value
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value)
    df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
    df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
    df['Payment_Balance'] = df['Project_Cost_Total'] - df['Payment_Amount_Total']

    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_path = os.getenv("DB_PATH")
    db_file =  rf"{db_path}\NY_City\data_store_NY.duckdb"
    table_name = "NY_DOT"

    # Current scraped data
    scraped_data = df

    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Contractor_Name  TEXT,
    Project_Dept_Facility TEXT,
    Agency_Name  TEXT,
    Contract_Number  TEXT,
    Contract_Type  TEXT,
    Contract_Subtype TEXT,
    Payment_Number  INTEGER,
    Project_Description	TEXT,
    Project_Start_Actual  TEXT,
    Project_Comp_Est  TEXT,	
    Project_Bid_Awarded  TEXT,
    Payment_Amount DOUBLE,	
    Previous_Payment_Amount_Total DOUBLE,
    Project_Cost_Total  DOUBLE,	
    Payment_Amount_Total  DOUBLE,
    Payment_Total_Percent  TEXT,
    Payment_Balance  DOUBLE,
    Pull_Date_Initial TEXT,
    Payment_Amount_Percent FLOAT,
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM {table_name}").df()

    # Deduplicate and merge
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
        # find duplicates by all columns except the columns below since they are calculated later. Later, a logic can be developed to find revised payments if any
        combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial','CONTRACT TYPE', 'Contract_Subtype','Payment_Total_Percent','Previous_Payment_Amount_Total', 'Payment_Number', 'Payment_Amount_Percent', 'Payment_Amount'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Pull_Date_Initial"], ascending=[True,False])
        # Group by Contract_Number, Contractor_Name and Project_Description (Since Ny has different jobs within the same contract number) and calculate the previous Payment_Amount_Total
        combined_data['Previous_Payment_Amount_Total'] = combined_data.groupby(["Contract_Number","Contractor_Name","Project_Description"])["Payment_Amount_Total"].shift(-1)
        # Assign a reversed payment estimate number
        combined_data["Payment_Number"] = combined_data.groupby(["Contract_Number","Contractor_Name","Project_Description"]).cumcount(ascending=False)
        # Calculate the Current Amount Paid
        combined_data['Payment_Amount'] = combined_data['Payment_Amount_Total'] - combined_data['Previous_Payment_Amount_Total']
        # Fill NaN for the first record in each group (no previous record exists)
        combined_data['Payment_Amount'] = combined_data['Payment_Amount'].fillna(combined_data['Payment_Amount_Total'])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent and Payment Total Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]

    else:
        print("I am here")
        combined_data = scraped_data
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Pull_Date_Initial"], ascending=[True,False])
        # Group by Contract_Number, Contractor_Name and Project_Description (Since Ny has different jobs within the same contract number) and calculate the previous Payment_Amount_Total
        combined_data['Previous_Payment_Amount_Total'] = combined_data.groupby(["Contract_Number","Contractor_Name","Project_Description"])["Payment_Amount_Total"].shift(-1)
        # Assign a reversed payment estimate number
        combined_data["Payment_Number"] = combined_data.groupby(["Contract_Number","Contractor_Name","Project_Description"]).cumcount(ascending=False)
        # Calculate the Current Amount Paid
        combined_data['Payment_Amount'] = combined_data['Payment_Amount_Total'] - combined_data['Previous_Payment_Amount_Total']
        # Fill NaN for the first record in each group (no previous record exists)
        combined_data['Payment_Amount'] = combined_data['Payment_Amount'].fillna(combined_data['Payment_Amount_Total'])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent and Payment Total Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conersion errors
        combined_data = combined_data[correct_order]

    # Replace the table with the updated data
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

    # Close connection
    con.close()
    print("NY City scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'NY City scraping completed and DUCKDB file updated Successfully.')
    ny_amendment.rename(columns = {'TRANSACTION TYPE': 'Transaction_Type','VENDOR NAME':'Contractor_Name','DEPARTMENT/FACILITY': 'Project_Dept_Facility','CONTRACT NUMBER':'Contract_Number','TRANSACTION AMOUNT':'Transaction_Amount',
                        'START DATE':'Start_Date','END DATE':'Project_Comp_Substantial','DESCRIPTION':'Description',
                        'TRANSACTION APPROVED/FILED DATE':'Transaction_Approved/Filed_Date'}, inplace=True)
    return combined_data, ny_amendment

def data_appended_ny(combined_data, ny_amendment=None) -> pd.DataFrame: # Fetch the data appended in the current run
    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    appended_data = combined_data[combined_data["Pull_Date_Initial"] == current_date]
    if appended_data.empty:
        print('Data not yet updated on Website.')
        logging.info(
            'Data not yet updated on Website.'
        )
    else:
        print('Successfully appended latest data.')
        logging.info('Successfully appended latest data.')
    return appended_data
    

