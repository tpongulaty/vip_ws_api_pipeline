import os
import time
import pandas as pd
from typing import List, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import InvalidArgumentException
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from datetime import datetime
import pytz
import logging
import duckdb
from dotenv import load_dotenv

load_dotenv()

service = Service(ChromeDriverManager().install())
THIS_DIR      = os.path.dirname(os.path.abspath(__file__))
CHUNK_SIZE    = int(os.getenv("LA_CHUNK_SIZE", 500))                 # >>> ADDED
PROGRESS_FILE = os.path.join(THIS_DIR, "la_progress.txt")             # >>> ADDED

def _load_progress() -> int:                                           # >>> ADDED
    try:
        return int(open(PROGRESS_FILE).read().strip())
    except Exception:
        return 0

def _save_progress(idx: int) -> None:                                 # >>> ADDED
    with open(PROGRESS_FILE, "w") as fh:
        fh.write(str(idx))
    logging.info("[LA] progress pointer saved → %s", idx)

HEADER_LA: List[str] = []                                              # global header   

def _scrape_la_chunk(contract_numbers: List[str]) -> List[List[str]]:
    """Scrape a chunk of LA work orders and return the data as a list of lists."""
    global HEADER_LA
    # URL of the site
    url = 'https://bca.lacity.org/Payments/payments_search.php'
    row_data_list_la: List[List[str]] = []
    # Start a new browser session
    driver = webdriver.Chrome(service=service)
    driver.get(url)

    try:
        for i,value in enumerate(contract_numbers):
        
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.NAME, "wo_nbr"))
            )

            # Find the contract number input box and enter the contract number
            contract_number_input = driver.find_element(By.NAME, "wo_nbr")
            contract_number_input.clear()
            contract_number_input.send_keys(value)
            contract_number_input.send_keys(Keys.RETURN)
            # Submit the search form
            try: 
                project_key = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/ul/li/form/input[2]'))
                )
                project_key.click()

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, value))
                )
                description = driver.find_element(By.XPATH, '/html/body/h1[@class="page-title"]').get_attribute("textContent").strip()

                table = driver.find_element(By.ID, value).find_element(By.TAG_NAME, 'thead').find_element(By.TAG_NAME, 'tr')
                # Extract the table headers
                if not HEADER_LA:  # Only extract headers once
                    headers = table.find_elements(By.TAG_NAME, 'th')
                    HEADER_LA.extend([header.get_attribute("textContent").strip() for header in headers if header.get_attribute("textContent").strip()])
                    HEADER_LA = ['Work Order','Job Title'] + HEADER_LA # Add work order and job title to the header list

                # Set the number of records to display per page
                # Displays maximum number of records in a page for efficiency
                set_records = WebDriverWait(driver, 10).until(
                                EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div[1]/div/div/select/option[5]'))
                                )
                set_records.click() 
                time.sleep(1)  
                # Iterate over all pages of transactions
                next_page_button = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '//div[@class="dt-paging"]/nav/button[@aria-label="Next"]'))
                )     
                while "disabled" not in next_page_button.get_attribute("class"):
                    # Extract all records of transactions for a work order
                    WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, value))
                    )
                    rows = driver.find_element(By.ID, value).find_element(By.TAG_NAME, 'tbody').find_elements(By.TAG_NAME, 'tr')
                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, 'td')  # Find data cells
                        row_data = [value] + [description] + [col.get_attribute("textContent") for col in cols]
                        if any(row_data):
                            row_data_list_la.append(row_data)
                    next_page_button.click()
                    time.sleep(2)           
                    next_page_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="dt-paging"]/nav/button[@aria-label="Next"]'))
                    )  
                    
                # Last page needs to be extracted as well
                if "disabled" in next_page_button.get_attribute("class"):
                    WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, value))
                    )
                    rows = driver.find_element(By.ID, value).find_element(By.TAG_NAME, 'tbody').find_elements(By.TAG_NAME, 'tr')
                    for row in rows:
                        cols = row.find_elements(By.TAG_NAME, 'td')  # Find data cells
                        row_data = [value] + [description] + [col.get_attribute("textContent") for col in cols]
                        if any(row_data):
                            row_data_list_la.append(row_data)
            
            except TimeoutException:
                print(value,"Data Not Found")
                
            driver.get(url)
    except TimeoutException:
            print(value,"Not Found")      

    finally:
        # Close the browser
        driver.quit()
    return row_data_list_la, HEADER_LA

def scrape_raw_la() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Chunk aware LA scraper. Returns (main_df, subcontractor_df)."""
    # Get the list of contract numbers with add info. Will be used as input in next step

    # URL of the site
    url_sub = 'https://bca.lacity.gov/approvedsubs/'          

    # Start a new browser session
    driver = webdriver.Chrome(service=service)
    driver.get(url_sub)
    row_data_list_la_sub = []
    header_data_la_sub = ["work_order","project_title","subcontractor_name","work_description","work_value"]

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "company_wrapper"))
        )
        # Submit the search form
         
        set_records = driver.find_element(By.XPATH, '//*[@id="company_length"]/label/select/option[4]')
        set_records.click()
        # Wait for the table to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="company"]'))
        )
            
        # Iterate and extract work order no's & add info over all the pages
        next_page_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="company_next"]'))
            ) 
            
        while "disabled" not in next_page_button.get_attribute("class"):
            # Ensure the table is loaded before extracting data and to avoid stale element reference
            WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="company"]'))
            )
            WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "company_wrapper"))
            )
            WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="company"]/tbody/tr'))
            )
            rows = driver.find_elements(By.XPATH, '//*[@id="company"]/tbody/tr')
            time.sleep(2)
            for i,row in enumerate(rows):
                row_data = row.find_elements(By.XPATH, './td')
                row_data_list_la_sub.append([cell.get_attribute("textContent") for cell in row_data[:-1]])   
            next_page_button_link = driver.find_element(By.XPATH,'//*[@id="company_next"]/a') 
            next_page_button_link.click()
            time.sleep(2)
            WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="company"]'))
            )             
            next_page_button = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="company_next"]'))
            )  
            if "disabled" in next_page_button.get_attribute("class"):
                rows =  WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '//*[@id="company"]/tbody/tr'))
                )
                for i,row in enumerate(rows):
                    row_data = driver.find_elements(By.XPATH, './td')
                    row_data_list_la_sub.extend([cell.get_attribute("textContent") for cell in row_data[:1]])

    finally:
        # Close the browser
        driver.quit()
        
    LA_city_data_sub = pd.DataFrame(data=row_data_list_la_sub, columns = header_data_la_sub)
    def parse_money(value):
        if not value:  # Check for None, empty string, or NaN
            return None
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value) if value.strip() else None
    # Convert 'work_value' to float for calculating 'Project_Cost_Total'
    LA_city_data_sub['work_value'] = LA_city_data_sub['work_value'].apply(parse_money)
    # Calculate 'Project_Cost_Total' by summing 'work_value'
    LA_city_data_sub['Project_Cost_Total'] = LA_city_data_sub[LA_city_data_sub['work_value'] >= 0].groupby('work_order')['work_value'].transform('sum')
    contract_numbers_list_la = LA_city_data_sub["work_order"].unique().tolist()
    print("Total Contracts:",len(contract_numbers_list_la))
    contract_numbers = contract_numbers_list_la

    # Get the associated data after extracting list of contract numbers in the previous step
    all_rows: List[List[str]] = []
    start_idx = _load_progress()
    remaining = contract_numbers[start_idx:]
    if not remaining:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        return pd.DataFrame(columns=HEADER_LA), LA_city_data_sub  
    total = len(remaining)
    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total)
        subset = remaining[chunk_start:chunk_end]
        abs_start = start_idx + chunk_start
        logging.info(f"[LA] chunk {abs_start},{abs_start + len(subset)-1}")
        try:
            rows, _ = _scrape_la_chunk(subset)          # >>> ADDED helper above
            chunk_df = pd.DataFrame(data=rows, columns=HEADER_LA) # Ensures there is no mismatch in columns
            all_rows.extend(rows)
            _save_progress(start_idx + chunk_end)                 # >>> ADDED
        except Exception as exc:
            logging.exception("[LA] chunk failed: will retry next run", exc_info=exc)
            if all_rows and HEADER_LA:  # If we have some rows, load them to DB andsave progress
                tmp_df = pd.DataFrame(all_rows, columns=HEADER_LA)
                transform_and_load_la(tmp_df, LA_city_data_sub)  # Load the data to DuckDB
                print("Executed transform_and_load_la() for the last successful chunk.")
            _save_progress(start_idx + chunk_start)
            driver.quit()
            raise

    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)

    LA_city_data = pd.DataFrame(data=all_rows, columns = HEADER_LA) 

    return LA_city_data, LA_city_data_sub

def transform_and_load_la(LA_city_data: pd.DataFrame, LA_city_data_sub: pd.DataFrame) -> pd.DataFrame:

    # Prepare the main table using informaton from sub table
    # Have only one instance of each work_order from the sub table to prepare for joining
    LA_city_data_sub_unique = LA_city_data_sub.drop_duplicates(subset=LA_city_data_sub.loc[:, LA_city_data_sub.columns.isin(['work_order'])].columns,keep="first")
    # Join the sub unique table with the main table in order to get 'project_title', 'Project_Cost_Total'
    LA_city_data = pd.merge(LA_city_data, LA_city_data_sub_unique, left_on='Work Order', right_on='work_order',how='left')
    # Drop unnecessary columns
    LA_city_data.drop(columns=['subcontractor_name','work_description','work_value','work_order'], inplace=True)


    # Prepare the main table using informaton from sub table
    def parse_money(value):
        if not value:  # Check for None, empty string, or NaN
            return None
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value) if value.strip() else None

    # Post Processing
    df = LA_city_data.copy()
    df_sub = LA_city_data_sub.copy()
    df.rename(columns = {'Work Order':'Project_Number','project_title':'Project_Description_Full','Job Title':'Project_Description','Approved by Inspector':'Inspector_Approval',
                        'Approved by Supervisor':'Supervisor_Approval','Payment Number':'Payment_Number','Amount':'Payment_Amount','Received by Payments':'Payment_Received',
                        'Sent to Accounting':'Payment_toAccounting','Escrow Released':'Escrow_Released','Escrow Sent to Agent':'Escrow_Sent_to_Agent',
                        'Payment Status':'Payment_Status'},inplace=True)
    df_sub.rename(columns = {'work_order':'Project_Number','project_title':'Project_Description_Full','work_description':'Project_Justification_Sub'}, inplace=True)

    # Convert data types of columns necessary for further calculations
    df = df[df['Payment_Number'].str.isnumeric()] # Check if the string is numeric before converting
    df['Payment_Number'] = df['Payment_Number'].astype(int)                                                    
    df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)
    df['Escrow_Released'] = df['Escrow_Released'].apply(parse_money)
    df['Escrow_Sent_to_Agent'] = df['Escrow_Sent_to_Agent'].apply(parse_money)

    #Convert the format of date columns to 'MM/DD/YYYY' format
    df['Supervisor_Approval'] = pd.to_datetime(df['Supervisor_Approval'],errors='coerce') 
    df['Supervisor_Approval'] = df['Supervisor_Approval'].dt.strftime('%m/%d/%Y')
    df['Inspector_Approval'] = pd.to_datetime(df['Inspector_Approval'],errors='coerce')
    df['Inspector_Approval'] = df['Inspector_Approval'].dt.strftime('%m/%d/%Y')
    df['Payment_Received'] = pd.to_datetime(df['Payment_Received'],errors='coerce')
    df['Payment_Received'] = df['Payment_Received'].dt.strftime('%m/%d/%Y')
    df['Payment_toAccounting'] = pd.to_datetime(df['Payment_toAccounting'],errors='coerce')
    df['Payment_toAccounting'] = df['Payment_toAccounting'].dt.strftime('%m/%d/%Y')
    df['Payment_Status'] = df['Payment_Status'].str.strip()

    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_path = os.getenv("DB_PATH")
    db_file = rf"{db_path}\LA_City\data_store_LA.duckdb"
    table_name = "LA_DOT"
    table_name_sub = "LA_DOT_sub" 

    # Current scraped data
    scraped_data = df.copy()

    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Project_Number  TEXT,
    Payment_Number  INTEGER,
    Project_Description_Full  TEXT,
    Project_Description	TEXT,
    Comments  TEXT,
    Inspector_Approval  TEXT,
    Supervisor_Approval  TEXT,

    Payment_Received  TEXT,
    Payment_toAccounting  TEXT,
    Payment_Status  TEXT,

    Payment_Amount DOUBLE,
    Escrow_Sent_to_Agent DOUBLE,	
    Escrow_Released  DOUBLE,
    Project_Cost_Total  DOUBLE,	
    Payment_Amount_Total  DOUBLE,
    Payment_Balance  DOUBLE,
    Payment_Total_Percent  FLOAT,
    Payment_Amount_Percent FLOAT,
    Pull_Date_Initial TEXT
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM {table_name}").df()

    # Deduplicate and merge
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
        #Remove the next two lines after updating te DuckDB with the new columns
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        # Sort the DataFrame. Note: Sorting "Pull_Date_Initial" isn't required. Only if a payment number is duplicated for some reason.
        combined_data = combined_data.sort_values(by=["Project_Number", "Payment_Number","Pull_Date_Initial"], ascending=[True,True,False],na_position='first') 
        # find duplicates by all columns except the columns below since they are calculated later. Later, a logic can be developed to find revised payments if any
        combined_data = combined_data.drop_duplicates(subset=combined_data.loc[:, ~combined_data.columns.isin(['Pull_Date_Initial', 'Payment_Amount_Percent','Payment_Amount_Total',
                                                                                                            'Payment_Total_Percent','Comments','Inspector_Approval','Supervisor_Approval',
                                                                                                            'Payment_Received','Payment_toAccounting','Payment_Balance','Escrow_Sent_to_Agent',
                                                                                                            'Payment_Status','Project_Description'])].columns,keep="first") 
        # Note:'Project_Description' excluded from finding unique columns since it seems to be same as 'Project_Description_Full' after the website has updated recently. Can be removed later if confirmed.
        
        # Post processing before loading into duckdb
        # combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        # Sort the DataFrame. Note: Sorting "Pull_Date_Initial" isn't required. Only if a payment number is duplicated for some reason.
        combined_data = combined_data.sort_values(by=["Project_Number", "Payment_Number","Pull_Date_Initial"], ascending=[True,True,False],na_position='first')
    
        # Calculate the Current Payment Amount Total
        combined_data['Payment_Amount_Total'] = combined_data[combined_data['Payment_Amount'] >= 0].groupby("Project_Number")['Payment_Amount'].cumsum()
        # Revert the formatting of the date column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment_Balance
        combined_data['Payment_Balance'] = combined_data['Project_Cost_Total'] - combined_data['Payment_Amount_Total']
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Calculate Payment Total Percent
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]   

    # If loading data for the first time
    else:
        combined_data = scraped_data
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        # Sort the DataFrame. Note: Sorting "Pull_Date_Initial" isn't required. Only if a payment number is duplicated for some reason.
        combined_data = combined_data.sort_values(by=["Project_Number", "Payment_Number","Pull_Date_Initial"], ascending=[True,True,False],na_position='first')
    
        # Calculate the Current Payment Amount Total
        combined_data['Payment_Amount_Total'] = combined_data[combined_data['Payment_Amount'] >= 0].groupby("Project_Number")['Payment_Amount'].cumsum()
        # Revert the formatting of the date column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment_Balance
        combined_data['Payment_Balance'] = combined_data['Project_Cost_Total'] - combined_data['Payment_Amount_Total']
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Calculate Payment Total Percent
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]  

    # Current scraped_sub data
    df_sub["Pull_Date_Initial"] = current_date
    scraped_data_sub = df_sub


    # Create sub_table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name_sub} (
    Project_Number  TEXT,
    Project_Description_Full  TEXT,
    Project_Justification_Sub  TEXT,
    subcontractor_name  TEXT,

    work_value  DOUBLE,
    Project_Cost_Total  DOUBLE,	
    Pull_Date_Initial TEXT
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data_sub = con.execute(f"SELECT * FROM {table_name_sub}").df()

    # Deduplicate and merge
    if not existing_data_sub.empty:
        combined_data_sub = pd.concat([existing_data_sub, scraped_data_sub], ignore_index=True)
        # find duplicates by all columns except Pull Date Initial
        combined_data_sub = combined_data_sub.drop_duplicates(subset=df_sub.loc[:, ~df_sub.columns.isin(['Pull_Date_Initial'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data_sub['Pull_Date_Initial'] = pd.to_datetime(combined_data_sub['Pull_Date_Initial'])
        # Sort the DataFrame. Note: Sorting "Pull_Date_Initial" isn't required. Only if a payment number is duplicated for some reason.
        combined_data_sub = combined_data_sub.sort_values(by=["Project_Number","Pull_Date_Initial"], ascending=[True,False])
        # Revert the formatting of the date column
        combined_data_sub["Pull_Date_Initial"] = combined_data_sub["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        table_info_sub = con.execute(f"DESCRIBE {table_name_sub}").fetchdf()
        correct_order_sub = table_info_sub['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data_sub = combined_data_sub[correct_order_sub]   

    # If loading data for the first time
    else:
        combined_data_sub = scraped_data_sub
    # Post processing before loading into duckdb
        combined_data_sub['Pull_Date_Initial'] = pd.to_datetime(combined_data_sub['Pull_Date_Initial'])
        # Sort the DataFrame. Note: Sorting "Pull_Date_Initial" isn't required. Only if a payment number is duplicated for some reason.
        combined_data_sub = combined_data_sub.sort_values(by=["Project_Number","Pull_Date_Initial"], ascending=[True,False])
        # Revert the formatting of the date column
        combined_data_sub["Pull_Date_Initial"] = combined_data_sub["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        table_info_sub = con.execute(f"DESCRIBE {table_name_sub}").fetchdf()
        correct_order_sub = table_info_sub['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data_sub = combined_data_sub[correct_order_sub] 

    # Replace the table with the updated data
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

    # Replace the sub_table with the updated data
    con.execute(f"DELETE FROM {table_name_sub}")
    con.execute(f"INSERT INTO {table_name_sub} SELECT * FROM combined_data_sub")

    # Close connection
    con.close()
    print("LA_City scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'LA_City scraping completed and DUCKDB file updated Successfully.')
    return combined_data, combined_data_sub

def data_appended_la(combined_data: pd.DataFrame, combined_data_sub: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    appended_data = combined_data[combined_data["Pull_Date_Initial"] == current_date]
    appended_data_sub = combined_data_sub[combined_data_sub["Pull_Date_Initial"] == current_date]
    if appended_data.empty and appended_data_sub.empty:
        print('Data and sub data not yet updated on Website.')
        logging.info(
            'Data not yet updated on Website.'
        )
    elif appended_data.empty:
        print('Data not yet updated on Website.')
        logging.info(
            'Data not yet updated on Website.'
        )
    elif appended_data_sub.empty:
        print('Sub Data not yet updated on Website. Successfully appended latest main data')
        logging.info(
            'Sub Data not yet updated on Website. Successfully appended latest main data'
        )
    else:
        print('Successfully appended latest data.')
        logging.info('Successfully appended latest data.')
    
    return appended_data, appended_data_sub