import duckdb
import logging
import pandas as pd
from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import InvalidArgumentException
from datetime import datetime
import pytz
import time
import os
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Automatic driver installer
service = Service(ChromeDriverManager().install())
THIS_DIR      = os.path.dirname(os.path.abspath(__file__))
CHUNK_SIZE: int    = int(os.getenv("OK_CHUNK_SIZE", 100))  # Default chunk size is set to 100 if not specified in the environment variable
PROGRESS_FILE: str = os.path.join(THIS_DIR, "ok_progress.txt")

HEADER_OK: List[str] = ["contract_id", "payment_number","date_let","ntp_eff_date","pay_period","date_awarded","date_work_began","org_cont_time","date_cont_executed","date_time_stopped","current_time_charged","date_ntp_issued",
                    "completion_date","current_time_allowed","general_liability_exp","workman_comp_exp","percent_time_used","spec_year","date_approved","bid_amount","funds_available_bid_co","percent_complete",
                    "unearned_balance", "total_to_date","prev_to_date","this_estimate", "project_numbers", "primary_job_num", "cont_desp","primary_county","road_name","prime_cont","surety_company"]

def _load_progress() -> int:
    try:
        return int(open(PROGRESS_FILE).read().strip())
    except Exception:
        return 0


def _save_progress(idx: int) -> None:
    with open(PROGRESS_FILE, "w") as fh:
        fh.write(str(idx))
    logging.info("[OK] progress pointer → %s", idx)

def _scrape_ok_chunk(contract_numbers: List[str]) -> List[List[str]]:
    rows: List[List[str]] = [] 
    # URL of the site
    url = 'https://www.odot.org/CONTRACTADMIN/ESTIMATES/'

    # Start a new browser session
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    original_window = driver.window_handles[0]
    
    try:

        for i,value in enumerate(contract_numbers):
            
            # Toggle the radio button filter
            contract_id_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "filt1"))
            )
            contract_id_button.click()

            # Find the contract number input box and enter the contract number
            contract_id_input = driver.find_element(By.ID, "filt1v")
            contract_id_input.clear()
            contract_id_input.send_keys(value)
            apply = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="menuBar"]/input[8]'))
            )
            apply.click()
            # Submit the search form
            try:
                contract_id_link = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.LINK_TEXT, value))
                )
                contract_id_link.click()            
                all_reports = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="main"]/table/tbody/tr/td'))
                )
                report_counter = 0
                for i in range(len(all_reports) - 1, -1, -1): #iterate backwards through the list
                    if report_counter > 3: # fetch last 4 reports only since historical data has been scraped already
                        break
                    row = all_reports[i].find_element(By.XPATH,"./a")
                    if "index" not in row.get_attribute('textContent') and row.get_attribute('textContent').isdigit(): # if only the latest report is needed, the loop should be ended the first time the condition is satisfied. conditional logic can be improved
                        est_num = row.get_attribute('textContent')
                        report = row
                        report.click()
                        time.sleep(1) # addresses race conditions. Expected issue to be resolved- WebDriverException: no such execution context 
                        # change the window of the driver since new tab is opened
                        new_window_handle = [handle for handle in driver.window_handles if handle != original_window][-1]
                        driver.switch_to.window(new_window_handle)
                        table = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.XPATH, '/html/body/table[3]/tbody/tr'))
                        )
                
                        current_data = []
                        current_data.append(value)
                        current_data.append(est_num)
                        for row in table:
                            cells = row.find_elements(By.XPATH, './td')
                            counter = 0
                            for cell in cells:
                                if counter % 2 != 0: # If the iteration is odd
                                    current_data.append(cell.text)
                                counter += 1
                        if len(current_data) < 18:
                            driver.close()
                            driver.switch_to.window(original_window)
                            continue
                        del current_data[18] # Removes extra empty values from empty cells
                        
                        bid_amount = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[1]/td[2]').text
                        funds_available = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[2]/td[2]').text
                        percent_complete = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[3]/td[2]').text
                        unearned_balance = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[4]/td[2]').text
                        total_to_date = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[9]/td[4]').text
                        prev_to_date = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[9]/td[5]').text
                        this_estimate = driver.find_element(By.XPATH, '/html/body/table[4]/tbody/tr[9]/td[6]').text

                        current_data.append(bid_amount)
                        current_data.append(funds_available)
                        current_data.append(percent_complete)
                        current_data.append(unearned_balance)
                        current_data.append(total_to_date)
                        current_data.append(prev_to_date)
                        current_data.append(this_estimate)
                        

                        additional_info_table = WebDriverWait(driver, 10).until(
                            EC.presence_of_all_elements_located((By.XPATH, '/html/body/table[2]/tbody/tr/td[2]'))
                        )
                        for j, row in enumerate(additional_info_table):
                            current_data.append(row.text)
                        del current_data[-3:-1]
                        if len(current_data) != 33:
                            print("check data for contract id", value)
                        else:
                            rows.append(current_data)
                        driver.close()
                        driver.switch_to.window(original_window)
                        report_counter += 1
                est_portal_link = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.LINK_TEXT, 'Estimates Portal'))
                    )
                est_portal_link.click()
                
            except TimeoutException:    
                print(value,"Not Found")
            except InvalidArgumentException:   # When true it indicates that the input contract ID is a numeric data type and needs to be changed to string
                print(value,"Not Found - change to string dtype")
        
    finally:
        # Close the browser
        driver.quit()
    return rows

def scrape_raw_ok() -> pd.DataFrame:
    url = 'https://www.odot.org/CONTRACTADMIN/ESTIMATES/'
 
    contract_numbers = []
    # Start a new browser session
    driver = webdriver.Chrome(service=service)
    try:
        driver.get(url) 
        bulk_contracts = True
        if bulk_contracts:
                all_contracts = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="main"]/div/a'))
                )
                all_contracts_list = []
                for row in all_contracts:
                    all_contracts_list.append(row.text)
                contract_numbers = all_contracts_list
        elif not bulk_contracts:
            contract_numbers = contract_numbers
    finally:
        driver.quit()
    all_rows: List[List[str]] = []
    start_idx = _load_progress()
    remaining = contract_numbers[start_idx:]

    if not remaining:
        if os.path.exists(PROGRESS_FILE): os.remove(PROGRESS_FILE)
        return pd.DataFrame(columns=HEADER_OK)

    total = len(remaining)
    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk_end  = min(chunk_start + CHUNK_SIZE, total)
        subset     = remaining[chunk_start:chunk_end]
        abs_start  = start_idx + chunk_start
        logging.info("[OK] chunk %s-%s", abs_start, abs_start+len(subset)-1)

        try:
            rows = _scrape_ok_chunk(subset)
            chunk_df = pd.DataFrame(rows, columns=HEADER_OK)
            all_rows.extend(rows)
            _save_progress(start_idx + chunk_end)

        except Exception as exc:
            logging.exception("[OK] chunk failed: flushing & retrying", exc_info=exc)
            if all_rows:
                tmp_df = pd.DataFrame(all_rows, columns=HEADER_OK)
                transform_and_load_ok(tmp_df)
                print("Executed transform_and_load_ok() for the last successful chunk.")
            _save_progress(start_idx + chunk_start)
            driver.quit()
            raise

    
    if os.path.exists(PROGRESS_FILE): 
        os.remove(PROGRESS_FILE)

    # Create a dataframe   
    ok_dot_data = pd.DataFrame(data=all_rows, columns = HEADER_OK)
    return ok_dot_data

def transform_and_load_ok(ok_dot_data: pd.DataFrame) -> pd.DataFrame:
    try:
        # POST PROCESSING
        df = ok_dot_data.copy()
        df.rename(columns = {'contract_id':'Contract_Number','payment_number':'Payment_Number','project_numbers':'Project_Number','date_let':'Project_Letting',
                            'date_awarded':'Project_Bid_Awarded','ntp_eff_date':'Project_Start_Effective','date_work_began':'Project_Start_Actual',
                            'cont_desp':'Project_Description','pay_period':'Payment_Work_Period','bid_amount':'Project_Cost_Total','funds_available_bid_co':'Project_Cost_Adjusted',
                            'completion_date':'Project_Comp_Final','date_time_stopped':'Project_Charges_Stopped','surety_company':'Bond_Company_Name',
                            'prime_cont':'Contractor_Name','this_estimate':'Payment_Amount','date_approved':'Payment_Approved','total_to_date':'Payment_Amount_Total',
                            'unearned_balance':'Payment_Balance','percent_complete':'Payment_Total_Percent'}, inplace=True)


        def parse_money(value):
            # Remove dollar signs and commas
            value = value.replace('$', '').replace(',', '')
            # Convert values in parentheses to negative numbers
            if '(' in value and ')' in value:
                value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
            return float(value)

        df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
        df['Project_Cost_Adjusted'] = df['Project_Cost_Adjusted'].apply(parse_money)
        df['Payment_Balance'] = df['Payment_Balance'].apply(parse_money)
        df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
        df['prev_to_date'] = df['prev_to_date'].apply(parse_money)
        df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)

        # convert "Payment_Number" column to integer type to avoid duplicate records when integrated with duckdb
        df['Payment_Number'] = df['Payment_Number'].astype(int)

        df['org_cont_time'] = pd.to_numeric(df['org_cont_time'], errors='coerce')
        df['current_time_allowed'] = pd.to_numeric(df['current_time_allowed'], errors='coerce')

        # replace 0 with blanks for the following date columns
        df[['general_liability_exp', 'workman_comp_exp']] = df[['general_liability_exp', 'workman_comp_exp']].replace(0, '')
        EST = pytz.timezone('US/Eastern')
        now = datetime.now(EST)
        current_date = now.strftime("%m/%d/%Y")
        df["Pull_Date_Initial"] = current_date

        # DUCKDB INTEGRATION
        # File to store DuckDB data
        db_path = os.getenv("DB_PATH")
        db_file = rf"{db_path}\Oklahoma\data_store_OK.duckdb"
        table_name = "OK_DOT"

        # Current scraped data
        scraped_data = df.copy()

        # Connect to DuckDB
        con = duckdb.connect(db_file)
        # Fields to be calculated


        # Create table if not exists
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        Contract_Number  TEXT,
        Project_Number  TEXT,
        primary_job_num TEXT,
        Payment_Number  INTEGER,
        Project_Description	TEXT,
        primary_county TEXT,
        Contractor_Name TEXT,
        Bond_Company_Name TEXT,
        road_name TEXT,

        org_cont_time DOUBLE,
        date_cont_executed TEXT,
        Project_Charges_Stopped TEXT,
        current_time_charged TEXT,
        current_time_allowed DOUBLE,
        general_liability_exp TEXT,
        workman_comp_exp TEXT,
        percent_time_used TEXT,
        spec_year TEXT,

        Project_Cost_Total DOUBLE,
        Project_Cost_Adjusted DOUBLE,
        Project_Cost_Additions DOUBLE,

        Payment_Balance DOUBLE,
        Payment_Amount_Total DOUBLE,
        prev_to_date DOUBLE,
        Payment_Amount DOUBLE,
        Payment_Total_Percent TEXT,
        Payment_Amount_Percent DOUBLE,


        Pull_Date_Initial TEXT,
        Payment_Approved TEXT,
        Payment_Date TEXT,
        Project_Letting TEXT,
        Project_Bid_Awarded TEXT,
        Project_Start_Effective TEXT,
        Project_Start_Actual TEXT,
        Payment_Work_Period TEXT,
        Project_Comp_Final TEXT,
        date_ntp_issued TEXT,
        Project_Comp_Est TEXT,
        Project_Comp_Substantial TEXT
        )
        """)

        # Insert or Update Logic
        # Load existing data from DuckDB
        existing_data = con.execute(f"SELECT * FROM {table_name}").df()

        # Deduplicate and merge
        if not existing_data.empty:
            combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
            # find duplicates by all columns except the columns below since they are calculated later. Later, a logic can be developed to find revised payments if any
            combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial','Project_Cost_Additions','Payment_Amount_Percent'
                                                                                            'Project_Comp_Est','Project_Comp_Substantial','Payment_Date'])].columns,keep="first") 
            # Post processing before loading into duckdb
            combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Number"], ascending=[True,False])

            # Calculate derived fields safely
            combined_data['Project_Cost_Additions'] = combined_data['Project_Cost_Adjusted'] - combined_data['Project_Cost_Total']
            combined_data['Payment_Amount_Percent'] = round(
            combined_data['Payment_Amount'] / combined_data['Payment_Amount_Total'].replace(0, pd.NA) * 100, 2
            )
            # Extract the last date string using string slicing (equivalent to SUBSTR)
            combined_data['Payment_Date'] = combined_data['Payment_Work_Period'].str[-10:]

            # Convert and calculate date-based fields
            combined_data['date_ntp_issued'] = pd.to_datetime(combined_data['date_ntp_issued'], errors='coerce')
            combined_data['Project_Comp_Est'] = combined_data['date_ntp_issued'] + pd.to_timedelta(combined_data['org_cont_time'], unit='D')
            combined_data['Project_Comp_Substantial'] = combined_data['date_ntp_issued'] + pd.to_timedelta(combined_data['current_time_allowed'], unit='D')

            # Format dates back to strings
            combined_data['Project_Comp_Est'] = combined_data['Project_Comp_Est'].dt.strftime('%m/%d/%Y')
            combined_data['Project_Comp_Substantial'] = combined_data['Project_Comp_Substantial'].dt.strftime('%m/%d/%Y')
            combined_data['date_ntp_issued'] = combined_data['date_ntp_issued'].dt.strftime('%m/%d/%Y')

            table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
            correct_order = table_info['column_name'].tolist()
            # Reorder the DataFrame to avoid conversion errors
            combined_data = combined_data[correct_order]

        else:
            combined_data = scraped_data.copy()
            # Post processing before loading into duckdb
            combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Number"], ascending=[True,False])
            # Calculate derived fields safely
            combined_data['Project_Cost_Additions'] = combined_data['Project_Cost_Adjusted'] - combined_data['Project_Cost_Total']
            combined_data['Payment_Amount_Percent'] = round(
            combined_data['Payment_Amount'] / combined_data['Payment_Amount_Total'].replace(0, pd.NA) * 100, 2
            )
            # Extract the last date string using string slicing (equivalent to SUBSTR)
            combined_data['Payment_Date'] = combined_data['Payment_Work_Period'].str[-10:]

            # Convert and calculate date-based fields
            combined_data['date_ntp_issued'] = pd.to_datetime(combined_data['date_ntp_issued'], errors='coerce')
            combined_data['Project_Comp_Est'] = combined_data['date_ntp_issued'] + pd.to_timedelta(combined_data['org_cont_time'], unit='D')
            combined_data['Project_Comp_Substantial'] = combined_data['date_ntp_issued'] + pd.to_timedelta(combined_data['current_time_allowed'], unit='D')

            # Format dates back to strings
            combined_data['Project_Comp_Est'] = combined_data['Project_Comp_Est'].dt.strftime('%m/%d/%Y')
            combined_data['Project_Comp_Substantial'] = combined_data['Project_Comp_Substantial'].dt.strftime('%m/%d/%Y')
            combined_data['date_ntp_issued'] = combined_data['date_ntp_issued'].dt.strftime('%m/%d/%Y')

            table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
            correct_order = table_info['column_name'].tolist()
            # Reorder the DataFrame to avoid conversion errors
            combined_data = combined_data[correct_order]

        # Replace the table with the updated data
        print(combined_data.shape)
        con.execute(f"DELETE FROM {table_name}")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

        # Close connection
        con.close()
        print("Oklahoma scraping completed and DUCKDB file updated Successfully.")
        logging.info(
            'Oklahoma scraping completed and DUCKDB file updated Successfully.')
        return combined_data
    
    except Exception as e:
        print(f"Error in west_virginia_scraping: {e}")

def data_appended_ok(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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
  