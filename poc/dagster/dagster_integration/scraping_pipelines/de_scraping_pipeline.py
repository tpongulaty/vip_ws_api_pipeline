import duckdb
import logging
from typing import List
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from datetime import datetime
from datetime import timedelta
import pytz
import time
import os
import re
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys


from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

CHUNK_SIZE = int(os.getenv("DE_CHUNK_SIZE", 100))
THIS_DIR = os.path.dirname(os.path.abspath(__file__))          # >>> ADDED
PROGRESS_FILE = os.path.join(THIS_DIR, "de_progress.txt")      # >>> ADDED

# Initialize the Chrome WebDriver service
service = Service(ChromeDriverManager().install())
def _load_progress() -> int:                                   # >>> ADDED
    try:
        return int(open(PROGRESS_FILE).read().strip())
    except Exception:
        return 0

def _save_progress(idx: int) -> None:                          # >>> ADDED
    with open(PROGRESS_FILE, "w") as fh:
        fh.write(str(idx))

HEADER_DE: List[str] = []

def extract_text(cell):
    """Extracts text from a cell, replacing <br> with new lines."""
    return cell.get_attribute('innerHTML').replace('<br>', '\n').strip()

def _scrape_de_chunk(contract_numbers: List[str]) -> List[List[str]]:
    """Scrape one batch of Delaware contract numbers"""
    global HEADER_DE
    if not contract_numbers:
        return [], HEADER_DE
    url = 'https://deldot.gov/Business/ProjectStatusQuery/'
    opts = Options()    
    opts.add_argument("--headless")
    driver = webdriver.Chrome(service=service, options=opts)
    driver.get(url)
    driver.set_window_size(1920, 1080)  # adjust window size to avoid elements from overlapping

    rows: List[List[str]] = []
    original_window = driver.window_handles[0]
    
    try:
        for t,value in enumerate(contract_numbers):
            print(f"iteration {t}th, working on {value}")
            # Wait for the contract number input box to be present
            state_no_input = WebDriverWait(driver, 15).until(     
                EC.visibility_of_element_located((By.CSS_SELECTOR, '#stateNoFilter input')) 
            ) 
            state_no_input.send_keys(value)
            try:
                contract_number_link = WebDriverWait(driver, 70).until(
                    EC.presence_of_element_located((By.LINK_TEXT, value))
                )
            
                contract_number_link.click()
            
                # Find tables
                info_table2 = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="project-info"]/div/table[2]/tbody/tr'))
                )
                info_table1 = WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="project-info"]/div/table[1]/tbody/tr'))
                )
                
                try:
                # Extract the table headers
                    if not HEADER_DE:
                        payment_header = driver.find_element(By.XPATH, '//table[@id="paymentInfo"]/thead/tr').find_elements(By.XPATH,'.//th') 
                        for h,header in enumerate(payment_header):
                            if h != 1 and h != 2:
                                HEADER_DE.append(header.get_attribute("textContent").strip())
                            else:
                                continue
                        HEADER_DE = ['project_number'] + ['description'] + ['justification'] + ['advertise_date'] + ['bid_amount'] + ['bids_received_date'] + ['date_awarded'] + ['total_project_cost'] + ['first_chargeable_day'] + ['contractor'] + ['estimated_completion_date'] +['company_name']+['address']+['attorney_name']+['attorney_company_name'] + ['attorney_address'] + ['attorney_phone_num'] + ['bond_number'] + HEADER_DE+['Estimated_Cost_of_Final_Contract'] + ['Total_Estimate_to_Date'] + ['Contract_Award_Price'] + ['Auth_Retainage'] + ['Proposed_Time']+['Held_in_Securities'] + ['Time_Extended'] + ['Amt_to_be_Retained'] + ['Total_Time'] + ['Previous_Payment'] + ['Previous_time_charged']+['Liq_Damages_($/Day)'] + ['Time_Used_this_period'] + ['Time_Remaining'] + ['Misc_Deduction'] + ['Incentive_Prg(+/-)'] + ['Total_Deduction']+['Amount_this_Estimate'] # delete 'Amount_this_Estimate' later as it is same as 'Est Amt'
                except NoSuchElementException:
                    print(value,"Out of scope/invalid format. Re-check periodically") 
                    driver.get(url)
                    continue
                
                # Extract Total Project Cost
                contract_info = []
                contract_info.append(value)
                for j, info in enumerate(info_table1):
                    cells = info.find_elements(By.XPATH,'./td') 
                    if j == 1 or j == 2:
                        contract_info.append(cells[1].get_attribute('textContent'))

                for c, row in enumerate(info_table2):
                    cells = row.find_elements(By.XPATH,'./td') 
                    if c == len(info_table2) - 1 or c == 1:
                        contract_info.append(cells[1].get_attribute('textContent'))
                    else: 
                        contract_info.append(cells[1].get_attribute('textContent'))
                        contract_info.append(cells[3].get_attribute('textContent'))
                try:
                    info_table_bond = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="bond-info"]/div/table/tbody/tr'))
                    )
                    for k, row in enumerate(info_table_bond):
                        cells = row.find_elements(By.XPATH,'./td')
                        if k == len(info_table_bond) - 1:
                            contract_info.append(extract_text(cells[1]))
                        else:
                            contract_info.append(extract_text(cells[1]))
                            contract_info.append(extract_text(cells[3]))
                except TimeoutException:
                    # Append empty values when bond information is not available to avoid assertion errors while creating data frames
                    for i in range(7):
                        contract_info.append(" ")    
                    print("No Bond Information",value)
                

                # if len(contract_info) != 17:
                #     print("check contract number data", value, i)
                time.sleep(5) # wait for transactions to load
                # Extract current amount
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//table[@ID="paymentInfo"]/tbody/tr'))
                )
                payment_data = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//table[@ID="paymentInfo"]/tbody/tr'))
                )
                for row in reversed(payment_data[-3:]): # Pull in the last 3 reports only
                    current_row = []
                    row_data = WebDriverWait(row, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, './td'))
                    )
                    if len(payment_data) <= 1 and len(row_data) <= 1:
                        current_row.extend(contract_info)
                        current_row.append("No Payment History Available")
                        rows.append(current_row)
                        break

                    current_row.extend(contract_info)
                    # current_row.extend([cell.get_attribute("textContent") for cell in row_data if cell.get_attribute("textContent")])
                    for i, data in enumerate(row_data):
                        if i == 1:
                            header_detail_link = data.find_element(By.XPATH,'./a')
                    current_row.extend([cell.text for cell in row_data])
                    del current_row[19:21]
                    header_detail_link.send_keys(Keys.CONTROL + Keys.RETURN)
                    
                    # change the window of the driver since new tab is opened
                    new_window_handle = [handle for handle in driver.window_handles if handle != original_window][-1]
                    driver.switch_to.window(new_window_handle)
                    time.sleep(0.5) # wait for page content to be loaded properly
                    header_detail_info = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '//*[@id="project-info"]/div/table/tbody/tr'))
                    )
                    for k, detail in enumerate(header_detail_info):
                        cell_data = detail.find_elements(By.XPATH,'./td')
                        # Below conditions ensure inconsistent arrangement of data elements is handled
                        if k < 8 and k != 6:
                            # Extract only the values not the headers
                            current_row.append(cell_data[1].get_attribute('textContent'))
                            current_row.append(cell_data[3].get_attribute('textContent'))
                        elif k == 6:
                            current_row.append(str(cell_data[1].get_attribute('textContent')) + "-" + str(cell_data[2].get_attribute('textContent')))
                        else:
                            current_row.append(cell_data[3].get_attribute('textContent'))

                    # Go back to details page
                    # details_page = WebDriverWait(driver, 10).until(
                    # EC.presence_of_element_located((By.LINK_TEXT, '[Back to Details Page]'))
                    # )
                    rows.append(current_row)
                    driver.close()
                    driver.switch_to.window(original_window)
                    # details_page.click() # go back to all estimates
                    
                        
            except TimeoutException:        
                print([value,t],"Not Found")

            # except StaleElementReferenceException:
            #     print(value,"Loading Error")
                    
            driver.get(url)
    
        
    finally:
        # Close the browser
        driver.quit()
    return rows, HEADER_DE
    

def scrape_raw_de() -> pd.DataFrame:
    # Retrieve all contract numbers

    # URL of the site
    url = 'https://deldot.gov/Business/ProjectStatusQuery/'
    # Start a new browser session
    opts = Options()    
    opts.add_argument("--headless")
    driver = webdriver.Chrome(service=service, options=opts)
    driver.get(url)
    driver.set_window_size(1920, 1080)  # adjust window size to avoid elements from overlapping
    contract_numbers_del = []
    header_contract_del = ["contract_number","awarded_date"]

    time.sleep(12) # Next button is disabled initially while page is loading. Explicit wait is required
    try:
        next_page_button = WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="property_next"]'))
                )        
        while "disabled" not in next_page_button.get_attribute("class"): 
        # wait to load
            body = WebDriverWait(driver, 15).until(
                        EC.presence_of_all_elements_located((By.XPATH, '//*[@id="property"]'))
                    )
            WebDriverWait(driver, 15).until(
                        EC.presence_of_all_elements_located((By.XPATH, '//*[@id="property"]/tbody/tr[1]/td[2]/a'))
                    )
            contract_numbers = driver.find_elements(By.XPATH, '//*[@id="property"]/tbody/tr')
            for row in contract_numbers:
                current_contract = []
                cell_2 = row.find_element(By.XPATH, "./td[2]/a")
                cell_5 = row.find_element(By.XPATH, "./td[5]")
                current_contract.append(cell_2.text)
                current_contract.append(cell_5.text)
                # if len(current_contract) < 2:
                #     current_contract.append(" ")
                contract_numbers_del.append(current_contract)
            next_page_button.click()
            next_page_button = WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="property_next"]'))
            ) 

    finally:
        # Close the browser
        driver.quit()
    del_contracts = pd.DataFrame(data=contract_numbers_del, columns = header_contract_del)

    # Filter out inactive contracts
    df_updated = del_contracts[del_contracts['awarded_date'] != '']
    contract_numbers_del_filtered = df_updated['contract_number'].to_list()

    # Filter out completed contracts
    # Create connection to duckdb table
    db_path = os.getenv("DB_PATH")
    db_file = rf"{db_path}\Delaware\data_store_de.duckdb"
    con = duckdb.connect(db_file)
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM DL_DOT").df()
    con.close()
    df1 = existing_data.drop_duplicates(subset=["Project_Number"], keep='last')
    print(df1.shape)
    print(len(df1['Project_Number'].unique()))

    # Convert 'Payment_Date' to datetime format
    df1['Payment_Date'] = pd.to_datetime(df1['Payment_Date'], format='%m/%d/%Y', errors='coerce')
    df1['Project_Bid_Awarded'] = pd.to_datetime(df1['Project_Bid_Awarded'], format='%m/%d/%Y', errors='coerce')

    # Define the filter conditions
    condition1 = (df1['Payment_Date'].dt.year <= 2024) & (df1['Payment_Total_Percent'] >= 100)
    condition2 = (df1['Payment_Date'].dt.year <= 2023) & (df1['Payment_Total_Percent'] >= 95)
    condition3 = df1['Payment_Date'].dt.year <= 2021
    condition4 = df1['Payment_Status'] == "Final"
    condition5 = (df1['Project_Bid_Awarded'].dt.year <= 2010)

    # Apply filtering: Keep only rows that satisfy any of the conditions
    filtered_df = df1[(condition1 | condition2 | condition3 | condition4 | condition5)]
    inactive_contracts_de = filtered_df['Project_Number'].to_list()
    # Filter out inactive contracts for scraping
    contract_numbers_de_updated = [num for num in contract_numbers_del_filtered if num not in inactive_contracts_de]
    print(len(contract_numbers_de_updated))

    all_rows: List[List[str]] = []
    start_idx = _load_progress()
    remaining = contract_numbers_de_updated[start_idx:]
    if not remaining:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        return pd.DataFrame(columns=HEADER_DE)

    total = len(remaining)
    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total)
        subset   = remaining[chunk_start:chunk_end]
        abs_start = start_idx + chunk_start
        logging.info("[DE] chunk %s-%s", abs_start, abs_start+len(subset)-1)

        try:
            rows, _ = _scrape_de_chunk(subset)

            # 1️: check each row, not the whole list
            good_rows = [r for r in rows if len(r) == len(HEADER_DE)]

            if good_rows:                                    # at least one full row
                # 2️: validate only the good rows
                pd.DataFrame(rows, columns=HEADER_DE)   # throws if header changes
                all_rows.extend(rows)                   # 3️: keep only good rows
                _save_progress(start_idx + chunk_end)
                continue                                     # next chunk

            # ── no good rows in this chunk ─────────────────────────────────────────
            logging.warning("[DE] chunk %s-%s had no full-length rows", abs_start,
                            abs_start + len(subset) - 1)

            
            logging.info("No data collected in this chunk yet; just saving progress")

            _save_progress(start_idx + chunk_end)            # still advance pointer
            # we *skip* the empty chunk but do NOT raise → continue run
            continue

        except Exception as exc:
            # 4️: any real scrape/driver error
            logging.exception("[DE] chunk failed: flushing & retrying", exc_info=exc)
            if all_rows:
                tmp_df = pd.DataFrame(all_rows, columns=HEADER_DE)
                transform_and_load_de(tmp_df)
            _save_progress(start_idx + chunk_start)
            driver.quit()
            raise 

    driver.quit()
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
    good_rows = [r for r in all_rows if len(r) == len(HEADER_DE)]
    if good_rows:
        de_dot_data = pd.DataFrame(all_rows, columns=HEADER_DE)
        return de_dot_data
    else:
        return pd.DataFrame(columns=HEADER_DE)

def transform_and_load_de(del_dot_data: pd.DataFrame) -> pd.DataFrame:
    # POST PROCESSING
    df = del_dot_data.copy()
    del df['Amount_this_Estimate']

    df.rename(columns = {'project_number':'Project_Number','description':'Project_Description','justification':'Project_Justification','advertise_date':'Project_Advertise',
                        'bid_amount':'Project_Bid_Amount','bids_received_date':'Project_Bid_Rcvd','date_awarded':'Project_Bid_Awarded','total_project_cost':'Project_Cost_Total',
                        'first_chargeable_day':'Project_Start_Actual','contractor':'Contractor_Name','estimated_completion_date':'Project_Comp_Est','bond_number':'Bond_Number',
                        'company_name':'Bond_Company_Name','address':'Bond_Company_Address','attorney_name':'Bond_Attorney_Contact','attorney_company_name':'Bond_Attorney_Name',
                        'attorney_address':'Bond_Attorney_Address','attorney_phone_num':'Bond_Attorney_Phone','Est No':'Payment_Number','Status':'Payment_Status','Est Amt':'Payment_Amount',
                        'Payment Type':'Payment_Type','Advice No':'Payment_CheckNo','Advice Date':'Payment_Date','Total_Estimate_to_Date':'Payment_Amount_Total',
                        'Liq_Damages_($/Day)':'Liq_Damages_Per_Day','Incentive_Prg(+/-)':'Incentive_Prg_pos_or_neg'
                        }, inplace=True)

    def parse_money(value):
        if not value:  # Check for None, empty string, or NaN
            return None
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value) if value.strip() else None

    df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
    df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
    df['Project_Bid_Amount'] = df['Project_Bid_Amount'].apply(parse_money)
    df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)
    df['Estimated_Cost_of_Final_Contract'] = df['Estimated_Cost_of_Final_Contract'].apply(parse_money)
    df['Contract_Award_Price'] = df['Contract_Award_Price'].apply(parse_money)
    df['Auth_Retainage'] = df['Auth_Retainage'].apply(parse_money)
    df['Held_in_Securities'] = df['Held_in_Securities'].apply(parse_money)
    df['Amt_to_be_Retained'] = df['Amt_to_be_Retained'].apply(parse_money)
    df['Liq_Damages_Per_Day'] = df['Liq_Damages_Per_Day'].apply(parse_money)
    df['Previous_Payment'] = df['Previous_Payment'].apply(parse_money)
    df['Misc_Deduction'] = df['Misc_Deduction'].apply(parse_money)
    df['Incentive_Prg_pos_or_neg'] = df['Incentive_Prg_pos_or_neg'].apply(parse_money)
    df['Total_Deduction'] = df['Total_Deduction'].apply(parse_money)

    df = df[df['Payment_Number'].str.isnumeric()]
    df['Payment_Number'] = df['Payment_Number'].astype(int)
    # Calculate Payment Balance
    df['Payment_Balance'] = df['Project_Cost_Total'] - df['Payment_Amount_Total']

    # Function to extract components
    def extract_address_components(address):
        lines = address.split("\n")  # Split into lines
        
        if len(lines) < 2:
            return pd.NA, pd.NA, pd.NA, pd.NA  # Handle unexpected formats
        
        city_state_zip = lines[-1]  # Last line contains City, State, Zip
        parts = city_state_zip.split()  # Split by spaces

        if len(parts) < 3:  # Need at least City, State, Zip
            print(f"Malformed address: {address}")
            return pd.NA, pd.NA, pd.NA, pd.NA

        zipcode = parts[-1]  # Last part is ZIP code
        state = parts[-2]  # Second last is state
        city = " ".join(parts[:-2])  # Everything before state is city
        
        street = " ".join(lines[:-1])  # Everything before last line is street

        return street, city, state, zipcode

    # Apply function to DataFrame
    df[['Bond_Company_Street', 'Bond_Company_City', 'Bond_Company_State', 'Bond_Company_Zipcode']] = df['Bond_Company_Address'].apply(
        lambda x: pd.Series(extract_address_components(x))
    )
    df[['Bond_Attorney_Street', 'Bond_Attorney_City', 'Bond_Attorney_State', 'Bond_Attorney_Zipcode']] = df['Bond_Attorney_Address'].apply(
        lambda x: pd.Series(extract_address_components(x))
    )

    # Strip all leading and trailing whitespaces in all the string columns of a dataframe, before using drop_suplicates()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # Convert and compute directly into Payment_Work_Period
    def compute_work_period(payment_date_str, Time_Used_this_period_str):
        try:
            payment_date = pd.to_datetime(payment_date_str, format='%m/%d/%Y', errors='coerce')
            if pd.isnull(payment_date):
                return None
            match = re.search(r'(\d+)', str(Time_Used_this_period_str))
            if not match:
                return None
            days = int(match.group(1))
            start_date = payment_date - timedelta(days=days)
            return f"{start_date.strftime('%m/%d/%Y')} to {payment_date.strftime('%m/%d/%Y')}"
        except Exception:
            return None

    # Extract number of days safely and compute max per Project_Number
    def get_max_days(series):
        days = series.dropna().apply(lambda s: int(re.search(r'(\d+)', str(s)).group(1)) if re.search(r'(\d+)', str(s)) else None)
        return days.max(skipna=True)

    # Compute the Project_Comp_Substantial column
    def compute_substantial_date(row):
        bid_date = row['Project_Bid_Awarded']
        max_days = max_days_per_project.get(row['Project_Number'], None)
        if pd.notnull(bid_date) and pd.notnull(max_days):
            completion_date = bid_date + timedelta(days=int(max_days))
            return completion_date.strftime('%m/%d/%Y')
        else:
            return None

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_path = os.getenv("DB_PATH")
    db_file = rf"{db_path}\Delaware\data_store_de.duckdb"
    table_name = "DL_DOT"

    # Current scraped data
    scraped_data = df
    print(df.columns)
    print(len(df.columns))
    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Contractor_Name  TEXT,
    Project_Number  TEXT,
    Payment_Number  INTEGER,
    Project_Description	TEXT,
    Project_Justification  TEXT,
    Project_Comp_Est  TEXT,
    Project_Comp_Substantial  TEXT,
    Project_Advertise  TEXT,
    Project_Bid_Rcvd  TEXT,	
    Project_Bid_Awarded  TEXT,	
    Project_Start_Actual  TEXT,
    Payment_Work_Period  TEXT,

    Bond_Number  TEXT,
    Bond_Company_Name  TEXT,
    Bond_Company_Address  TEXT,
    Bond_Attorney_Contact  TEXT,
    Bond_Attorney_Name  TEXT,
    Bond_Attorney_Address  TEXT,
    Bond_Attorney_Phone  TEXT,
    Bond_Company_Street  TEXT, 
    Bond_Company_City  TEXT, 
    Bond_Company_State  TEXT, 
    Bond_Company_Zipcode  TEXT,
    Bond_Attorney_Street  TEXT, 
    Bond_Attorney_City  TEXT, 
    Bond_Attorney_State  TEXT, 
    Bond_Attorney_Zipcode  TEXT,

    Project_Bid_Amount  DOUBLE,
    Payment_Amount DOUBLE,
    Payment_Status  TEXT,
    Payment_Type  TEXT,
    Payment_CheckNo  TEXT,
    Payment_Date  TEXT,	
    Project_Cost_Total  DOUBLE,	
    Payment_Amount_Total  DOUBLE,
    Payment_Total_Percent  FLOAT,
    Payment_Balance  DOUBLE,
    Pull_Date_Initial TEXT,
    Payment_Amount_Percent FLOAT,

    Estimated_Cost_of_Final_Contract  DOUBLE,
    Contract_Award_Price  DOUBLE, 
    Auth_Retainage  DOUBLE,
    Proposed_Time  TEXT, 
    Held_in_Securities  DOUBLE, 
    Time_Extended  TEXT,
    Amt_to_be_Retained  DOUBLE, 
    Total_Time  TEXT, 
    Previous_Payment  DOUBLE,
    Previous_time_charged  TEXT, 
    Liq_Damages_Per_Day  DOUBLE, 
    Time_Used_this_period  TEXT,
    Time_Remaining  TEXT, 
    Misc_Deduction  DOUBLE, 
    Incentive_Prg_pos_or_neg  DOUBLE,
    Total_Deduction  DOUBLE
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM {table_name}").df()

    print(len(existing_data.columns))
    # Deduplicate and merge
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
        # find duplicates by all columns except the columns below since they are calculated later. Later, a logic can be developed to find revised payments if any
        combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial', 'Payment_Amount_Percent', 'Payment_Total_Percent','Payment_Work_Period','Project_Comp_Substantial'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Project_Number","Payment_Number", "Pull_Date_Initial"], ascending=[True,True,False])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        combined_data['Payment_Work_Period'] = df.apply(lambda row: compute_work_period(row['Payment_Date'], row['Time_Used_this_period']), axis=1)

        # Convert Project_Bid_Awarded to datetime (we'll use string output later)
        combined_data['Project_Bid_Awarded'] = pd.to_datetime(combined_data['Project_Bid_Awarded'], format='%m/%d/%Y', errors='coerce')

        # Create a mapping from Project_Number → max days
        max_days_per_project = combined_data.groupby('Project_Number')['Total_Time'].apply(get_max_days)
        combined_data['Project_Comp_Substantial'] = combined_data.apply(compute_substantial_date, axis=1)

        # Revert the formatting of Project_Bid_Awarded column
        combined_data["Project_Bid_Awarded"] = combined_data["Project_Bid_Awarded"].dt.strftime('%m/%d/%Y')
        
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]

    else:
        combined_data = scraped_data
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Project_Number","Payment_Number", "Pull_Date_Initial"], ascending=[True,True,False])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        combined_data['Payment_Work_Period'] = combined_data.apply(lambda row: compute_work_period(row['Payment_Date'], row['Time_Used_this_period']), axis=1)

        # Convert Project_Bid_Awarded to datetime (we'll use string output later)
        combined_data['Project_Bid_Awarded'] = pd.to_datetime(df['Project_Bid_Awarded'], format='%m/%d/%Y', errors='coerce')
        # Create a mapping from Project_Number → max days
        max_days_per_project = combined_data.groupby('Project_Number')['Total_Time'].apply(get_max_days)
        combined_data['Project_Comp_Substantial'] = combined_data.apply(compute_substantial_date, axis=1)

        # Revert the formatting of Project_Bid_Awarded column
        combined_data["Project_Bid_Awarded"] = combined_data["Project_Bid_Awarded"].dt.strftime('%m/%d/%Y')
        
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]

    # Replace the table with the updated data
    print(combined_data)
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

    # Close connection
    con.close()
    print("Delaware scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'Delaware scraping completed and DUCKDB file updated Successfully.')
    return combined_data

def data_appended_de(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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