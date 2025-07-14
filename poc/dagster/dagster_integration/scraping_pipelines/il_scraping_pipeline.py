import re
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.keys import Keys
from datetime import datetime
import pytz
import logging
import duckdb
from dotenv import load_dotenv

load_dotenv()

def scrape_raw_il() -> pd.DataFrame:
    # Retrieve contract numbers
    # WebDriver Manager will handle everything automatically
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    # URL of the site
    url = 'https://apps1.dot.illinois.gov/WCP/PEstimate/PEstimateSearch'

    contract_numbers_il = []
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    driver.maximize_window() # resolves issue where dropdown buttons on this page are not visible when window is minimized

    try:
        time.sleep(10)
        wait = WebDriverWait(driver, 20)
        contract_num_button = wait.until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[1]/div[2]/div[1]/div/div/form[2]/div/div[2]/span/button'))
        )
        contract_num_button.click()

        contract_num_list = wait.until(
            EC.presence_of_all_elements_located((By.XPATH, '/html/body/div[4]/div/div/div[2]/ul/li'))
        )

        for i, row in enumerate(contract_num_list):
            number = row.get_attribute("textContent")
            contract_numbers_il.append(number)

    finally:
        driver.quit()

    # Get associated Contract information
    driver = webdriver.Chrome(service=service)
    # URL of the site
    url = 'https://apps1.dot.illinois.gov/WCP/'


    # Create connection to duckdb table
    db_file = r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\Illinois\data_store_IL.duckdb"
    con = duckdb.connect(db_file)
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM IL_DOT").df()
    con.close()
    df1 = existing_data.drop_duplicates(subset=["Contract_Number"], keep='first')

    # Convert 'Payment_Date' to datetime format
    df1['Payment_Date'] = pd.to_datetime(df1['Payment_Date'], format='%m/%d/%Y', errors='coerce')

    # Convert 'Percent_Complete' to numeric format (remove '%' and convert to float)
    df1['Payment_Total_Percent'] = df1['Payment_Total_Percent'].str.rstrip('%').astype(float)

    # Define the filter conditions
    condition1 = (df1['Payment_Date'].dt.year <= 2024) & (df1['Payment_Total_Percent'] >= 100)
    condition2 = (df1['Payment_Date'].dt.year <= 2023) & (df1['Payment_Total_Percent'] >= 95)
    condition3 = df1['Payment_Date'].dt.year <= 2021

    # Apply filtering: Keep only rows that satisfy any of the conditions
    filtered_df = df1[(condition1 | condition2 | condition3)]
    inactive_contracts = filtered_df['Contract_Number'].to_list()
    # Filter out inactive contracts for scraping
    contract_numbers_il_updated = [num for num in contract_numbers_il if num not in inactive_contracts]
    print(len(contract_numbers_il_updated))

    # Contract number to search for
    contract_numbers = contract_numbers_il_updated # Modify this to scrape all contracts in specific chunks as scraping all the contracts can crash the browser   
    # Start a new browser session
    driver.get(url)
    driver.maximize_window() 
    row_data_list_il = []
    header_data_il = ["percent_completed", "current_report","from_date", "to_date", "Contract_Awarded_Amount", "Additions",
                    "Deductions", "Total_Adjusted_Contract_Value", "Total_Amount_Due_to_Date", "Payment_Number", "total", "contract_number",
                    "state_job", "dot_vendor", "district/county", "il_project", "letting_date", "route", "airport/section", "section",
                    "project", "payee", "scope"]

    try:
        wait = WebDriverWait(driver, 100)
        for i,value in enumerate(contract_numbers):
            # Wait for the contract number input box to be present
            state_no_input = WebDriverWait(driver, 15).until(     
                EC.visibility_of_element_located((By.ID, 'ContractNumber')) 
            ) 
            state_no_input.send_keys(value)
            state_no_input.send_keys(Keys.RETURN)
            try:
                try:
                    contract_number_link = WebDriverWait(driver, 7).until(
                        EC.presence_of_element_located((By.LINK_TEXT, value))
                    )
                    time.sleep(3) # possibly resolves staleelement error
                    contract_number_link.click()
                except StaleElementReferenceException:
                    print(value,"StaleElementReferenceException error. Rescrape")
                    continue
                contract_info = []
                contractor_info = WebDriverWait(driver,10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="EstimateChoiceContractorInfo"]'))
                )
                time.sleep(5)
                all_invoices = WebDriverWait(driver,15).until(
                    EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[1]/div[2]/div[1]/div[3]/div/div/div[1]/ul/li/span[@class='k-link']"))
                )
                
                data = contractor_info.find_elements(By.XPATH, './/div[label]')

                if len(data) != 12:
                    print("possible data column mismatch at following contract and iteration", value,i)

                for info in data:
                    cell = info.find_element(By.XPATH, "./div")
                    contract_info.append(cell.get_attribute("textContent").strip())
        
                # iterate through all transactions
                report_counter = 0
                try:
                    for j, invoice in enumerate(all_invoices):
                        if report_counter > 2: # fetch latest 3 reports only since historical data has been scraped already
                            break
                        print("current value, iteration, and invoice", value, i, j)
                        if j == 0: # This condition resolves ElementClickInterceptedException error which can occur during first iteration
                            WebDriverWait(driver, 25).until(
                            EC.visibility_of_element_located((By.XPATH, './/div[@ID="TabStrip"]'))
                            )
                            current_id = "TabStrip-" + str(j+1)
                            container = wait.until(
                                EC.visibility_of_element_located((By.ID, current_id)) # modify this to remove sleep(add class container)
                            )
                        invoice.click()
                        time.sleep(1)   
                        # Wait for content to load
                        if j > 0:
                            wait.until(
                                EC.visibility_of_element_located((By.XPATH, './/div[@ID="TabStrip"]')) # modify this to remove sleep(add class container)
                            )
                            current_id = "TabStrip-" + str(j+1)
                            container = wait.until(
                                EC.visibility_of_element_located((By.ID, current_id)) # modify this to remove sleep(add class container)
                            )
                        time.sleep(1)

                        # Retrieve dates and percent complete information
                        current_data = []
                        payment_data1 = container.find_element(By.XPATH, './/div[@class="container mt-3"]')
                        for cell in payment_data1.find_elements(By.XPATH,'.//div[@class="form-group"]/div'):
                            current_data.append(cell.get_attribute("textContent").strip())

                        # Retrieve additional contract payment info such as 'contract awarded amount', 'additions', 'Total Adjusted Contract Value' and more
                        payment_data2 = container.find_element(By.XPATH, './div[3]')
                        payment_data2 = payment_data2.find_element(By.XPATH, './/table[1]/tbody/tr')
                        for cell in payment_data2.find_elements(By.XPATH, './td'):
                            current_data.append(cell.get_attribute("textContent").strip())

                        # Retrieve Total Cost and payment number of corresponding invoice
                        payment_number = container.find_element(By.XPATH, "./div[3]//div[@class='form-group'][last()]/div[@class='form-group row']/div[2]")
                        Total = container.find_element(By.XPATH, "./div[3]//div[@class='form-group'][last()]/div[@class='form-group row']/div[last()]")
                        current_data.append(payment_number.text)
                        current_data.append(Total.text)
                        combined_data = current_data + contract_info
                        row_data_list_il.extend([combined_data])      
                        report_counter += 1

                except ElementClickInterceptedException:
                    print("ElementClickInterceptedException error, rescrape", value)
                except TimeoutException:        
                    print(value,"Invoice loading timed out")
                except StaleElementReferenceException:
                    print(value,"Loading Error")

            except TimeoutException:        
                print(value,"Not Found")
                    
            driver.get(url)

    # Close the browser
    finally:
        driver.quit()
    il_dot_data = pd.DataFrame(data=row_data_list_il, columns = header_data_il)
    return il_dot_data

def transform_and_load_il(il_dot_data: pd.DataFrame) -> pd.DataFrame:
    #Remove unnecessary columns
    del il_dot_data['section']
    del il_dot_data['il_project']

    #DuckDB Integration
    # POST PROCESSING
    df = il_dot_data.copy()
    df = df[df['Payment_Number'].str.isnumeric()]
    df['Payment_Number'] = df['Payment_Number'].astype(int)
    # Split "district/county" into 'Project_Location_District' and 'Project_Location_County'
    df[['Project_Location_District', 'Project_Location_County']] = df['district/county'].str.extract(r'([\w\s]+?)\s*-\s*\d{3}\s*\(\s*([\w\s.]+?)\s*\)')
    del df['district/county']

    # Create payment work period column from "from_date" and "to_date"
    df["Payment_Work_Period"] = df["from_date"].str.cat(df["to_date"], sep=" to ", na_rep="Unknown")
    del df['from_date']
    del df['to_date']

    df.rename(columns = {'project':'Project_Number','scope':'Project_Description','route':'Project_Location_Route','airport/section':'Project_Location_Section',
                        'state_job':'Project_Location_StateJob','letting_date':'Project_Letting','contract_number':'Contract_Number','dot_vendor':'Contractor_DOT',
                        'total':'Payment_Amount','current_report':'Payment_Date','payee':'Contractor_Name','percent_completed':'Payment_Total_Percent',
                        'Total_Amount_Due_to_Date':'Payment_Amount_Total','Contract_Awarded_Amount':'Project_Cost_Total','Additions':'Project_Cost_Additions',
                        'Deductions':'Project_Cost_Deductions','Total_Adjusted_Contract_Value':'Project_Cost_Total_Adjusted'}, inplace=True)
    def parse_money(value):
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value) if value.strip() else None 

    # Convert money columns into calculable columns
    df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
    df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
    df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)
    df['Project_Cost_Additions'] = df['Project_Cost_Additions'].apply(parse_money)
    df['Project_Cost_Deductions'] = df['Project_Cost_Deductions'].apply(parse_money)
    df['Project_Cost_Total_Adjusted'] = df['Project_Cost_Total_Adjusted'].apply(parse_money)

    df['Payment_Balance'] = (df['Project_Cost_Total_Adjusted'] - df['Payment_Amount_Total']).round(2)

    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_path = os.getenv("DB_PATH")
    db_file = rf"{db_path}\Illinois\data_store_IL.duckdb"
    table_name = "IL_DOT"

    # Current scraped data
    scraped_data = df.copy()

    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Contract_Number  TEXT,
    Contractor_Name  TEXT,
    Contractor_DOT  TEXT,
    Project_Number  TEXT,
    Payment_Number  INTEGER,
    Project_Description	TEXT,
    Project_Location_Route TEXT,
    Project_Location_Section  TEXT,
    Project_Location_StateJob  TEXT,
    Project_Location_District  TEXT, 
    Project_Location_County  TEXT,
    Project_Letting  TEXT,
    Payment_Date  TEXT,
    Payment_Work_Period  TEXT,

    Payment_Amount DOUBLE,	
    Project_Cost_Total  DOUBLE,	
    Project_Cost_Additions  DOUBLE,
    Project_Cost_Deductions  DOUBLE,
    Project_Cost_Total_Adjusted  DOUBLE,
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
        # find duplicates by all columns except the columns below since they are calculated later and those columns that need not be tracked for changes if occur separately.
        # eg: for a given contract and payment number, if only "Contractor_DOT" is changed while other fields are the same we don't need to capture that unless required by Census.
        # Later, a logic can be developed to find revised payments if any
        combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial', 'Payment_Amount_Percent','Contractor_DOT','Project_Location_District','Project_Location_County','Payment_Balance'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Number","Pull_Date_Initial"], ascending=[True,False,False])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]

    else:
        combined_data = scraped_data.copy()
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Number","Pull_Date_Initial"], ascending=[True,False,False])
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
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

    print("Illinois scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'Illinois scraping completed and DUCKDB file updated Successfully.')

    return combined_data

def data_appended_il(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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