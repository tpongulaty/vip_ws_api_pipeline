import tabula
import pyautogui
import duckdb
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
import time
from datetime import datetime
import pytz
import os

from webdriver_manager.chrome import ChromeDriverManager

def scrape_raw_wa() -> pd.DataFrame:
    # Type the desired file name and save
    pdf_file_path = os.getenv("PDF_FILE_PATH_WA")
    
    # Automatic driver installer
    service = Service(ChromeDriverManager().install())
    # Set up the WebDriver
    options = webdriver.ChromeOptions() 
    options.add_argument("--start-maximized") # Ensures the browser is fullscreen
    # URL of the site
    url = 'https://www.wsdot.wa.gov/publications/fulltext/construction/projectreports/Active.pdf-en-us.pdf'
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)
    # original_window = driver.window_handles[0]
    try:
            time.sleep(3)
            # Simulate pressing Ctrl+S to open "Save As" dialog
            pyautogui.hotkey('ctrl', 's')
            time.sleep(2)
            # Update with your desired location and name
            pyautogui.typewrite(pdf_file_path)
            time.sleep(1)  # Small delay for typing

            # Press 'Enter' to save
            pyautogui.press('enter')

            time.sleep(2)
            pyautogui.press('left')  # Move focus to the "Yes" button
            time.sleep(1)  # Small delay for smooth navigation
            pyautogui.press('enter')

            # Wait for the file to be saved
            time.sleep(5)
    finally:
        # Close the browser
        driver.quit()

    # pdf reader

    tables = tabula.read_pdf(pdf_file_path, pages='all', multiple_tables=True)
    df = pd.concat(tables, ignore_index=True)
    df['Contract\rNumber'] = df['Contract\rNumber'].astype(str).apply(lambda x: x.split('.')[0].zfill(6)) # makes sure leading zeros are retained by fixing the length of ID to 6. In the website it is mentioned that the contract numbers are 6 in length
    contract_numbers_wa = df['Contract\rNumber'].to_list() # 'split('.')[0]' is used above as the pdf reader adds decimals to numbers in some cases
    # Automatic driver installer
    service = Service(ChromeDriverManager().install())

    # URL of the site
    url = 'https://remoteapps.wsdot.wa.gov/construction/project/progress/'

    # Contract number to search for
    contract_numbers = contract_numbers_wa          
    header_data_wa = []
    # Start a new browser session
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    driver.maximize_window()
    row_data_list_wa = []
    try:
        for i,value in enumerate(contract_numbers):
            # Wait for the contract number input box to be present
        
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "txtContractNumber"))
            )

            # Find the contract number input box and enter the contract number
            contract_number_input = driver.find_element(By.ID, "txtContractNumber")
            contract_number_input.clear()
            contract_number_input.send_keys(value)
            

            # Submit the search form
            contract_number_input.send_keys(Keys.RETURN)
            # Locate the dropdown element for payment dates
            try:
                payment_date_options = WebDriverWait(driver, 100).until(
                            EC.visibility_of_all_elements_located((By.XPATH, '//*[@id="select-payment-date"]/option'))
                        )
                report_counter = 0
                # iterate over the payment date options:
                for option in payment_date_options:
                    if report_counter > 4: # fetch last 4 reports only since historical data has been scraped already
                        break
                    option.click()
                    try:
                        WebDriverWait(driver, 300).until(
                            EC.presence_of_element_located((By.ID, 'Page1'))
                        )
                        table1 = WebDriverWait(driver, 300).until(
                            EC.visibility_of_element_located((By.XPATH, '//table[@class="S45"]'))
                        )
                        # Find all headers from 2 tables and append them
                        if not header_data_wa:
                            # Find all cells in the row
                            for row in table1.find_elements(By.XPATH,'.//tr'):    
                                cells = row.find_elements(By.XPATH,'.//td')    
                                # Extract headers only from table 1    
                                header_data_wa.extend([cells[0].get_attribute("textContent").strip()]) 
                            del header_data_wa[2]               
                            # Extract header only from table 2    
                            header = driver.find_element(By.XPATH,'//table[@id="List1"]/tbody/tr[last()]/td')
                            header_data_wa.extend([header.get_attribute("textContent").strip()])

                        # Extract row data from table 1 and 2
                        row_data_list = []
                        for row in table1.find_elements(By.XPATH,'.//tr'):    
                            cells = row.find_elements(By.XPATH,'.//td')   
                            # Extract records only from table 1 
                            if len(cells) >=2:          
                                values = cells[1].get_attribute("textContent")
                                if not values:
                                    values = "N/A"  # Use a default value
                            if any(values):
                                row_data_list.append(values)
                            
                        del row_data_list[2] # Deletes the empty element extracted  due to extra tag containing just space in html structure
                        # Extract total from table 2
                        row_data = driver.find_element(By.XPATH,'//table[@id="List1"]/tbody/tr[last()]/td[last()]')
                        row_data_list.append(row_data.get_attribute("textContent"))
                        row_data_list_wa.extend([row_data_list])
                        report_counter += 1
            
                    except NoSuchElementException:
                        print("No Data for", value)    
                    except TimeoutException:
                        print(value,"some payment estimate data Not Found")
                    except Exception as e:
                        print(f"Error processing payment estimate: {e}")
            except TimeoutException:
                        print("Not Found for", value)
            
            # delay_seconds = random.uniform(2,10)
            # time.sleep(delay_seconds)
            driver.get(url)
    except TimeoutException:
        print("Website not responding. Re-run")
    finally:
        # Close the browser
        driver.quit()
        
    wa_dot_data = pd.DataFrame(data=row_data_list_wa, columns = header_data_wa)
    return wa_dot_data

def transform_and_load_wa(wa_dot_data: pd.DataFrame) -> pd.DataFrame:
    # POST PROCESSING
    df = wa_dot_data.copy()

    # Split "Contract Number:" into 'Contract_Number' and 'Project_Description'
    split_columns = df['Contract Number:'].str.split(' ', n=1, expand=True)
    split_columns.columns = ['Contract_Number', 'Project_Description']
    df = pd.concat([df, split_columns], axis = 1)
    del df['Contract Number:']


    df.rename(columns = {'Prime Contractor:':'Contractor_Name','Project Engineer:':'Engineer_Name','Estimate Number(s):': 'Estimate_Number_Original',
                        'Payment Date:':'Payment_Date','Overall - Total':'Payment_Amount','Last Date of Work This Payment:':'Last_Date_of_Work_This_Payment'}, inplace=True)

    # Create payment work period column from prior last date of work this payment to payment date
    df["Payment_Work_Period"] = df["Last_Date_of_Work_This_Payment"].str.cat(df["Payment_Date"], sep=" to ", na_rep="Unknown")

    def parse_money(value):
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value)

    # format money columns to numeric for calculations ahead
    df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)

    # Create scraping pull date column
    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_file = r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\Washington\data_store_WA.duckdb"
    table_name = "WA_DOT"

    # Current scraped data
    scraped_data = df

    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Contract_Number  TEXT,
    Contractor_Name  TEXT,
    Engineer_Name  TEXT,
    Payment_Number  INT,
    Estimate_Number_Original  TEXT,
    Project_Description	TEXT,
    Payment_Date  TEXT,
    Last_Date_of_Work_This_Payment  TEXT,
    Payment_Work_Period  TEXT,
    Payment_Amount DOUBLE,	
    Payment_Amount_Total  DOUBLE,
    Project_Cost_Total	DOUBLE,
    Payment_Balance DOUBLE,
    Pull_Date_Initial TEXT,
    Payment_Amount_Percent FLOAT,
    Payment_Total_Percent  FLOAT
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM {table_name}").df()

    # Deduplicate and merge
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
        combined_data['Estimate_Number_Original'] = combined_data['Estimate_Number_Original'].str.strip()
        # find duplicates by all columns except the columns below. Compared to other DOT'S WA requires different dedup columns since the 'payment amount total' is calculated post scraping 
        # and if minor changes such as 'engineer name' or 'project description' occur individually that will not be captured as it would lead to erroneous payment amount total. However if these changes happen in tandem with 
        # any one of the columns identified for deduplication ('Contract_Number','Estimate_Number_Original','Payment_Amount','Payment_Date') then those changes will be captured
        combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Project_Cost_Total','Payment_Balance','Pull_Date_Initial','Project_Description',
                                                                                        'Payment_Amount_Percent','Payment_Number','Payment_Total_Percent','Contractor_Name',
                                                                                        'Payment_Amount_Total', 'Engineer_Name','Payment_Work_Period','Last_Date_of_Work_This_Payment'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data['Payment_Date'] = pd.to_datetime(combined_data['Payment_Date'], errors="coerce")
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        # Sort the DataFrame
        combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Date","Pull_Date_Initial"], ascending=[True,True,False],na_position='first')
        # Assign a payment estimate number
        combined_data["Payment_Number"] = combined_data.groupby("Contract_Number").cumcount(ascending=True) 
        # Calculate the Current Payment Amount Total
        combined_data['Payment_Amount_Total'] = combined_data[combined_data['Payment_Amount'] >= 0].groupby("Contract_Number")['Payment_Amount'].cumsum()
        # Revert the formatting of the date columns
        combined_data["Payment_Date"] = combined_data["Payment_Date"].dt.strftime('%m/%d/%Y')
        combined_data["Payment_Date"] = combined_data["Payment_Date"].fillna("N/A")
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Calculate Project_Cost_Total by populating the the most recent Payment_Amount_Total. Note that this approach doesn't yield the 'True' Project value.
        combined_data['Project_Cost_Total'] = combined_data.groupby('Contract_Number')['Payment_Amount_Total'].transform('max')
        # Calculate Payment_Balance
        combined_data['Payment_Balance'] = combined_data['Project_Cost_Total'] - combined_data['Payment_Amount_Total']
        # Calculate Payment_Total_Percent
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conversion errors
        combined_data = combined_data[correct_order]    

    else:
        combined_data = scraped_data.copy()
        # Post processing before loading into duckdb
        combined_data['Estimate_Number_Original'] = combined_data['Estimate_Number_Original'].str.strip()
        combined_data['Payment_Date'] = pd.to_datetime(combined_data['Payment_Date'], errors="coerce")
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Payment_Date","Pull_Date_Initial"], ascending=[True,True,False],na_position='first')
        # Assign a payment estimate number
        combined_data["Payment_Number"] = combined_data.groupby("Contract_Number").cumcount(ascending=True)
        # Calculate the Current Payment Amount Total
        combined_data['Payment_Amount_Total'] = combined_data[combined_data['Payment_Amount'] >= 0].groupby("Contract_Number")['Payment_Amount'].cumsum()
        # Revert the formatting of the date columns
        combined_data["Payment_Date"] = combined_data["Payment_Date"].dt.strftime('%m/%d/%Y')
        combined_data["Payment_Date"] = combined_data["Payment_Date"].fillna("N/A")
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        # Calculate Payment Amount Percent
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Calculate Project_Cost_Total by populating the the most recent Payment_Amount_Total. Note that this approach doesn't yield the 'True' Project value.
        combined_data['Project_Cost_Total'] = combined_data.groupby('Contract_Number')['Payment_Amount_Total'].transform('max')
        # Calculate Payment_Balance
        combined_data['Payment_Balance'] = combined_data['Project_Cost_Total'] - combined_data['Payment_Amount_Total']
        # Calculate Payment_Total_Percent
        combined_data['Payment_Total_Percent'] = (combined_data['Payment_Amount_Total']/combined_data['Project_Cost_Total'] * 100).round(2)
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
    print("Washington scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'Washington scraping completed and DUCKDB file updated Successfully.')
    return combined_data

def data_appended_wa(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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