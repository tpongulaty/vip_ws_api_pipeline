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
from datetime import datetime
import pytz
import time
import os

from webdriver_manager.chrome import ChromeDriverManager


def scrape_raw_wv() -> pd.DataFrame:
    """
    Scrape the WV DOT contracts table and return the raw DataFrame.
    """
    # Automatic driver installer
    service = Service(ChromeDriverManager().install())
    url = 'https://www.wva.state.wv.us/wvdot/surety/'


    driver = webdriver.Chrome(service=service)
    driver.get(url)
    row_data_list_wv = []
    header_data_wv = []

    try:
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, 'ddlVendors'))
        )
        vendor_dropdown = driver.find_element(By.ID, 'ddlVendors')
        time.sleep(3)
        select = Select(vendor_dropdown)

        for i in range(len(select.options)):
            vendor_dropdown = driver.find_element(By.ID, 'ddlVendors')
            select = Select(vendor_dropdown)
            vendor_name = select.options[i].get_attribute(
                "textContent").strip()
            if i == 0:
                continue  # Skip the 'select vendor' option

            logging.info(f"Scraping for vendor: {vendor_name}")
            select.select_by_visible_text(vendor_name)

            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located(
                    (By.XPATH, '//table[@ID="gvContracts"]/tbody/tr'))
            )

            if i == 1 or not header_data_wv:
                table_header = driver.find_elements(
                    By.XPATH, '//table[@ID="gvContracts"]/tbody/tr/th')
                header_data_wv.extend([header.get_attribute('textContent').strip(
                ) for header in table_header if header.get_attribute('textContent').strip()])
                header_data_wv = ['Contract_Vendors'] + header_data_wv

            table_data = driver.find_elements(
                By.XPATH, '//table[@ID="gvContracts"]/tbody/tr')
            for j, row in enumerate(table_data):
                if j > 0:
                    cells = row.find_elements(By.XPATH, './td')
                    current_row_data = [vendor_name]
                    current_row_data.extend(
                        [cell.text for cell in cells if cell.text])
                    row_data_list_wv.append(current_row_data)

    except TimeoutException as e:
        print(f"Timeout Exception: {e}")
    finally:
        # Fechar o navegador
        driver.quit()

    # Create a Dataframe
    wv_dot_data = pd.DataFrame(data=row_data_list_wv, columns=header_data_wv)
    return wv_dot_data

def transform_and_load_wv(wv_dot_data: pd.DataFrame) -> pd.DataFrame:
    try:
        # POST PROCESSING
        df = wv_dot_data.copy()
        df.rename(columns = {'Federal/State Project Number':'Project_Number','Description':'Project_Description','Contract Amount':'Project_Cost_Total','Plan Completion Date':'Project_Comp_Est','Substantial Completion Date':'Project_Comp_Substantial',
                'Complete Date':'Project_Comp_Final', 'Release Date': 'Project_Close','Contract_Vendors':'Contractor_Name','Paid Amount':'Payment_Amount_Total','Percent Complete':'Payment_Total_Percent'}, inplace=True)
        def parse_money(value):
            # Remove dollar signs and commas
            value = value.replace('$', '').replace(',', '')
            # Convert values in parentheses to negative numbers
            if '(' in value and ')' in value:
                value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
            return float(value)
        df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
        df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
        df['Payment_Balance'] = df['Project_Cost_Total'] - df['Payment_Amount_Total']
        df['Project_Comp_Est'] = pd.to_datetime(df['Project_Comp_Est'],errors='coerce')
        df['Project_Comp_Substantial'] = pd.to_datetime(df['Project_Comp_Substantial'],errors='coerce')
        df['Project_Comp_Final'] = pd.to_datetime(df['Project_Comp_Final'],errors='coerce')
        df['Project_Close'] = pd.to_datetime(df['Project_Close'],errors='coerce')

        
        df['Project_Comp_Est'] = df['Project_Comp_Est'].dt.strftime('%m/%d/%Y')
        df['Project_Comp_Substantial'] = df['Project_Comp_Substantial'].dt.strftime('%m/%d/%Y')
        df['Project_Comp_Final'] = df['Project_Comp_Final'].dt.strftime('%m/%d/%Y')
        df['Project_Close'] = df['Project_Close'].dt.strftime('%m/%d/%Y')
        
        EST = pytz.timezone('US/Eastern')
        now = datetime.now(EST)
        current_date = now.strftime("%m/%d/%Y")
        df["Pull_Date_Initial"] = current_date

        # DUCKDB INTEGRATION
        # File to store DuckDB data
        db_file = r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\West_virginia\data_store_WV.duckdb"
        table_name = "WV_DOT"
        
        # Current scraped data
        scraped_data = df

        # Connect to DuckDB
        con = duckdb.connect(db_file)

        # Create table if not exists
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
        Contractor_Name  TEXT,
        Project_Number  TEXT,
        Payment_Number  INTEGER,
        Project_Description	TEXT,
        Project_Comp_Est  TEXT,
        Project_Comp_Substantial  TEXT,	
        Project_Comp_Final  TEXT,	
        Project_Close  TEXT,
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
            combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial', 'Previous_Payment_Amount_Total', 'Payment_Number', 'Payment_Amount_Percent', 'Payment_Amount'])].columns,keep="first") 
            # Post processing before loading into duckdb
            combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
            combined_data = combined_data.sort_values(by=["Project_Number", "Pull_Date_Initial"], ascending=[True,False])
            # Group by Project_Number and calculate the previous Payment_Amount_Total
            combined_data['Previous_Payment_Amount_Total'] = combined_data.groupby("Project_Number")["Payment_Amount_Total"].shift(-1)
            # Assign a reversed payment estimate number
            combined_data["Payment_Number"] = combined_data.groupby("Project_Number").cumcount(ascending=False)
            # Calculate the Current Amount Paid
            combined_data['Payment_Amount'] = combined_data['Payment_Amount_Total'] - combined_data['Previous_Payment_Amount_Total']
            # Fill NaN for the first record in each group (no previous record exists)
            combined_data['Payment_Amount'] = combined_data['Payment_Amount'].fillna(combined_data['Payment_Amount_Total'])
            # Revert the formatting of pull_date_initial column
            combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
            # Calculate Payment Amount Percent
            combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
            table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
            correct_order = table_info['column_name'].tolist()
            # Reorder the DataFrame to avoid conversion errors
            combined_data = combined_data[correct_order]

        else:
            combined_data = scraped_data
            # Post processing before loading into duckdb
            combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
            combined_data = combined_data.sort_values(by=["Project_Number", "Pull_Date_Initial"], ascending=[True,False])
            # Group by Project_Number and calculate the previous Payment_Amount_Total
            combined_data['Previous_Payment_Amount_Total'] = combined_data.groupby("Project_Number")["Payment_Amount_Total"].shift(-1)
            # Assign a reversed payment estimate number
            combined_data["Payment_Number"] = combined_data.groupby("Project_Number").cumcount(ascending=False)
            # Calculate the Current Amount Paid
            combined_data['Payment_Amount'] = combined_data['Payment_Amount_Total'] - combined_data['Previous_Payment_Amount_Total']
            # Fill NaN for the first record in each group (no previous record exists)
            combined_data['Payment_Amount'] = combined_data['Payment_Amount'].fillna(combined_data['Payment_Amount_Total'])
            # Revert the formatting of pull_date_initial column
            combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
            # Calculate Payment Amount Percent
            combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
            table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
            correct_order = table_info['column_name'].tolist()
            # Reorder the DataFrame to avoid conversion errors
            combined_data = combined_data[correct_order]

        # Replace the table with the updated data
        con.execute(f"DELETE FROM {table_name}")
        con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

        # Close connection
        con.close()
        print("West Virginia scraping completed and DUCKDB file updated Successfully.")
        logging.info(
            'West Virginia scraping completed and DUCKDB file updated Successfully.')
        return combined_data

    except Exception as e:
        print(f"Error in west_virginia_scraping: {e}")

def data_appended_wv(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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

# def()scrape all the current data- asset
# deduplicate def () -> asset -> partitioned data by pull date