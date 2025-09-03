import duckdb
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime
import pytz
import time
import os
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv  
# Load environment variables from .env file
load_dotenv()

def scrape_raw_ga() -> pd.DataFrame:
    # Automatic driver installer
    service = Service(ChromeDriverManager().install())

    # URL of the site
    url = 'https://www.dot.ga.gov/GDOT/pages/contractors.aspx'

    # Set Chrome options to disable alerts
    chrome_options = Options() 
    chrome_options.add_argument("--disable-popup-blocking") 
    chrome_options.add_argument("--disable-notifications") 
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-web-security") 
    chrome_options.add_argument("--ignore-certificate-errors")   
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.get(url)
    driver.set_window_size(1920, 1080)  # adjust window size to avoid elements from overlapping
    # Contract number to search for
    # contract_numbers = list_ga[0:1000] # try with smaller list and clean '#' 
    contract_numbers = ['B10007-97-M00-1','APER40-09-127-0'] #contract_id
    # contract_numbers = ['0000184.E3000', '0000301', '0000304']  #project_number
    # contract_numbers = ['0000078','0000088','0000083']  #PI_number   
    primary_counties = ['ALL COUNTIES']  # when value = all counties it is equivalent to the bulk option by primary counties
    vendor_names = ['BAKER INFRASTRUCTURE GROUP, INC.']      
    header_data_ga = ["contract_id", "description", "source", "pi_numbers", "last_voucher_processed", "project_numbers", "current_contract_amount", "voucher/est_num", "voucher/est_date", "payment_percent_complete","date_approved","net_payment","date_received","balance",
                    "date_processed","material_allowance","date_sent_to_accounting", "total_payments", "estimate_link"]
    row_data_list_ga = []
    # Create a WebDriver instance with Chrome options
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get(url)
    original_window = driver.current_window_handle
    pi_number = False
    contract_id = False
    project_number = False
    primary_county = True
    vendor_name = False
    vendor_name_bulk = False # this option may trigger dos attack and lock the account. do not use yet
    try: #unexpected alerts need to be handled
        
        # Wait for psi_report to be present
        WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, '//button[@class="nb-expando__toggle js-expando__toggle"]'))
        )
        psi_report = driver.find_elements(By.XPATH, '//button[@class="nb-expando__toggle js-expando__toggle"]/div[@class="nb-expando__toggle-text"]')
        for j, row in enumerate(psi_report):
            if j == 4:
                row.click()
        WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.LINK_TEXT, "PSI"))
        )
        # Navigate to payment dashboard
        psi_link = driver.find_element(By.LINK_TEXT, "PSI")
        psi_link.click()
        # change the window of the driver since new tab is opened
        for window_handle in driver.window_handles:
            if window_handle != original_window:
                driver.switch_to.window(window_handle)
                break
        # Navigate to payment window dashboard
        payment_info = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="dashboard_page_2_tab"]/tbody/tr/td/div'))
        )
        payment_info.click()

        # Retrive list of all primary counties
        primary_county_dropdown = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
            )
        primary_county_dropdown.click()
        primary_county_labels = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '/html/body/div[9]/div/div[2]/div/div/label'))
            ) 
        primary_county_list = [name.get_attribute('textContent').strip() for name in primary_county_labels if name.get_attribute('textContent').strip()]
        
        del primary_county_list[0]
        primary_county_list = primary_county_list
        primary_county_dropdown.click() # disengage the dropdown
        # list to store individual dataframes
        dataframes_payment = []
        time.sleep(5)
        def remove_selection():
            remove_all_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="idShuttleContainerBody"]/tr[3]/td[2]/table/tbody/tr[5]/td'))
                ) 
            remove_all_button.click()

        def check_conditions():
            if contract_id:
                # Find the contract ID dropdown
                # Note: shortcut XPATH doesn't seem to be working because of the HTML structure. So full path is taken
                contract_id_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, "/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[4]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img"))
                )
                contract_id_dropdown.click()
            if pi_number:
                pi_num_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[3]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                pi_num_dropdown.click()
                deselect_default = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[9]/div/div[2]/div[1]/div/label'))
                ) 
                deselect_default.click() 
            if project_number:
                project_num_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[5]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                project_num_dropdown.click()
            if primary_county:
                primary_county_dropdown = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                    )
                primary_county_dropdown.click()
            if vendor_name or vendor_name_bulk:
                vendor_name_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                vendor_name_dropdown.click()
        def payment_info_selected():
            if vendor_name_bulk:
                move_all_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/table/tbody[1]/tr/td/div[2]/div/table/tbody/tr[3]/td[2]/table/tbody/tr[2]/td/img'))
                )
                for i in range(10):
                    move_all_button.click()

            ok_button_2 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/table/tbody[1]/tr/td/div[3]/a[1]'))
            )
            ok_button_2.click()
            # Deselect "default project" value of 'PI Number' if not already deselected (that is in the case where the search is not by pi_number or primary county or vendor_name)
            # Note: Again in this case shortcut XPATH isn't working so full path is used
            if not pi_number and not primary_county and not vendor_name and not vendor_name_bulk:
                ### This logic needs to be edited to account for when contract ID is true so that "deselect_default" is not triggered more than once (recommended : create a separate def for this logic)    
                pi_num_dropdown = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[3]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                pi_num_dropdown.click()
                deselect_default = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/div[2]/div[1]/div/label'))
                ) 
                deselect_default.click()
            # Apply the filters
            time.sleep(10)
            apply = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="gobtn"]'))
            )

            apply.click()
            time.sleep(10)
            try:
                #switch the driver to iframe ID
                iframe_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="d:dashboard~p:9297np0cm5nna3pq~x:m7no0s394ckllasi"]'))
                )
                driver.switch_to.frame(iframe_element)
                # format the report to html for interaction
                format = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/table/tbody/tr/td/div/div/div/div/div[2]/div[1]/div/div[2]/a[1]/img[2]'))
                )
                format.click()
                html = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '//*[@id="_xdoFMenu1"]/div/div/ul/li[1]/div/a/div[2]'))
                )
                html.click()
                #switch the driver to the next iframe ID
                time.sleep(15)
                iframe_element_html = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="xdo:docframe0"]'))
                )
                driver.switch_to.frame(iframe_element_html)

                # iterate and extract relevent data for all 
                # driver.switch_to.default_content()
                tables = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.XPATH, '/html/body/table[@class="c29"]'))
                )

                for i, row in enumerate(tables):
                    current_data = []
                    cells = row.find_elements(By.XPATH,'./tbody/tr/td/p/span')
                    siblings = row.find_elements(By.XPATH, './following-sibling::*')
                    cost_tables = []
                    counter = 0
                    for value in cells: 
                        if counter % 2 != 0:
                            current_data.append(value.text)
                        counter +=1
                    if len(current_data) != 7:
                        print("check data of type1 table, iteration", i)
                    for sibling in siblings:
                        if not project_number and "c39" in sibling.get_attribute("class"):
                            cost_tables.append(sibling)
                        elif project_number and "c41" in sibling.get_attribute("class"):
                            cost_tables.append(sibling)
                        elif "c29" in sibling.get_attribute("class"):
                            break
                    for j, cost_table in enumerate(cost_tables):
                        current_cost = []
                        if not project_number:
                            data_table = cost_table.find_element(By.XPATH,'./tbody/tr/td/table[@class="c37"]')
                            data = data_table.find_elements(By.XPATH,'./tbody/tr/td')
                        elif project_number:
                            data_table = cost_table.find_element(By.XPATH,'./tbody/tr/td/table[@class="c39"]')
                            data = data_table.find_elements(By.XPATH,'./tbody/tr/td')
                        counter = 0
                        for cell in data:
                            link_element = cell.find_element(By.TAG_NAME, "a") if len(cell.find_elements(By.TAG_NAME, "a")) > 0 else None
                            if counter % 2 != 0:
                                if link_element:
                                    # Get the hyperlink (href attribute)
                                    link = link_element.get_attribute("href")
                                    current_cost.append(link)
                                else:
                                    current_cost.append(cell.text)
                            counter +=1
                        if len(current_cost) != 12:
                            print("check data of type2 table, with following iterations of type1 and type2", i, j)
                        current_cost_join = current_data + current_cost
                        row_data_list_ga.extend([current_cost_join])
                current_df = pd.DataFrame(data=row_data_list_ga, columns = header_data_ga)
                dataframes_payment.append(current_df)
                # switch the driver back to default html content
                driver.switch_to.default_content()
            except TimeoutException:
                print("Data not found for current input of contract numbers.")


        # Enter all contract_id's/vendor names/primary counties
        if not primary_county and not vendor_name and not vendor_name_bulk:
            for i in range(0,len(contract_numbers),100):
                check_conditions() # corresponding dropdown is engaged
                if i == 0:
                    search_more = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/div[9]/div/div[3]/span'))
                        )
                else:
                    search_more = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/div[3]/span')) # search button path changes slightly after default is deselected which happens automatically by the second iteration
                        )
                search_more.click()
                remove_selection() # removes selected filters from the previous iteration
                edit_button = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, '//*[@id="idShuttleContainerBody"]/tr[1]/th[5]/a/img'))
                    )
                edit_button.click() # opens a text box for entering required filters
                contract_id_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
                )
                value = contract_numbers[i:i + 100]
                value = "\n".join(value) # Create new lines for every value
                contract_id_input.send_keys(value)
                # county_input.send_keys(Keys.RETURN) # useful only if sending one value at a time that is to create a new line
                ok_button_1 = driver.find_element(By.XPATH, '/html/body/div[12]/div/table/tbody[1]/tr/td/div[3]/a[1]')
                ok_button_1.click() # Button to confirm values for filters after entering in the text box
                payment_info_selected() # Retrieves the data from the selected filters and stores it in a dataframe
            GA_DOT_payment_data = pd.concat(dataframes, ignore_index=True)
        elif primary_county:
            for i in range(0,len(primary_counties),1): # iterate through the list of primary counties that was intially fetched in steps of 20 (adjust as suitable)
                check_conditions() # corresponding dropdown is engaged
                # Navigate to Text area box for entering all the contract ID's, primary counties or any other filters
                if i == 0:
                    search_more = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/div[9]/div/div[3]/span'))
                        )
                else:
                    search_more = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/div[3]/span')) # search button path changes slightly after default is deselected which happens automatically by the second iteration
                        )
                search_more.click()
                remove_selection() # removes selected filters from the previous iteration
                edit_button = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, '//*[@id="idShuttleContainerBody"]/tr[1]/th[5]/a/img'))
                    )
                edit_button.click() # opens a text box for entering required filters
                county_input = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
                    )
                value = primary_counties[i:i + 1]
                value = "\n".join(value) # Create new lines for every value
                county_input.send_keys(value)
                # county_input.send_keys(Keys.RETURN) # useful only if sending one value at a time that is to create a new line
                ok_button_1 = driver.find_element(By.XPATH, '/html/body/div[12]/div/table/tbody[1]/tr/td/div[3]/a[1]')
                ok_button_1.click() # Button to confirm values for filters after entering in the text box
                payment_info_selected() # Retrieves the data from the selected filters and stores it in a dataframe
            GA_DOT_payment_data = pd.concat(dataframes_payment, ignore_index=True)

        elif vendor_name: # no need to develop this functionality now, unless required in future
            vendor_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
            )
            for value in vendor_names: 
                vendor_input.send_keys(value)
                vendor_input.send_keys(Keys.RETURN)
                payment_info_selected()

        # Fetch milestone dates information
        dataframes_milestone = []
        milestone_dates = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.XPATH, '//*[@id="dashboard_page_0_tab"]/tbody/tr/td/div'))
        )
        milestone_dates.click()
        def check_conditions1():
            if contract_id:
                # Find the contract ID dropdown
                # Note: shortcut XPATH doesn't seem to be working because of the HTML structure. So full path is taken
                contract_id_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, "/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[4]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img"))
                )
                contract_id_dropdown.click()
            if pi_number:
                pi_num_dropdown = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[3]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                pi_num_dropdown.click()
                deselect_default = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[9]/div/div[2]/div[1]/div/label'))
                ) 
                deselect_default.click() 
            if project_number:
                project_num_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[5]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                project_num_dropdown.click()
            if primary_county:
                primary_county_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[2]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                primary_county_dropdown.click()
            if vendor_name or vendor_name_bulk:
                vendor_name_dropdown = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[1]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                vendor_name_dropdown.click()
        def milestone_data_selected():
            if vendor_name_bulk:
                move_all_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/table/tbody[1]/tr/td/div[2]/div/table/tbody/tr[3]/td[2]/table/tbody/tr[2]/td/img'))
                )
                for i in range(10):
                    move_all_button.click()

            ok_button_2 = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/table/tbody[1]/tr/td/div[3]/a[1]'))
            )
            ok_button_2.click()
            # Deselect "default project" value of 'PI Number' if not already deselected (that is in the case where the search is not by pi_number or primary county or vendor_name)
            # Note: Again in this case shortcut XPATH isn't working so full path is used
            if not pi_number and not primary_county and not vendor_name and not vendor_name_bulk:    
                pi_num_dropdown = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[7]/table[1]/tbody/tr/td[2]/div/table[1]/tbody/tr/td[2]/div[1]/table/tbody/tr/td[1]/div/table/tbody/tr[2]/td/div/table/tbody/tr/td/div/table/tbody/tr/td/div/div/div/table/tbody/tr/td/div/form/div/table/tbody/tr[2]/td/table/tbody/tr/td[3]/table/tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/img'))
                )
                pi_num_dropdown.click()
                deselect_default = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/div[2]/div[1]/div/label'))
                ) 
                deselect_default.click()
            # Apply the filters
            time.sleep(10)
            apply = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="gobtn"]'))
            )

            apply.click()
            time.sleep(10)
            #switch the driver to iframe ID
            iframe_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="d:dashboard~p:16a6r3787gg7lutu~x:93gsu676msc1raim"]'))
            )
            driver.switch_to.frame(iframe_element)
            # format the report to html for interaction
            format = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '/html/body/table/tbody/tr/td/div/div/div/div/div[2]/div[1]/div/div[2]/a[1]/img[2]'))
            )
            format.click()
            html = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//*[@id="_xdoFMenu1"]/div/div/ul/li[1]/div/a/div[2]'))
            )
            html.click()
            #switch the driver to the next iframe ID
            time.sleep(10)
            iframe_element_next = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="xdo:docframe0"]'))
            )
            driver.switch_to.frame(iframe_element_next)

            # iterate and extract relevent data for all 
            # driver.switch_to.default_content()
            tables = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.XPATH, '/html/body/table[@class="c7"]'))
            )

            records = []

            # Iterate through the tables in groups of 3
            for i in range(0, len(tables), 3):
                group_tables = tables[i:i+3]
                if len(group_tables) < 3:
                    continue  # Skip incomplete groups

                # Extract the vendor name from the first table
                vendor_name = None
                try:
                    vendor_name = group_tables[0].find_element(By.CLASS_NAME, "c12").text.strip()
                except:
                    pass
                
                # Initialize a dictionary to store the combined data for this vendor
                record = {'Vendor Name': vendor_name}
                # Extract data from the second and third tables
                for data_table in group_tables[1:]:
                    rows = data_table.find_elements(By.TAG_NAME, "tr")
                
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        for idx in range(0, len(cells), 2):
                            try:
                                header = cells[idx].text.strip()
                                value = cells[idx + 1].text.strip() if idx + 1 < len(cells) else None
                                record[header] = value
                            except IndexError:
                                continue  # Skip incomplete rows

                # Add the record to the list
                records.extend([record])
            records_df = pd.DataFrame(records)
            dataframes_milestone.append(records_df) # Append the current iteration dataframe to the list "dataframes1"
            # switch the driver back to default html content
            driver.switch_to.default_content()
        
        # Enter all contract_id's/vendor names/primary counties
        if not primary_county and not vendor_name and not vendor_name_bulk:
            for i in range(0,len(contract_numbers),100):
                check_conditions1() # corresponding dropdown is engaged
                # Navigate to Text area box for entering all the contract ID's
                search_more = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div[10]/div/div[3]/span'))
                    )
                search_more.click()
                remove_selection()
                edit_button = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, '//*[@id="idShuttleContainerBody"]/tr[1]/th[5]/a/img'))
                    )
                edit_button.click()
                county_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
                )
                value = primary_counties[i:i + 100]
                value = "\n".join(value) # Create new lines for every value
                county_input.send_keys(value)
                ok_button_1 = driver.find_element(By.XPATH, '/html/body/div[12]/div/table/tbody[1]/tr/td/div[3]/a[1]')
                ok_button_1.click()
                milestone_data_selected()
            GA_DOT_milestone_data = pd.concat(dataframes_milestone, ignore_index=True)
        elif primary_county:
            for i in range(0,len(primary_counties),1): # iterate through the list of primary counties that was intially fetched in steps of 20
                check_conditions1() # corresponding dropdown is engaged
                # Navigate to Text area box for entering all the contract ID's
                search_more = WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '/html/body/div[9]/div/div[3]/span'))
                    )
                search_more.click()
                remove_selection()
                edit_button = WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, '//*[@id="idShuttleContainerBody"]/tr[1]/th[5]/a/img'))
                    )
                edit_button.click()
                county_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
                )
                value = primary_counties[i:i + 1]
                value = "\n".join(value) # Create new lines for every value
                county_input.send_keys(value)
                ok_button_1 = driver.find_element(By.XPATH, '/html/body/div[12]/div/table/tbody[1]/tr/td/div[3]/a[1]')
                ok_button_1.click()
                milestone_data_selected()
            GA_DOT_milestone_data = pd.concat(dataframes_milestone, ignore_index=True)
        elif vendor_name: # no need to develop this functionality now, unless required in future
            vendor_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="shuttleSelectEditTextArea"]'))
            )
            for value in vendor_names: 
                vendor_input.send_keys(value)
                vendor_input.send_keys(Keys.RETURN)

    
    finally:
        # Close the browser
        driver.quit()
    # join payment info data and milestone dates data
    ga_dot_data = pd.merge(GA_DOT_payment_data,GA_DOT_milestone_data, left_on="contract_id", right_on="ContractID:", how='inner')
    return ga_dot_data

def transform_and_load_ga(ga_dot_data: pd.DataFrame) -> pd.DataFrame:
    # POST PROCESSING
    df = ga_dot_data.copy()
    df.rename(columns = {'contract_id':'Contract_Number','project_numbers':'Project_Number','description':'Project_Description','Let Date:':'Project_Letting','current_contract_amount':'Project_Cost_Total','Original Contract Completion Date:':'Project_Comp_Est',
                        'Current Specified Completion Date:':'Project_Comp_Substantial','source': 'Contract_Source','voucher/est_num':'Payment_Number','net_payment':'Payment_Amount','voucher/est_date':'Payment_Date','total_payments':'Payment_Amount_Total',
                        'payment_percent_complete':'Payment_Total_Percent','date_approved':'Payment_Approved','date_received':'Payment_Received','date_processed':'Payment_Processed','date_sent_to_accounting':'Payment_toAccounting',
                        'material_allowance':'Material_Allowance','balance':'Payment_Balance','estimate_link':'Comments','Vendor Name':'Contractor_Name','Material Certificate:':'Material_Certificate',
                        'Punch List to Contractor:':'Project_PunchList_Sent','Punch List Complete:':'Project_PunchList_Complete','Closing Conference:':'Project_Closing_Conference','Time Charges Stopped:':'Project_Charges_Stopped',
                        'Contractor Sent Final Quantities:':'Project_FinalQuantities_Sent','Contractor Accepts Final Quantities:':'Project_FinalQuantities_Accept','Date Accepted':'Project_Date_Accepted'}, inplace=True)

    # Drop duplicate and irrelevant columns
    df.drop(['pi_numbers','last_voucher_processed','ContractID:','Description:','Project#(s):','PI Numbers(s):','Source:'], axis=1, inplace=True)

    def parse_money(value): 
        # Remove dollar signs and commas
        value = value.replace('$', '').replace(',', '')
        # Convert values in parentheses to negative numbers
        if '(' in value and ')' in value:
            value = '-' + value[1:-1]  # Remove the parentheses and add a negative sign
        return float(value)
    df['Project_Cost_Total'] = df['Project_Cost_Total'].apply(parse_money)
    df['Payment_Amount_Total'] = df['Payment_Amount_Total'].apply(parse_money)
    df['Payment_Balance'] = df['Payment_Balance'].apply(parse_money)
    df['Payment_Amount'] = df['Payment_Amount'].apply(parse_money)
    df['Material_Allowance'] = df['Material_Allowance'].apply(parse_money)

    # Convert all date columns to a standard format MM-DD-YYYY
    df['Project_Comp_Est'] = pd.to_datetime(df['Project_Comp_Est'],format='%b-%d-%Y',errors='coerce')
    df['Project_Comp_Substantial'] = pd.to_datetime(df['Project_Comp_Substantial'],format='%b-%d-%Y',errors='coerce')
    df['Payment_Date'] = pd.to_datetime(df['Payment_Date'],format='%b-%d-%Y',errors='coerce')
    df['Payment_Approved'] = pd.to_datetime(df['Payment_Approved'],format='%b-%d-%Y',errors='coerce')
    df['Payment_Received'] = pd.to_datetime(df['Payment_Received'],format='%b-%d-%Y',errors='coerce')
    df['Payment_Processed'] = pd.to_datetime(df['Payment_Processed'],format='%b-%d-%Y',errors='coerce')
    df['Payment_toAccounting'] = pd.to_datetime(df['Payment_toAccounting'],format='%b-%d-%Y',errors='coerce')
    df['Project_Letting'] = pd.to_datetime(df['Project_Letting'],format='%b-%d-%Y',errors='coerce')
    df['Project_PunchList_Sent'] = pd.to_datetime(df['Project_PunchList_Sent'],format='%b-%d-%Y',errors='coerce')
    df['Project_PunchList_Complete'] = pd.to_datetime(df['Project_PunchList_Complete'],format='%b-%d-%Y',errors='coerce')
    df['Project_Closing_Conference'] = pd.to_datetime(df['Project_Closing_Conference'],format='%b-%d-%Y',errors='coerce')
    df['Project_Charges_Stopped'] = pd.to_datetime(df['Project_Charges_Stopped'],format='%b-%d-%Y',errors='coerce')
    df['Project_FinalQuantities_Sent'] = pd.to_datetime(df['Project_FinalQuantities_Sent'],format='%b-%d-%Y',errors='coerce')
    df['Project_FinalQuantities_Accept'] = pd.to_datetime(df['Project_FinalQuantities_Accept'],format='%b-%d-%Y',errors='coerce')
    df['Project_Date_Accepted'] = pd.to_datetime(df['Project_Date_Accepted'],format='%b-%d-%Y',errors='coerce')
    df['Material_Certificate'] = pd.to_datetime(df['Material_Certificate'],format='%b-%d-%Y',errors='coerce')


    df['Project_Comp_Est'] = df['Project_Comp_Est'].dt.strftime('%m-%d-%Y')
    df['Project_Comp_Substantial'] = df['Project_Comp_Substantial'].dt.strftime('%m-%d-%Y')
    df['Payment_Date'] = df['Payment_Date'].dt.strftime('%m-%d-%Y')
    df['Payment_Approved'] = df['Payment_Approved'].dt.strftime('%m-%d-%Y')
    df['Payment_Received'] = df['Payment_Received'].dt.strftime('%m-%d-%Y')
    df['Payment_Processed'] = df['Payment_Processed'].dt.strftime('%m-%d-%Y')
    df['Payment_toAccounting'] = df['Payment_toAccounting'].dt.strftime('%m-%d-%Y')
    df['Project_Letting'] = df['Project_Letting'].dt.strftime('%m-%d-%Y')
    df['Project_PunchList_Sent'] = df['Project_PunchList_Sent'].dt.strftime('%m-%d-%Y')
    df['Project_PunchList_Complete'] = df['Project_PunchList_Complete'].dt.strftime('%m-%d-%Y')
    df['Project_Closing_Conference'] = df['Project_Closing_Conference'].dt.strftime('%m-%d-%Y')
    df['Project_Charges_Stopped'] = df['Project_Charges_Stopped'].dt.strftime('%m-%d-%Y')
    df['Project_FinalQuantities_Sent'] = df['Project_FinalQuantities_Sent'].dt.strftime('%m-%d-%Y')
    df['Project_FinalQuantities_Accept'] = df['Project_FinalQuantities_Accept'].dt.strftime('%m-%d-%Y')
    df['Project_Date_Accepted'] = df['Project_Date_Accepted'].dt.strftime('%m-%d-%Y')
    df['Material_Certificate'] = df['Material_Certificate'].dt.strftime('%m-%d-%Y')

    # convert "Payment_Number" column to integer type to avoid duplicate records when integrated with duckdb
    df['Payment_Number'] = df['Payment_Number'].astype(int)

    EST = pytz.timezone('US/Eastern')
    now = datetime.now(EST)
    current_date = now.strftime("%m/%d/%Y")
    df["Pull_Date_Initial"] = current_date

    # DUCKDB INTEGRATION
    # File to store DuckDB data
    db_path = os.getenv('DB_PATH')
    db_file = rf"{db_path}\Georgia\data_store_GA.duckdb"
    table_name = "GA_DOT"

    # Current scraped data
    scraped_data = df

    # Connect to DuckDB
    con = duckdb.connect(db_file)

    # Create table if not exists
    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    Contractor_Name  TEXT,
    Contract_Number  TEXT,
    Project_Number  TEXT,
    Contract_Source	 TEXT,
    Payment_Number  INTEGER,
    Project_Description	TEXT,
    Project_Comp_Est  TEXT,
    Project_Comp_Substantial  TEXT,	

    Payment_Amount DOUBLE,	
    Project_Cost_Total  DOUBLE,	
    Payment_Amount_Total  DOUBLE,
    Payment_Total_Percent  TEXT,
    Payment_Amount_Percent  FLOAT,
    Payment_Balance  DOUBLE,
    Material_Allowance DOUBLE,

    Pull_Date_Initial TEXT,
    Payment_Date  TEXT,
    Payment_Approved  TEXT,
    Payment_Received  TEXT,
    Payment_Processed  TEXT,
    Payment_toAccounting  TEXT,
    Comments  TEXT,

    Project_Letting TEXT,
    Material_Certificate  TEXT,	
    Project_PunchList_Sent  TEXT,
    Project_PunchList_Complete	TEXT,
    Project_Closing_Conference  TEXT,
    Project_Charges_Stopped  TEXT,
    Project_FinalQuantities_Sent  TEXT,
    Project_FinalQuantities_Accept TEXT,	
    Project_Date_Accepted TEXT
    )
    """)

    # Insert or Update Logic
    # Load existing data from DuckDB
    existing_data = con.execute(f"SELECT * FROM {table_name}").df()

    # Deduplicate and merge
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, scraped_data], ignore_index=True)
        # find duplicates by all columns except the columns below since they are calculated later. Later, a logic can be developed to find revised payments if any
        combined_data = combined_data.drop_duplicates(subset=df.loc[:, ~df.columns.isin(['Pull_Date_Initial','Payment_Amount_Percent'])].columns,keep="first") 
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Project_Number", "Pull_Date_Initial"], ascending=[True,False])
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conersion errors
        combined_data = combined_data[correct_order]

    else:
        combined_data = scraped_data
        # Post processing before loading into duckdb
        combined_data['Pull_Date_Initial'] = pd.to_datetime(combined_data['Pull_Date_Initial'])
        combined_data = combined_data.sort_values(by=["Contract_Number", "Pull_Date_Initial"], ascending=[True,False])
        combined_data['Payment_Amount_Percent'] = (combined_data['Payment_Amount']/combined_data['Payment_Amount_Total'] * 100).round(2)
        # Revert the formatting of pull_date_initial column
        combined_data["Pull_Date_Initial"] = combined_data["Pull_Date_Initial"].dt.strftime('%m/%d/%Y')
        table_info = con.execute(f"DESCRIBE {table_name}").fetchdf()
        correct_order = table_info['column_name'].tolist()
        # Reorder the DataFrame to avoid conersion errors
        combined_data = combined_data[correct_order]

    # Replace the table with the updated data
    con.execute(f"DELETE FROM {table_name}")
    con.execute(f"INSERT INTO {table_name} SELECT * FROM combined_data")

    # Close connection
    con.close()
    print("Georgia scraping completed and DUCKDB file updated Successfully.")
    logging.info(
        'Georgia scraping completed and DUCKDB file updated Successfully.')
    return combined_data
    
def data_appended_ga(combined_data: pd.DataFrame) -> pd.DataFrame: # Fetch the data appended in the current run
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