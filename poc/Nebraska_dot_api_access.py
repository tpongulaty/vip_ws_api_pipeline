import requests
import time
import base64
import hashlib
import pandas as pd
import os

# API prod connection
base_url = os.getenv("BASE_URL_NE")
IntegrationName = os.getenv("OAPI_Census_Contract")
secrectAccessKey = os.getenv("SECRET_ACCESS_KEY_NE")   
subscription_key = os.getenv("SUBSCRIPTION_KEY") # dedicated key for all open api tasks

cache_Control = "no-cache"
entities_to_fetch = ['Contracts', 'ContractTimes', 'ContractProjects', 'PaymentEstimates', 'RefVendors','RefPaymentEstimateTypes','ContractPaymentEstimateTypes']
entity_dataframes = {}

def get_server_time():
    thirty_sec_window = 30
    current_time = str(int(time.time() // thirty_sec_window))
    return current_time

# calculate TOTP using PBKDF2 with HMAC-SHA1
def calculate_totp(secret, current_time):
    iteration_count = 1000
    # calculate TOTP using PBKDF2 with HMAC-SHA1
    key = hashlib.pbkdf2_hmac('sha1', secret.encode(), current_time.encode(), iteration_count)
    totp = base64.b64encode(key).decode()
    return totp

def create_auth_header(integration_name, totp, username=None):
    if username:
        auth_string = f'{integration_name};{username}:{totp}'
    else:
        auth_string = f'{integration_name}:{totp}'
    auth_base64 = base64.b64encode(auth_string.encode()).decode()
    return f'Basic {auth_base64}'

def get_entity_data(auth_header, subscription_key, entity,skip=0):
    headers = {
        'Authorization': auth_header,
        'Cache_Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': subscription_key
    }
    params = {'$skip': skip}
    response = requests.get(f'{base_url}/{entity}', headers=headers, params=params)
    response.raise_for_status()
    return response.json()

server_time = get_server_time()
totp = calculate_totp(secrectAccessKey, server_time)
auth_header = create_auth_header(IntegrationName, totp)
for entity in entities_to_fetch:
    skip = 0
    all_records = []

    while True:
        server_time = get_server_time()
        totp = calculate_totp(secrectAccessKey, server_time)
        auth_header = create_auth_header(IntegrationName, totp)
        entity_data = get_entity_data(auth_header, subscription_key, entity, skip=skip)
        records = entity_data['value']
        if not records:
            break
        all_records.extend(records)
        skip += len(records)
        print(f"Fetched {len(records)} records for {entity}, total fetched so far: {len(all_records)}")

    # Convert all records to a DataFrame
    if all_records:
        entity_dataframes[entity] = pd.DataFrame(all_records)
        print(f"Total records for {entity}: {len(entity_dataframes[entity])}")
    else:
        print(f"No records found for {entity}")

# Data Transformation
# copy dataframes from the dictionary
df_contracts = entity_dataframes['Contracts'].copy()
df_contractTimes = entity_dataframes['ContractTimes'].copy()
df_contractProjects = entity_dataframes['ContractProjects'].copy()
df_paymentEstimates = entity_dataframes['PaymentEstimates'].copy()
df_RefVendors = entity_dataframes['RefVendors'].copy()
df_ContractPaymentEstimateTypes = entity_dataframes['ContractPaymentEstimateTypes'].copy()
df_RefPaymentEstimateTypes = entity_dataframes['RefPaymentEstimateTypes'].copy()

# Add suffix to all dataframes
df_contracts = df_contracts.add_suffix('_contracts')
df_contractTimes = df_contractTimes.add_suffix('_contractTimes')
df_contractProjects = df_contractProjects.add_suffix('_contractProjects')
df_paymentEstimates = df_paymentEstimates.add_suffix('_paymentEstimates')
df_RefVendors = df_RefVendors.add_suffix('_refVendors')
df_ContractPaymentEstimateTypes = df_ContractPaymentEstimateTypes.add_suffix('_ContractPaymentEstimateTypes')
df_RefPaymentEstimateTypes = df_RefPaymentEstimateTypes.add_suffix('_RefPaymentEstimateTypes')

# Rename columns
df_contracts = df_contracts.rename(columns={'Name_contracts': 'contract_id'}) # add 'PercentPaid':'percent_complete_work'
df_RefVendors = df_RefVendors.rename(columns={'LongName_refVendors': 'contractor_longname','ShortName_refVendors':'contractor_shortname'})
df_contractProjects = df_contractProjects.rename(columns={'Name_contractProjects': 'project_ids'})


# join 1: Join Contract.PrimeRefVendorId with RefVendor.Id 
df_join = pd.merge(df_contracts, df_RefVendors, left_on='PrimeRefVendorId_contracts', right_on = 'Id_refVendors', how='left')

# join 2: Join ContractProjects.ContractId with Contract.Id 
df_contractProjects = df_contractProjects[df_contractProjects['Controlling_contractProjects'] == 1] # Filter out associated Project id's and keep only the primary one to avoid duplicate data. Also, the rest of the project ID's can be joined through a separate table.
df_join = pd.merge(df_join, df_contractProjects, left_on='Id_contracts', right_on='ContractId_contractProjects', how='outer')

# join 4: Join PaymentEstimates.ContractId with Contract.Id - **Need to use the most recent payment estimate.
df_join = pd.merge(df_join, df_paymentEstimates, left_on='Id_contracts', right_on='ContractId_paymentEstimates', how='left')
# join 5: Join ContractTime.ContractId with Contract.Id 
df_join = pd.merge(df_join, df_contractTimes, left_on='Id_contracts', right_on= 'ContractId_contractTimes', how = 'left')

# join 6: join 'PaymentEstimate' with 'ContractPaymentEstimateType' on PaymentEstimate.ContractPaymentEstimateTypeId and ContractPaymentEstimateType.ContractPaymentEstimateTypeId
df_join = pd.merge(df_join, df_ContractPaymentEstimateTypes, left_on='ContractPaymentEstimateTypeId_paymentEstimates', right_on='Id_ContractPaymentEstimateTypes', how='left')
# join 7: 'ContractPaymentEstimateType' and 'RefPaymentEstimateType' on ContractPaymentEstimateType.RefPaymentEstimateTypeId and RefPaymentEstimateType.Id
df_join = pd.merge(df_join, df_RefPaymentEstimateTypes, left_on='RefPaymentEstimateTypeId_ContractPaymentEstimateTypes', right_on='Id_RefPaymentEstimateTypes', how='left' )
# optional - print all the data fetched


# Subset columns
df_join1 = df_join.copy()
df_join1 = df_join1[['contract_id','FedProjectNum_contracts','Description_contracts','Location_contracts', 'contractor_longname', 
                    'project_ids','AwardedContractAmount_contracts','CurrentContractAmount_contracts','PercentPaid_contracts',
                    'EstimateNumber_paymentEstimates','Status_paymentEstimates','ApprovalDate_paymentEstimates','PeriodEndDate_paymentEstimates','TransferToAccountingDate_paymentEstimates',
                    'AccountingReceivedDate_paymentEstimates','CheckDate_paymentEstimates','PaymentEstimateType_RefPaymentEstimateTypes','PreviousGrossItemInstalledAmount_paymentEstimates',
                    'PreviousOverrunAdjustmentAmount_paymentEstimates','PreviousPriceAdjustmentAmount_paymentEstimates','PreviousStockpileAdjustmentAmount_paymentEstimates','PreviousOtherItemAdjustmentAmount_paymentEstimates','PreviousGrossItemAdjustmentAmount_paymentEstimates',
                    'PreviousItemPaidGrossAmount_paymentEstimates','PreviousGrossRetainageAmount_paymentEstimates','PreviousDisincentiveAmount_paymentEstimates','PreviousLiqDamageAmount_paymentEstimates','PreviousOtherContractAdjAmount_paymentEstimates',
                    'PreviousPaidAmount_paymentEstimates','CurrentGrossItemInstalledAmount_paymentEstimates','CurrentOverrunAdjustmentAmount_paymentEstimates','CurrentPriceAdjustmentAmount_paymentEstimates','CurrentStockpileAdjustmentAmount_paymentEstimates',
                    'CurrentOtherItemAdjustmentAmount_paymentEstimates','CurrentGrossItemAdjustmentAmount_paymentEstimates','CurrentItemPaidGrossAmount_paymentEstimates','CurrentGrossRetainageAmount_paymentEstimates','CurrentDisincentiveAmount_paymentEstimates',
                    'CurrentLiqDamageAmount_paymentEstimates','CurrentOtherContractAdjAmount_paymentEstimates','CurrentPaidAmount_paymentEstimates',
                    'TotalGrossItemInstalledAmount_paymentEstimates','TotalOverrunAdjustmentAmount_paymentEstimates','TotalPriceAdjustmentAmount_paymentEstimates','TotalStockpileAdjustmentAmount_paymentEstimates','TotalOtherItemAdjustmentAmount_paymentEstimates',
                    'TotalGrossItemAdjustmentAmount_paymentEstimates','TotalItemPaidGrossAmount_paymentEstimates','TotalGrossRetainageAmount_paymentEstimates','TotalDisincentiveAmount_paymentEstimates','TotalLiqDamageAmount_paymentEstimates',
                    'TotalOtherContractAdjAmount_paymentEstimates','TotalPaidAmount_paymentEstimates','ActualCompletionDate_contractTimes','Name_contractTimes','Description_contractTimes'
                    ]] # Get rid of lastupdated, projectedcompletion dates and add dates code columns tat describe the type of actual completion date. Refer to data ditcionary

# get year-month from approval date column
# Convert the 'datetime' column to datetime format 

df_join1['Payment_Approval_YearMonth'] = df_join1['ApprovalDate_paymentEstimates'].str.extract(r'(\d{4}-\d{2})')

df_join1['Payment_Amount_Sum_by_Month'] = df_join1.groupby(['contract_id','Payment_Approval_YearMonth'])['CurrentPaidAmount_paymentEstimates'].transform('sum')

# Subset records with paymentEstimates status as 'Approved'
df_join1 = df_join1[df_join1['Status_paymentEstimates'] == 'Approved']
# Subset records with 'ContractTimes' of interest
df_join1 = df_join1[df_join1['Name_contractTimes'].isin(['NTP-DT','ACTS','PCD'])]

# Drop duplicates as a result of a join with ContractTimes in case
df_join1.drop_duplicates(keep='first',inplace=True)
# print(df_join1) # print main/primary output

# Optional - Write data to a directory
df_join1.to_csv(r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\State DOT API\Nebraska DOT\Monthly\ne_api_bulk_may_2025.csv")

# Associated project ID's/sub project ID's table
associated_projectNumbers_df = entity_dataframes['ContractProjects'][entity_dataframes['ContractProjects']['Controlling'] == 0]

associated_projectNumbers = pd.merge(df_contracts,associated_projectNumbers_df, left_on='Id_contracts', right_on='ContractId', how='inner')
associated_projectNumbers = associated_projectNumbers[['Id_contracts', 'ContractId', 'contract_id', 'Name', 'Description', 'ContractProposalName','Location','CreatedDate','LastUpdatedDate','FedProjectNum']]
associated_projectNumbers = associated_projectNumbers.rename({'Name': 'project_ID'})

# Optional - Write data to a directory
associated_projectNumbers.to_csv(r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\State DOT API\Nebraska DOT\Monthly\ne_api_sub_proj_may_2025.csv")