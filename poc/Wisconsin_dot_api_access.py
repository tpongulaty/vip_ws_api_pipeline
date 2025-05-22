import requests
import time
import base64
import hashlib
import pandas as pd
import os

pd.set_option('display.max_columns', None)
# API prod connection
base_url = os.getenv("BASE_URL_WS")

IntegrationName = os.getenv("INTEGRATION_NAME_WS") # or other name
# For prod environment
secrectAccessKey = os.getenv("SECRET_ACCESS_KEY_WS")  
subscription_key = os.getenv("SUBSCRIPTION_KEY")
cache_Control = "no-cache"

entities_to_fetch = ['Contracts', 'ContractTimes', 'ContractProjects', 'PaymentEstimates', 'RefVendors','ContractPaymentEstimateTypes','RefPaymentEstimateTypes']
entity_dataframes = {}

def get_server_time():
    thirty_sec_window = 30
    current_time = str(int(time.time() // thirty_sec_window))
    return current_time

# calculate TOTP using PBKDF2 with HMAC-SHA1
def calculate_totp(secret:str, current_time):
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

def get_entity_data(auth_header, subscription_key, entity,skip=0,temporal_start = None):
    headers = {
        'Authorization': auth_header,
        'Cache_Control': 'no-cache',
        'Ocp-Apim-Subscription-Key': subscription_key
    }
    params = {'$skip': skip}
    if temporal_start:
        params['temporalStart'] = temporal_start
    response = requests.get(f'{base_url}/{entity}', headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# loop through the list of entities and use skip parameter to fetch all records (note: Record limit per request - 1000)
for entity in entities_to_fetch:
    skip = 0
    all_records = []

    while True:
        server_time = get_server_time()
        totp = calculate_totp(secrectAccessKey, server_time)
        auth_header = create_auth_header(IntegrationName, totp)
        # Optional: Retreive only updated records since a specific date (Note: Currently this method isn't working and is fetching all the records)
        # temporal_start = '2024-07-01'
        # temporal_start = None
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
df_contracts = entity_dataframes['Contracts']
df_contractTimes = entity_dataframes['ContractTimes']
df_contractProjects = entity_dataframes['ContractProjects']
df_paymentEstimates = entity_dataframes['PaymentEstimates']
df_RefVendors = entity_dataframes['RefVendors']
df_ContractPaymentEstimateTypes = entity_dataframes['ContractPaymentEstimateTypes']
df_RefPaymentEstimateTypes = entity_dataframes['RefPaymentEstimateTypes']

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

# Filter out testing contracts
df_contracts = df_contracts[df_contracts['contract_id'] != '20191210007T']
df_contracts = df_contracts[df_contracts['contract_id'] != 'LCS001']

# Contract.SuretyCompanyId joined with RefVendor.Id and display RefVendor.LongName
df_4 = pd.merge(df_contracts, df_RefVendors, left_on='SuretyCompanyId_contracts', right_on = 'Id_refVendors' ,how='left')
df_4 = df_4[['contract_id','contractor_longname']]
df_4 = df_4.rename(columns={'contractor_longname': 'surety_longname'})


# join 1: Join Contract.PrimeRefVendorId with RefVendor.Id 
df_join = pd.merge(df_contracts, df_RefVendors, left_on='PrimeRefVendorId_contracts', right_on = 'Id_refVendors', how='left')
# join 2: Join ContractProjects.ContractId with Contract.Id 
df_contractProjects = df_contractProjects[df_contractProjects['Controlling_contractProjects'] == 1] # Filter out associated Project id's and keep only the primary one to avoid duplicate data. Also, the rest of the project ID's can be joined through a separate table.
df_join = pd.merge(df_join, df_contractProjects, left_on='Id_contracts', right_on='ContractId_contractProjects', how='outer')
# join 3: join with df_4 to get surety_name
df_join = pd.merge(df_join, df_4, on='contract_id', how='left' )
# join 4: Join PaymentEstimates.ContractId with Contract.Id - **Need to use the most recent payment estimate.
df_join = pd.merge(df_join, df_paymentEstimates, left_on='Id_contracts', right_on='ContractId_paymentEstimates', how='left')
#filter contracttimes with Main = 1
df_contractTimes = df_contractTimes[df_contractTimes['Main_contractTimes'] == 1]
# join 5: Join ContractTime.ContractId with Contract.Id - Also will want to make sure that ContractTime.Main ='1'
df_join = pd.merge(df_join, df_contractTimes, left_on='Id_contracts', right_on= 'ContractId_contractTimes', how = 'left')
# join 6: join 'PaymentEstimate' with 'ContractPaymentEstimateType' on PaymentEstimate.ContractPaymentEstimateTypeId and ContractPaymentEstimateType.ContractPaymentEstimateTypeId
df_join = pd.merge(df_join, df_ContractPaymentEstimateTypes, left_on='ContractPaymentEstimateTypeId_paymentEstimates', right_on='Id_ContractPaymentEstimateTypes', how='left')
# join 7: 'ContractPaymentEstimateType' and 'RefPaymentEstimateType' on ContractPaymentEstimateType.RefPaymentEstimateTypeId and RefPaymentEstimateType.Id
df_join = pd.merge(df_join, df_RefPaymentEstimateTypes, left_on='RefPaymentEstimateTypeId_ContractPaymentEstimateTypes', right_on='Id_RefPaymentEstimateTypes', how='left' )
# optional - print all the data fetched
# print(df_join)

# Subset columns
df_join1 = df_join[['contract_id','Description_contracts','Location_contracts', 'contractor_shortname','surety_longname', 
                    'project_ids','AwardedContractAmount_contracts','CurrentContractAmount_contracts','PercentPaid_contracts','Type_contractTimes',
                    'TimeUnitsChargedOnApprEsts_contractTimes','CurrentNumberOfTimeUnits_contractTimes','StartTime_contractTimes','CurrentCompletionDate_contractTimes','ActualCompletionDate_contractTimes',
                    'PercentComplete_contractTimes', 'EstimateNumber_paymentEstimates','Status_paymentEstimates','ApprovalDate_paymentEstimates','PeriodEndDate_paymentEstimates','TransferToAccountingDate_paymentEstimates',
                    'AccountingReceivedDate_paymentEstimates','CheckDate_paymentEstimates','Name_RefPaymentEstimateTypes','PaymentEstimateType_RefPaymentEstimateTypes','PreviousGrossItemInstalledAmount_paymentEstimates',
                    'PreviousOverrunAdjustmentAmount_paymentEstimates','PreviousPriceAdjustmentAmount_paymentEstimates','PreviousStockpileAdjustmentAmount_paymentEstimates','PreviousOtherItemAdjustmentAmount_paymentEstimates','PreviousGrossItemAdjustmentAmount_paymentEstimates',
                    'PreviousItemPaidGrossAmount_paymentEstimates','PreviousGrossRetainageAmount_paymentEstimates','PreviousDisincentiveAmount_paymentEstimates','PreviousLiqDamageAmount_paymentEstimates','PreviousOtherContractAdjAmount_paymentEstimates',
                    'PreviousPaidAmount_paymentEstimates','CurrentGrossItemInstalledAmount_paymentEstimates','CurrentOverrunAdjustmentAmount_paymentEstimates','CurrentPriceAdjustmentAmount_paymentEstimates','CurrentStockpileAdjustmentAmount_paymentEstimates',
                    'CurrentOtherItemAdjustmentAmount_paymentEstimates','CurrentGrossItemAdjustmentAmount_paymentEstimates','CurrentItemPaidGrossAmount_paymentEstimates','CurrentGrossRetainageAmount_paymentEstimates','CurrentDisincentiveAmount_paymentEstimates',
                    'CurrentLiqDamageAmount_paymentEstimates','CurrentOtherContractAdjAmount_paymentEstimates','CurrentPaidAmount_paymentEstimates',
                    'TotalGrossItemInstalledAmount_paymentEstimates','TotalOverrunAdjustmentAmount_paymentEstimates','TotalPriceAdjustmentAmount_paymentEstimates','TotalStockpileAdjustmentAmount_paymentEstimates','TotalOtherItemAdjustmentAmount_paymentEstimates',
                    'TotalGrossItemAdjustmentAmount_paymentEstimates','TotalItemPaidGrossAmount_paymentEstimates','TotalGrossRetainageAmount_paymentEstimates','TotalDisincentiveAmount_paymentEstimates','TotalLiqDamageAmount_paymentEstimates',
                    'TotalOtherContractAdjAmount_paymentEstimates','TotalPaidAmount_paymentEstimates'
                    ]]

# Subset records with paymentEstimates status as 'Approved'
df_join1 = df_join1[df_join1['Status_paymentEstimates'] == 'Approved']
# print(df_join1) # print main/primary output

# Optional - Write data to a directory
# df_join1.to_excel(r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\Wisconsin\Monthly\ws_api_bulk_april.xlsx")

# Associated project ID's/sub project ID's table
associated_projectNumbers_df = entity_dataframes['ContractProjects'][entity_dataframes['ContractProjects']['Controlling'] == 0]

associated_projectNumbers = pd.merge(df_contracts,associated_projectNumbers_df, left_on='Id_contracts', right_on='ContractId', how='inner')
associated_projectNumbers = associated_projectNumbers[['Id_contracts', 'ContractId', 'contract_id', 'Name', 'Description', 'ContractProposalName','Location','CreatedDate','LastUpdatedDate','FedProjectNum']]
associated_projectNumbers = associated_projectNumbers.rename({'Name': 'project_ID'})
# print(associated_projectNumbers) # print Associated project ID's/sub project ID's table

# Optional - Write data to a directory
# associated_projectNumbers.to_excel(r"C:\Users\TarunPongulaty\Documents\Revealgc\Reveal_Census - databases\Tarun\dot_scraping\Wisconsin\Monthly\ws_api_sub_proj_april.xlsx")