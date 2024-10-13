import requests
import pandas as pd
import time

import csv
import ssl

from fredapi import Fred

# Disable ssl verification
#ssl._create_default_https_context = ssl._create_unverified_context

# api keys
api_key = 'j9olnO9YOpXU3YS2CqrdOQNjXz5ncbXv'  
fred_api_key = '4848b89eeb906a5bf2d7f58983a1eb81'


# number of quarters to retrieve (last 20 years = 80 quarters)
num_quarters = 80

# Function to fetch income statement data for a company from FMP api
def fetch_income_statement_data(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/income-statement/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()
        
        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append profit-related financial data for each quarter
                financial_data.append({
                     "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "reportedCurrency": statement.get('reportedCurrency'),
                    "cik": statement.get('cik'),
                    "fillingDate": statement.get('fillingDate'),
                    "acceptedDate": statement.get('acceptedDate'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),
                    "revenue": statement.get('revenue'),
                    "costOfRevenue": statement.get('costOfRevenue'),
                    "grossProfit": statement.get('grossProfit'),
                    "grossProfitRatio": statement.get('grossProfitRatio'),
                    "researchAndDevelopmentExpenses": statement.get('researchAndDevelopmentExpenses'),
                    "generalAndAdministrativeExpenses": statement.get('generalAndAdministrativeExpenses'),
                    "sellingAndMarketingExpenses": statement.get('sellingAndMarketingExpenses'),
                    "sellingGeneralAndAdministrativeExpenses": statement.get('sellingGeneralAndAdministrativeExpenses'),
                    "otherExpenses": statement.get('otherExpenses'),
                    "operatingExpenses": statement.get('operatingExpenses'),
                    "costAndExpenses": statement.get('costAndExpenses'),
                    "interestIncome": statement.get('interestIncome'),
                    "interestExpense": statement.get('interestExpense'),
                    "depreciationAndAmortization": statement.get('depreciationAndAmortization'),
                    "ebitda": statement.get('ebitda'),
                    "ebitdaratio": statement.get('ebitdaratio'),
                    "operatingIncome": statement.get('operatingIncome'),
                    "operatingIncomeRatio": statement.get('operatingIncomeRatio'),
                    "totalOtherIncomeExpensesNet": statement.get('totalOtherIncomeExpensesNet'),
                    "incomeBeforeTax": statement.get('incomeBeforeTax'),
                    "incomeBeforeTaxRatio": statement.get('incomeBeforeTaxRatio'),
                    "incomeTaxExpense": statement.get('incomeTaxExpense'),
                    "netIncome": statement.get('netIncome'),
                    "netIncomeRatio": statement.get('netIncomeRatio'),
                    "eps": statement.get('eps'),
                    "epsdiluted": statement.get('epsdiluted'),
                    "weightedAverageShsOut": statement.get('weightedAverageShsOut'),
                    "weightedAverageShsOutDil": statement.get('weightedAverageShsOutDil')
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []

# Function to fetch cash flow data for a company from FMP api
def fetch_cash_flow_data(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/cash-flow-statement/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)

    try: 
        data = response.json()
        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "reportedCurrency": statement.get('reportedCurrency'),
                    "cik": statement.get('cik'),
                    "fillingDate": statement.get('fillingDate'),
                    "acceptedDate": statement.get('acceptedDate'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),
                    "netIncome": statement.get('netIncome'),
                    "depreciationAndAmortization": statement.get('depreciationAndAmortization'),
                    "deferredIncomeTax": statement.get('deferredIncomeTax'),
                    "stockBasedCompensation": statement.get('stockBasedCompensation'),
                    "changeInWorkingCapital": statement.get('changeInWorkingCapital'),
                    "accountsReceivables": statement.get('accountsReceivables'),
                    "inventory": statement.get('inventory'),
                    "accountsPayables": statement.get('accountsPayables'),
                    "otherWorkingCapital": statement.get('otherWorkingCapital'),
                    "otherNonCashItems": statement.get('otherNonCashItems'),
                    "netCashProvidedByOperatingActivities": statement.get('netCashProvidedByOperatingActivities'),
                    "investmentsInPropertyPlantAndEquipment": statement.get('investmentsInPropertyPlantAndEquipment'),
                    "acquisitionsNet": statement.get('acquisitionsNet'),
                    "purchasesOfInvestments": statement.get('purchasesOfInvestments'),
                    "salesMaturitiesOfInvestments": statement.get('salesMaturitiesOfInvestments'),
                    "otherInvestingActivites": statement.get('otherInvestingActivites'),
                    "netCashUsedForInvestingActivites": statement.get('netCashUsedForInvestingActivites'),
                    "debtRepayment": statement.get('debtRepayment'),
                    "commonStockIssued": statement.get('commonStockIssued'),
                    "commonStockRepurchased": statement.get('commonStockRepurchased'),
                    "dividendsPaid": statement.get('dividendsPaid'),
                    "otherFinancingActivites": statement.get('otherFinancingActivites'),
                    "netCashUsedProvidedByFinancingActivities": statement.get('netCashUsedProvidedByFinancingActivities'),
                    "effectOfForexChangesOnCash": statement.get('effectOfForexChangesOnCash'),
                    "netChangeInCash": statement.get('netChangeInCash'),
                    "cashAtEndOfPeriod": statement.get('cashAtEndOfPeriod'),
                    "cashAtBeginningOfPeriod": statement.get('cashAtBeginningOfPeriod'),
                    "operatingCashFlow": statement.get('operatingCashFlow'),
                    "capitalExpenditure": statement.get('capitalExpenditure'),
                    "freeCashFlow": statement.get('freeCashFlow')

                })
            return financial_data
        else: 
            print(f'Unexpected data format for {company}: {data}')
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []

# Function to fetch key metrics data for a company from FMP api
def fetch_key_metrics(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/ratios/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()

        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append the selected key financial ratios for each quarter
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),

                    # Profitability Ratios
                    "grossProfitMargin": statement.get('grossProfitMargin'),
                    "operatingProfitMargin": statement.get('operatingProfitMargin'),
                    "netProfitMargin": statement.get('netProfitMargin'),
                    "returnOnAssets": statement.get('returnOnAssets'),
                    "returnOnEquity": statement.get('returnOnEquity'),
                    "returnOnCapitalEmployed": statement.get('returnOnCapitalEmployed'),

                    # Valuation Ratios
                    "peRatio": statement.get('peRatio'),
                    "priceToSalesRatio": statement.get('priceToSalesRatio'),
                    "enterpriseValueOverEBITDA": statement.get('enterpriseValueOverEBITDA'),
                    "priceToFreeCashFlowsRatio": statement.get('priceToFreeCashFlowsRatio'),

                    # Cash Flow Ratios
                    "operatingCashFlowPerShare": statement.get('operatingCashFlowPerShare'),
                    "freeCashFlowPerShare": statement.get('freeCashFlowPerShare'),
                    "operatingCashFlowSalesRatio": statement.get('operatingCashFlowSalesRatio'),
                    "cashFlowCoverageRatios": statement.get('cashFlowCoverageRatios'),

                    # Leverage Ratios
                    "debtToEquity": statement.get('debtToEquity'),
                    "debtRatio": statement.get('debtRatio'),
                    "interestCoverage": statement.get('interestCoverage'),

                    # Efficiency Ratios
                    "receivablesTurnover": statement.get('receivablesTurnover'),
                    "inventoryTurnover": statement.get('inventoryTurnover'),
                    "assetTurnover": statement.get('assetTurnover'),

                    # Dividend Ratios (Optional)
                    "dividendPayoutRatio": statement.get('dividendPayoutRatio'),
                    "dividendYield": statement.get('dividendYield')
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []



# Function to fetch balance sheet data for a company
def fetch_balance_sheet_data(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/balance-sheet-statement/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()

        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append balance sheet-related financial data for each quarter
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "reportedCurrency": statement.get('reportedCurrency'),
                    "cik": statement.get('cik'),
                    "fillingDate": statement.get('fillingDate'),
                    "acceptedDate": statement.get('acceptedDate'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),
                    "cashAndCashEquivalents": statement.get('cashAndCashEquivalents'),
                    "shortTermInvestments": statement.get('shortTermInvestments'),
                    "cashAndShortTermInvestments": statement.get('cashAndShortTermInvestments'),
                    "netReceivables": statement.get('netReceivables'),
                    "inventory": statement.get('inventory'),
                    "otherCurrentAssets": statement.get('otherCurrentAssets'),
                    "totalCurrentAssets": statement.get('totalCurrentAssets'),
                    "propertyPlantEquipmentNet": statement.get('propertyPlantEquipmentNet'),
                    "goodwill": statement.get('goodwill'),
                    "intangibleAssets": statement.get('intangibleAssets'),
                    "goodwillAndIntangibleAssets": statement.get('goodwillAndIntangibleAssets'),
                    "longTermInvestments": statement.get('longTermInvestments'),
                    "taxAssets": statement.get('taxAssets'),
                    "otherNonCurrentAssets": statement.get('otherNonCurrentAssets'),
                    "totalNonCurrentAssets": statement.get('totalNonCurrentAssets'),
                    "otherAssets": statement.get('otherAssets'),
                    "totalAssets": statement.get('totalAssets'),
                    "accountPayables": statement.get('accountPayables'),
                    "shortTermDebt": statement.get('shortTermDebt'),
                    "taxPayables": statement.get('taxPayables'),
                    "deferredRevenue": statement.get('deferredRevenue'),
                    "otherCurrentLiabilities": statement.get('otherCurrentLiabilities'),
                    "totalCurrentLiabilities": statement.get('totalCurrentLiabilities'),
                    "longTermDebt": statement.get('longTermDebt'),
                    "deferredRevenueNonCurrent": statement.get('deferredRevenueNonCurrent'),
                    "deferredTaxLiabilitiesNonCurrent": statement.get('deferredTaxLiabilitiesNonCurrent'),
                    "otherNonCurrentLiabilities": statement.get('otherNonCurrentLiabilities'),
                    "totalNonCurrentLiabilities": statement.get('totalNonCurrentLiabilities'),
                    "otherLiabilities": statement.get('otherLiabilities'),
                    "capitalLeaseObligations": statement.get('capitalLeaseObligations'),
                    "totalLiabilities": statement.get('totalLiabilities'),
                    "preferredStock": statement.get('preferredStock'),
                    "commonStock": statement.get('commonStock'),
                    "retainedEarnings": statement.get('retainedEarnings'),
                    "accumulatedOtherComprehensiveIncomeLoss": statement.get('accumulatedOtherComprehensiveIncomeLoss'),
                    "othertotalStockholdersEquity": statement.get('othertotalStockholdersEquity'),
                    "totalStockholdersEquity": statement.get('totalStockholdersEquity'),
                    "totalEquity": statement.get('totalEquity'),
                    "totalLiabilitiesAndStockholdersEquity": statement.get('totalLiabilitiesAndStockholdersEquity'),
                    "minorityInterest": statement.get('minorityInterest'),
                    "totalLiabilitiesAndTotalEquity": statement.get('totalLiabilitiesAndTotalEquity'),
                    "totalInvestments": statement.get('totalInvestments'),
                    "totalDebt": statement.get('totalDebt'),
                    "netDebt": statement.get('netDebt')
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []

import requests

# Function to fetch cash flow grwoth data for a company from FMP api
def fetch_cashflow_growth(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/cash-flow-statement-growth/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()

        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append the selected key financial growth metrics for each quarter
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),

                    # Key Growth Metrics
                    "growthNetIncome": statement.get('growthNetIncome'),
                    "growthDepreciationAndAmortization": statement.get('growthDepreciationAndAmortization'),
                    "growthOperatingCashFlow": statement.get('growthOperatingCashFlow'),
                    "growthFreeCashFlow": statement.get('growthFreeCashFlow'),
                    "growthCapitalExpenditure": statement.get('growthCapitalExpenditure'),
                    "growthCommonStockRepurchased": statement.get('growthCommonStockRepurchased'),
                    "growthDeferredIncomeTax": statement.get('growthDeferredIncomeTax'),
                    "growthNetCashProvidedByOperatingActivities": statement.get('growthNetCashProvidedByOperatingActivities')
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []


# Function to fetch income statment growth data for a company from FMP api
def fetch_income_growth(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/income-statement-growth/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()

        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append the selected key financial growth metrics for each quarter
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),

                    # Key Income Growth Metrics
                    "growthRevenue": statement.get('growthRevenue'),
                    "growthGrossProfit": statement.get('growthGrossProfit'),
                    "growthOperatingIncome": statement.get('growthOperatingIncome'),
                    "growthEbitda": statement.get('growthEbitda'),
                    "growthNetIncome": statement.get('growthNetIncome'),
                    "growthIncomeBeforeTax": statement.get('growthIncomeBeforeTax'),
                    "growthOperatingExpenses": statement.get('growthOperatingExpenses')
  
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []

# Function to fetch balance sheet growth data for a company from FMP api
def fetch_balance_sheet_growth(company, api_key, num_quarters):
    url = f'https://financialmodelingprep.com/api/v3/balance-sheet-statement-growth/{company}?period=quarter&apikey={api_key}'
    response = requests.get(url)
    
    try:
        data = response.json()

        if isinstance(data, list):
            financial_data = []
            for statement in data[:num_quarters]:
                # Append the selected key financial growth metrics for each quarter
                financial_data.append({
                    "date": statement.get('date'),
                    "symbol": statement.get('symbol'),
                    "calendarYear": statement.get('calendarYear'),
                    "period": statement.get('period'),

                    # Key Growth Metrics
                    "growthCashAndCashEquivalents": statement.get('growthCashAndCashEquivalents'),
                    "growthNetReceivables": statement.get('growthNetReceivables'),
                    "growthInventory": statement.get('growthInventory'),
                    "growthTotalAssets": statement.get('growthTotalAssets'),
                    "growthTotalLiabilities": statement.get('growthTotalLiabilities'),
                    "growthRetainedEarnings": statement.get('growthRetainedEarnings'),
                    "growthTotalStockholdersEquity": statement.get('growthTotalStockholdersEquity')
         
                })
            return financial_data
        else:
            print(f"Unexpected data format for {company}: {data}")
            return []
    except Exception as e:
        print(f"Error processing data for {company}: {e}")
        return []




# Function to save income statment data for a list of companies into a csv file
def fetch_and_save_income(companies, api_key, num_quarters, filename):
    all_financial_data = []

    for company in companies:
        financial_data = fetch_income_statement_data(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)

        # to avoid hitting the api limit
        #time.sleep(1)


    df = pd.DataFrame(all_financial_data)

    # Save the dataframe to a csv file 
    df.to_csv(filename, index=False)
    print(f"Income Statment data saved to {filename}")


# Function to save balance sheet data for a list of companies into a csv file
def fetch_and_save_balance_sheet(companies, api_key, num_quarters, filename):
    all_financial_data = []

    for company in companies:
        financial_data = fetch_balance_sheet_data(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)

       
        #time.sleep(1)


    df = pd.DataFrame(all_financial_data)

    # Save the df to a csv file
    df.to_csv(filename, index=False)
    print(f"Balance sheet data saved to {filename}")

# Function to save cash flow data for a list of companies into a csv file
def fetch_and_save_cash_flow(companies, api_key, num_quarters, filename):
    all_financial_data = []
    for company in companies:
        financial_data = fetch_cash_flow_data(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)
        
        # time.sleep(1)
    
    df = pd.DataFrame(all_financial_data)

    df.to_csv(filename, index=False)
    print(f"Cash flow statment data saved to {filename}")

# Function to save key metrics data for a list of companies into a csv file
def fetch_and_save_key_metric(companies, api_key, num_quarters, filename):
    all_financial_data = []
    for company in companies:
        financial_data = fetch_key_metrics(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)
        
        # time.sleep(1)
    
    df = pd.DataFrame(all_financial_data)

    df.to_csv(filename, index=False)
    print(f"Key metric ratios saved to {filename}")

# Function to save cash flow growth data for a list of companies into a csv file
def fetch_and_save_cashflow_growth(companies, api_key, num_quarters, filename):
    all_financial_data = []
    for company in companies:
        financial_data = fetch_cashflow_growth(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)
        
        # time.sleep(1)
    
    df = pd.DataFrame(all_financial_data)

    df.to_csv(filename, index=False)
    print(f"Cash flow growth data saved to {filename}")

# Function to save income statement growth data for a list of companies into a csv file
def fetch_and_save_income_growth(companies, api_key, num_quarters, filename):
    all_financial_data = []
    for company in companies:
        financial_data = fetch_income_growth(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)
        
        # time.sleep(1)
    
    df = pd.DataFrame(all_financial_data)

    df.to_csv(filename, index=False)
    print(f"Income growth data saved to {filename}")    

# Function to save balance sheet statement growth data for a list of companies into a csv file
def fetch_and_save_balance_sheet_growth(companies, api_key, num_quarters, filename):
    all_financial_data = []
    for company in companies:
        financial_data = fetch_balance_sheet_growth(company, api_key, num_quarters)
        all_financial_data.extend(financial_data)
        
        # time.sleep(1)
    
    df = pd.DataFrame(all_financial_data)

    df.to_csv(filename, index=False)
    print(f"Blance sheet growth data saved to {filename}")



# Function to fetch and save quarterly data(interest rate, unemployment ...) from FRED(Federal Reserve of Economic Data) 
def fetch_fred_data(api_key, fred_ids, start_date, end_date, file_names):
    

    fred = Fred(fred_api_key)

    for series_id, description in fred_ids.items():
        
        # Fetch data from FRED for each series i.e 
        data = fred.get_series(series_id, start_date, end_date)

        # Convert to a df and resample to quarterly data
        df = pd.DataFrame(data, columns=[description])
        df.index = pd.to_datetime(df.index)
        df_quarterly = df.resample('Q').mean()  # resample to quarterly and calculate the mean for each quarter

        # Create quarter column 
        df_quarterly['Quarter'] = df_quarterly.index.quarter  
        df_quarterly['Quarter'] = df_quarterly['Quarter'].map({1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'})

        # to csv file
        csv_file = file_names.get(series_id, f'{series_id}.csv')  
        df_quarterly.to_csv(csv_file)

        print(f"Quarterly {description} data has been saved to '{csv_file}'")




