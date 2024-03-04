# Code for ETL operations on Country-GDP data

# Importing the required libraries
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3

def log_progress(message):
    #This function logs the mentioned message of a given stage of the code execution to a log file.

    now = datetime.now()
    timestamp = now.strftime('%Y-%h-%d %H:%M:%S')

    with open("code_log.txt", "a") as f:
        f.write(timestamp + ' : ' + message + "\n")

def extract(url, table_attributes):
    #This function extracts the required information from the website and save it to a data frame.

    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attributes)
    tables = soup.find_all('tbody')
    table_rows = tables[0].find_all('tr')

    for row in table_rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = float(col[2].contents[0][:-1])
            df.loc[len(df.index)] = [bank_name,market_cap]

    return df

def transform(df, csv_path):
    #This function accesses the CSV file for exchange rate information, and adds three columns to the data frame, each containing the transformed version of Market Cap column to respective currencies.

    exchange_rate = pd.read_csv(csv_path)
    dict = exchange_rate.set_index('Currency').to_dict()['Rate']

    df['MC_GBP_Billion'] = [np.round(x*dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*dict['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    #This function saves the final data frame as a CSV file in the provided path.

    df.to_csv(output_path, index = False)


def load_to_db(df, sql_connection, table_name):
    #This function saves the final data frame to a database table with the provided name.

    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

def run_query(query_statement, sql_connection):
    #This function prints and runs the given query on the database table and prints the output on the terminal.

    print(query_statement)
    df = pd.read_sql(query_statement, sql_connection)
    print(df)

def main():
    #This function defines the required entities and calls the relevant functions in the correct order to complete the project.

    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    column_names = ["Name", "MC_USD_Billion"]
    output_path = "./Largest_banks_csv"
    log_progress("Preliminaries complete. Initiating ETL process")

    data = extract(url,column_names)
    log_progress("Data extraction complete. Initiating Transformation process")

    transform(data,csv_path)
    log_progress("Data transformation complete. Initiating Loading process")

    load_to_csv(data, output_path)
    log_progress("Data saved to CSV file")

    sql_connection = sqlite3.connect("Banks.db")
    table_name = "Largest_banks"
    log_progress("SQL Connection initiated")

    load_to_db(data,sql_connection,table_name)
    log_progress("Data loaded to Database as a table, Executing queries")

    query_1 = "SELECT * FROM Largest_banks"
    run_query(query_1, sql_connection)
    query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    run_query(query_2, sql_connection)
    query_3 = "SELECT Name from Largest_banks LIMIT 5"
    run_query(query_3, sql_connection)
    log_progress("Process Complete")

    sql_connection.close()
    log_progress("Server Connection closed")

if __name__ == "__main__":
    main()