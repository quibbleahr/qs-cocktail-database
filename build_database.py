# Use this script to write the python needed to complete this task
import requests
import json
import pandas as pd
import csv
import sqlite3
import time

base_url = 'https://www.thecocktaildb.com/api/json/v1/1/'

def upload_df_to_sqlite_db(df, con, table_name):
    df.to_sql(table_name, con, if_exists='replace', index=False)

def normalize_bar_stock_data(filename):
    bar_stock_data = pd.read_csv(filename)
    bar_stock_data['stock'] = bar_stock_data['stock'].str.extract('(\d+)').astype(int)
    return bar_stock_data

def create_db_and_tables():
    with open('data_tables.SQL', 'r') as sql_file:
        data_tables_sql = sql_file.read()
    con = sqlite3.connect("bars_database.sqlite")
    cursor = con.cursor()
    cursor.executescript(data_tables_sql)
    con.commit()
    return con

def standardize_london_transactions_csv_to_df(filename):
    """Standardizes the london_transactions.csv file to separate columns as 'timestamp','drink','amount', and places the data into a Pandas Dataframe.

    Args:
        filename (string): a string containing the name of the file to open

    Returns:
        pandas.Dataframe: A pandas dataframe created from the file with columns 'timestamp','drink','amount'
    """
    csv_data = []
    with open(filename, "r") as file:
        reader = csv.reader(file, delimiter='\t')
        for row in reader:
            csv_data.append(row)
    df = pd.DataFrame(csv_data, columns=['row','timestamp','drink','amount'])
    df.drop(columns=df.columns[0], axis=1, inplace=True)
    return df

def get_distinct_drinks(con):
    con.row_factory = lambda cursor, row: row[0]
    c = con.cursor()
    distinct_drinks = c.execute('''SELECT DISTINCT drink FROM all_bars_transaction_data''').fetchall()
    return distinct_drinks

def create_all_transactions_dataframe():
    """Imports transaction data from all branches and places them into a single dataframe.

    Returns:
        pandas.Dataframe: a pandas dataframe containing all branch transaction data
    """
    # define columns
    branch_transaction_cols = ['timestamp','drink','amount','branch']
    
    budapest_transactions = pd.read_csv('data/budapest.csv', index_col=[0]) # ignore the index column
    london_transactions = standardize_london_transactions_csv_to_df('data/london_transactions.csv')
    ny_transactions = pd.read_csv('data/ny.csv', index_col=[0]) # ignore the index column
    
    # create new column all with the same value, depending on the branch
    budapest_transactions['branch'] = 'budapest'
    london_transactions['branch'] = 'london'
    ny_transactions['branch'] = 'new york'
    
    # rename columns in each dataframe so they match
    budapest_transactions.columns = branch_transaction_cols
    london_transactions.columns = branch_transaction_cols
    ny_transactions.columns = branch_transaction_cols
    
    # append all the dataframes
    all_transactions_dataframe = pd.concat([budapest_transactions,london_transactions, ny_transactions], ignore_index=True)
    return all_transactions_dataframe

def get_glass_from_drink(distinct_drinks):
    drinks_and_glasses = []
    endpoint = 'search.php?s='
    headers = {'Accept': 'application/json'}
    for dd in distinct_drinks:
        temp = {}
        data = requests.get(f'{base_url}{endpoint}{dd}', headers=headers).json()
        glass = data.get("drinks")[0].get("strGlass")
        temp["drink"] = dd
        temp["glass"] = glass
        drinks_and_glasses.append(temp)
        time.sleep(0.5) # I have had to include this due to a request time out issue I experienced with the API
    drinks_and_glasses_df = pd.DataFrame(drinks_and_glasses)
    return drinks_and_glasses_df

def create_poc_table(con):
    with open('poc_tables.SQL', 'r') as sql_file:
        poc_table_sql = sql_file.read()
    cursor = con.cursor()
    cursor.executescript(poc_table_sql)
    con.commit()

def main():
    bar_stock_data = normalize_bar_stock_data('data/bar_data.csv')
    all_transactions_df = create_all_transactions_dataframe()
    conn = create_db_and_tables()
    distinct_drinks = get_distinct_drinks(conn)
    drinks_and_glasses_df = get_glass_from_drink(distinct_drinks)
    upload_df_to_sqlite_db(all_transactions_df, conn, 'all_bars_transaction_data')
    upload_df_to_sqlite_db(drinks_and_glasses_df, conn, 'drinks_and_glasses')
    upload_df_to_sqlite_db(bar_stock_data, conn, 'bar_stock_data')
    create_poc_table(conn)
    

if __name__ == '__main__':
    main()