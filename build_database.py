# Use this script to write the python needed to complete this task
import requests
import json
import pandas as pd
import csv
import sqlite3
import time

base_url = 'https://www.thecocktaildb.com/api/json/v1/1/'

bar_stock_data = pd.read_csv('data/bar_data.csv')

def create_db_and_tables(df):
    with open('data_tables.SQL', 'r') as sql_file:
        data_tables_sql = sql_file.read()
    con = sqlite3.connect("bars_database.sqlite")
    cursor = con.cursor()
    cursor.executescript(data_tables_sql)
    con.commit()
    df.to_sql('all_bars_transaction_data', con, if_exists='replace', index=False)
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
    drinks_and_glasses = {}
    endpoint = 'search.php?s='
    headers = {'Accept': 'application/json'}
    for dd in distinct_drinks:
        data = requests.get(f'{base_url}{endpoint}{dd}', headers=headers).json()
        glass = data.get("drinks")[0].get("strGlass")
        drinks_and_glasses[dd] = glass
        time.sleep(1)
    return drinks_and_glasses

def create_sqlite_database(db):
    """Creates a new database based on the provided database file
    Args:
        db (string): The name of the database file
    """
    return 0

def main():
    df = create_all_transactions_dataframe()
    con = create_db_and_tables(df)
    distinct_drinks = get_distinct_drinks(con)
    drinks_and_glasses = get_glass_from_drink(distinct_drinks)
    json.dump(drinks_and_glasses, 'drinks_and_glasses.json')
    

if __name__ == '__main__':
    main()