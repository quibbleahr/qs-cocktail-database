# Use this script to write the python needed to complete this task
import requests
import pandas as pd
import csv
import sqlite3
import time

base_url = 'https://www.thecocktaildb.com/api/json/v1/1/' # the API key is already included here as "1"

def normalize_bar_stock_data(filename):
    """Uploads bar_data.csv into a Pandas dataframe with the 'stock' column cast as int data type

    Args:
        filename (string): a string containing the name of the file containing bar stock data

    Returns:
        pandas.Dataframe: a dataframe containing bar stock data
    """
    bar_stock_data = pd.read_csv(filename)
    bar_stock_data['stock'] = bar_stock_data['stock'].str.extract('(\d+)').astype(int)
    return bar_stock_data

def create_db_and_tables():
    """Creates the bar database and creates the tables using the 'data_tables.SQL' file

    Returns:
        sqlite3.Connection: the connection to 'bars_database' 
    """
    with open('data_tables.SQL', 'r') as sql_file:
        data_tables_sql = sql_file.read()
    conn = sqlite3.connect("bars_database.sqlite")
    cursor = conn.cursor()
    cursor.executescript(data_tables_sql)
    conn.commit()
    return conn

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

def get_distinct_drinks(conn):
    """Connects to the SQLite 'bars_database' and gets all distinct drinks from all transactions in the three branches

    Args:
        conn (sqlite3.Connection): the connection to 'bars_database' 

    Returns:
        list: a list containing the distinct drinks 
    """
    conn.row_factory = lambda cursor, row: row[0]
    c = conn.cursor()
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
    """Gets the glass type from the list of distinct drinks using the Cocktail API

    Args:
        distinct_drinks (list): a list containing distinct drinks

    Returns:
        pandas.DataFrame: A DataFrame with the distinct drinks and their corresponding glass types
    """
    drinks_and_glasses = []
    base_url = 'https://www.thecocktaildb.com/api/json/v1/1/'
    endpoint = 'search.php?s='
    headers = {'Accept': 'application/json'}
    
    for dd in distinct_drinks:
        try:
            temp = {}
            data = requests.get(f'{base_url}{endpoint}{dd}', headers=headers).json()
            
            # Check if the 'drinks' field is present in the response
            if 'drinks' in data:
                glass = data['drinks'][0].get('strGlass')
                temp['drink'] = dd
                temp['glass'] = glass
                drinks_and_glasses.append(temp)
            else:
                # Case where the drink information is not available
                temp['drink'] = dd
                temp['glass'] = 'N/A'
                drinks_and_glasses.append(temp)
        
        # For any request-related errors
        except requests.RequestException as e:
            
            print(f"Error occurred while requesting data for '{dd}': {str(e)}")
        # For any index-related errors for the distinct drink list
        except (KeyError, IndexError) as e:
            print(f"Error occurred while parsing data for '{dd}': {str(e)}")
        
        time.sleep(0.5)  # added this delay between requests due to API timeout issues I was experiencing
    drinks_and_glasses_df = pd.DataFrame(drinks_and_glasses)
    return drinks_and_glasses_df

def create_poc_table(conn):
    """Creates the proof of concept table

    Args:
        conn (sqlite3.Connection): The connection to the SQLite 'bars_database'
    """
    with open('poc_tables.SQL', 'r') as sql_file:
        poc_table_sql = sql_file.read()
    cursor = conn.cursor()
    cursor.executescript(poc_table_sql)
    conn.commit()

def main():
    """The main function."""
    bar_stock_data = normalize_bar_stock_data('data/bar_data.csv')
    all_transactions_df = create_all_transactions_dataframe()
    # this 'with' statement ensures that the connection isn't interrupted and remains open for the duration of the script's execution
    with create_db_and_tables() as conn:
        distinct_drinks = get_distinct_drinks(conn)
        drinks_and_glasses_df = get_glass_from_drink(distinct_drinks)
        all_transactions_df.to_sql('all_bars_transaction_data', conn, if_exists='replace', index=False)
        drinks_and_glasses_df.to_sql('drinks_and_glasses', conn, if_exists='replace', index=False)
        bar_stock_data.to_sql('bar_stock_data', conn, if_exists='replace', index=False)
        create_poc_table(conn)
    

if __name__ == '__main__':
    main()