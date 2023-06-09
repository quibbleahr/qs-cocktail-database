# Analytics-engineering test

## To run the script

To run the script, please install the required packages listed in the `requirements.txt` file. You can then run the script by typing `python3 build_database.py` into your terminal window (in the virtual machine you installed the requirements in). 

Please note that I ran into some issues with the `to_sql` function in the `main` function below:

```python
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
```

The issue seems to be that the connection gets interrupted when I first ran the script without the `bars_database.sqlite` file in existence. However, when I ran the script a 2nd time (and therefore the `bars_database.sqlite` file already existed) right after the error, it worked as expected. In debugging this error, it seems this could be due to the Python 3 and sqlite3 versions I am using. However, I have not had enough time to complete the debugging.  


## Some notes

1. I'm assuming all three branch files (London, NY, Budapest) have the same currency since the numbers representing the "amount" are the same for each file (e.g., "Sweet Sangria" is 4.0 in every file). I therefore did not convert any currencies.
2. I am using Python version `3.11.3`
3. Though the "Outputs" requirement below did not mention it, I've included my `requirements.txt` file in the event you would want to run the Python script.
4. I have created the `glasses_used_per_hour` table in my SQLite database as my Proof of Concept table. The idea here is to provide bar staff with a high-level overview of where they ought to focus their restocking on. For example, those in the Budapest branch can immediately see that having only 8 cocktail glasses on December 28th, 2020 would not cover the total of 2780 ordered in that day. As the table shows, on that date they received about 116 orders per hour. So, in order to keep up with this demand, the total time--from the preparation of the cocktail to the customer finishing their drink--that each cocktail glass has to be "in use" would be as follows:

```
Total glasses used: 2780
Number of glasses in stock: 8
Estimated time per glass in use = 24 / (Total glasses used / Number of glasses in stock)
=> Estimated time per glass in use = 0.069 hours
```
Converting this to minutes, the time for each glass to be in use (from the preparation of the cocktail to the customer finishing their drink) in order to cope with the demand would be about 4.13 minutes, which is surely too little time!

## Task Description
### Requirements
- You will need sqlite and Python available to you
- You can expect to spend around 2-3 hours on this task.
    - Don't worry if you cannot finish the exercise in this time, you may submit a description of the next steps you would have taken.

### Client
The client is the owner of a high-end chain of bars. Recently they've experienced complaints about the time it's taken staff to deliver their drinks after they've been ordered.

An initial analysis by the team has shown that this issue is primarily driven by the lack of available glasses for the drinks being ordered. The owners want to take a data driven approach to business decisions in future but they currently have no historical data. They have asked staff to start recording data and we have been provided with an inventory across all three bars as well as the last week of transaction data.

They use the online cocktails database (`https://www.thecocktaildb.com/api.php`) for their menu and all bars serve a subset of these drinks according to the instructions in the database.

The bars are 24hour and for simplicity you can assume they are equally busy around the clock.

### Task
You will need to create a database for the bar owners so that they can begin their data jouney, structuring the tables in a sensible way so that queries can easily be written and potential new data sources could be added. You will need to populate this database with the data they have provided in a way which minimises the overhead as new data is generated by the bars. You will also need to provide a proof of concept table to enable the staff in each bar to make decisions on glass purchases.

Finally please structure the project so that any analysts hired by the bar could easily take over the work from you.

### Outputs
- A SQL file with the CREATE TABLE statements for setting up your database.
- A SQL file with the queries to create a PoC table for the bar staff to manage glass stock.
- A *single* python script which:
    - Reads in the datafiles
    - Imports the relevant data from the cocktails database API
    - Generates the data for the database
    - Creates the database and tables using the data_tables SQL script
    - Imports the data to the database
    - Runs the poc_tables SQL script
- A .sqlite database
- A repository which is in good condition for analysts to take over your work.