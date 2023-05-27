# Analytics-engineering test

## Notes

1. I'm assuming all three branch files (London, NY, Budapest) have the same currency since the numbers representing the "amount" are the same for each file (e.g., "Sweet Sangria" is 4.0 in every file). I therefore did not convert any currencies.
2. I have created the `glasses_used_per_hour` table in my SQLite database as my Proof of Concept table. The idea here is to provide bar staff with a high-level overview of where they ought to focus their restocking on. For example, those in the Budapest branch can immediately see that the having only 8 cocktail glasses on December 28th, 2020 would not cover the total of 2780 ordered in that day. As the table shows, on that date they recieved about 116 orders per hour. So, in order to keep up with this demand, the total time--from preparation of the cocktail to the customer finishing their drink--that each cocktail glass has to be "in use" would be as follows:

```
Total glasses used: 2780
Number of glasses in stock: 8
Estimated time per glass in use = 24 / (Total glasses used / Number of glasses in stock)
=> Estimated time per glass in use = 0.069 hours
```
Converting this to minutes, the time for each glass to be in use (from preparation of the cocktail to the customer finishing their drink) in order to cope with the demand would be about 4.13 minutes, which is surely too little time!

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