-- This is where you will write the SQL to create the tables needed by the bar staff to assist on restocking decisions
CREATE TABLE IF NOT EXISTS glasses_used_per_hour
AS
WITH transactions_by_glass_drink_and_branch AS (
    SELECT
	    SUBSTR(timestamp, 1, INSTR(timestamp, ' ') - 1) AS date,
	    SUBSTR(timestamp, INSTR(timestamp, ' ') +1) AS timestamp,
	    SUBSTR(SUBSTR(timestamp, INSTR(timestamp, ' ') + 1), 1, INSTR(SUBSTR(timestamp, INSTR(timestamp, ' ') + 1), ':')-1) AS hour,
	    all_bars_transaction_data.drink AS drink_name,
	    LOWER(drinks_and_glasses.glass) AS glass_type,
	    branch
	FROM all_bars_transaction_data LEFT JOIN drinks_and_glasses ON all_bars_transaction_data.drink = drinks_and_glasses.drink
	GROUP BY 1,2,3,4,5,6
),
transactions_by_hour AS (
    SELECT 
	    date AS transaction_date,
	    hour AS transaction_hour,
	    LOWER(glass_type) AS glass_type, 
	    branch,
	    COUNT(drink_name) as glasses_used
	FROM transactions_by_glass_drink_and_branch
	GROUP BY 1,2,3,4
)
SELECT 
	transaction_date, 
    transactions_by_hour.glass_type AS glass_type,
    branch,
    bar_stock_data.stock AS bar_stock,
	SUM(glasses_used) AS glasses_used,
    ROUND(CAST(SUM(glasses_used) AS REAL) / 24.0, 2) AS glasses_used_per_hour
FROM transactions_by_hour LEFT JOIN bar_stock_data ON transactions_by_hour.glass_type = LOWER(bar_stock_data.glass_type) 
AND transactions_by_hour.branch = bar_stock_data.bar
GROUP BY 1,2,3,4
