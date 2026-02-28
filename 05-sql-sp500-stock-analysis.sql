#Creating Database.
CREATE DATABASE snp_stock;

#Entering Database
USE snp_stock;

#Creating Table.
CREATE TABLE fact_stock_prices (
    trade_date DATE,
    ticker VARCHAR(10),
    open_price DECIMAL(10,2),
    high_price DECIMAL(10,2),
    low_price DECIMAL(10,2),
    close_price DECIMAL(10,2),
    volume BIGINT,
    PRIMARY KEY (trade_date, ticker)
);

#Viewing Table.
SELECT * FROM fact_stock_prices;
SELECT COUNT(*) FROM fact_stock_prices;

#Creating Analytical View.
CREATE VIEW stock_analysis AS
WITH base_data AS (
    SELECT 
        trade_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        (high_price - low_price) AS volatility,
        ((close_price - open_price) / open_price) * 100 AS daily_return_pct
    FROM fact_stock_prices
)

SELECT 
    *,
    AVG(close_price) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_avg_7d,
    
    SUM(daily_return_pct) OVER (
        PARTITION BY ticker 
        ORDER BY trade_date
    ) AS cumulative_return
FROM base_data;
--------------------------------------------------------------------------------------------
#Solving Business Questions.

#Q1: Largest Overall Trading Volume Date.
#1: Aggregate Daily Volume
WITH daily_volume AS (
    SELECT 
        trade_date,
        SUM(volume) AS total_market_volume
    FROM fact_stock_prices
    GROUP BY trade_date
)

SELECT *
FROM daily_volume
ORDER BY total_market_volume DESC
LIMIT 1;

#2: Top 2 Stocks on That Day.
WITH ranked_volume AS (
    SELECT 
        trade_date,
        ticker,
        volume,
        RANK() OVER (
            PARTITION BY trade_date 
            ORDER BY volume DESC
        ) AS volume_rank
    FROM fact_stock_prices
)

SELECT *
FROM ranked_volume
WHERE trade_date = '2015-08-24'
AND volume_rank <= 2;

#Q2: Which Weekday Has Highest/Lowest Volume?
WITH weekday_volume AS (
    SELECT 
        DAYNAME(trade_date) AS weekday,
        AVG(volume) AS avg_volume
    FROM fact_stock_prices
    GROUP BY DAYNAME(trade_date)
)

SELECT *
FROM weekday_volume
ORDER BY avg_volume DESC;

#Q3: AMZN Most Volatile Day
SELECT 
    trade_date,
    volatility,
    RANK() OVER (ORDER BY volatility DESC) AS volatility_rank
FROM stock_analysis
WHERE ticker = 'AMZN'
LIMIT 1;

#Q4: Best Investment
CREATE VIEW vw_stock_summary AS
WITH price_points AS (
    SELECT 
        ticker,
        trade_date,
        close_price,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date
        ) AS first_price,
        LAST_VALUE(close_price) OVER (
            PARTITION BY ticker 
            ORDER BY trade_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_price
    FROM fact_stock_prices
)

SELECT DISTINCT
    ticker,
    first_price,
    last_price,
    ((last_price - first_price)/first_price)*100 AS total_return_pct
FROM price_points
ORDER BY total_return_pct DESC;
