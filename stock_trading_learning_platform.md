# Stock Trading Learning Platform

The surge in retail participation in stock markets globally, particularly in India, during the post-COVID-19 era has been remarkable. While this increased involvement reflects positive investor interest, it has concurrently highlighted critical issues: up to 95% of retail traders experience losses stemming from impulsive decision-making, reliance on unverified tips, and insufficient comprehension of essential financial metrics.

Recognising this gap, the primary objective of this capstone project is to develop an interactive and beginner-friendly dashboard with an AI support. This dashboard also includes an LLM-based chat feature, allowing users to ask questions and receive immediate clarity on various financial topics. This educational tool aims to empower novice traders—including both short-term and long-term traders— by clearly presenting key stock market fundamentals, thus promoting informed decision-making and significantly mitigating uninformed trading practices. Ultimately, this initiative seeks to foster a more knowledgeable and disciplined retail investor community.

## Motivation
As a long-term stocks trader, my personal journey in trading sparked a keen interest in exploring different trading methods. Initially, learning trading concepts seemed overwhelming and confusing, with scattered information from YouTube videos often failing to translate into actionable insights. However, the introduction of Large Language Models (LLMs) transformed my learning experience by simplifying complex concepts with straightforward explanations and relevant practical examples.

This personal experience, combined with the constant need to alternate between platforms like TradingView and ChatGPT during research, became the foundational motivation for this project. Recognising this gap, the primary objective of this capstone project is to develop an interactive and beginner-friendly dashboard enhanced with AI support. The dashboard includes an LLM-based chat feature, enabling users to ask questions and instantly gain clarity on various financial topics. This educational tool aims to empower novice traders—including both short-term and long-term traders—by clearly presenting key stock market fundamentals, thereby promoting informed decision-making and significantly mitigating uninformed trading practices. Ultimately, this initiative seeks to foster a more knowledgeable and disciplined retail investor community.

## Problem Statement
- **Fragmented Learning Resources:** Novice traders frequently struggle with scattered information across multiple platforms, lacking a unified environment to learn stock market fundamentals and apply them practically.

- **Complexity of Technical Indicators:** New traders find core technical indicators—including Simple Moving Averages (SMA), Exponential Moving Averages (EMA), MACD indicators, candlestick charts, and annualized volatility—challenging to interpret and apply effectively without clear, structured visual aids.

- **Delayed Integration of Historical Data and Insights:** Beginners often lack access to simplified visualizations of historical market data that illustrate the practical implications of trading strategies clearly and intuitively.

- **Limited Real-Time Clarification of Doubts:** New traders frequently encounter confusion and uncertainty that remains unaddressed due to limited opportunities for immediate expert assistance or clarifications.

- **Lack of Educational, Risk-Free Environment:** Traders require a purely educational space free from impulsive trading risks; hence, the data presented is strictly historical, up to the previous day's market close, to reinforce informed learning rather than speculative behavior.

## Platform Overview
**Unified Dashboard of Technical Indicators** : Consolidates critical trading metrics in one interactive space, simplifying learning and visualization:

- Simple Moving Averages (SMA) to identify price trends.

- Exponential Moving Averages (EMA) to highlight recent market actions.

- Candlestick Charts depicting daily price movements in detail.

- MACD Indicators providing insights into momentum and trend shifts.

- Annualized Volatility offering clarity on market risks and price fluctuations.

**Integrated LLM-based Chat Feature**

- Enables immediate, practical clarifications through natural language queries.

- Provides concise and beginner-friendly explanations of complex financial concepts.

- Supports interactive learning and continuous trader engagement.

**Educational, Risk-free Environment**

- Focuses exclusively on historical data (up to the previous trading day), reinforcing an educational approach rather than encouraging impulsive trades.

- Empowers traders by bridging knowledge gaps and promoting informed decision-making.

## Data Sources
The platform leverages Polygon API as the primary source for reliable, comprehensive market data, including:

**Daily OHLC Indicators** : Open, High, Low, and Close price data for daily stock analysis.

**Historical CSV Ingestions** : Bulk historical market data ingested from CSV files to provide extensive analytical context.

**News Mentions** : Real-time and historical news references linked directly to stock tickers, enhancing contextual understanding.

**Ticker Details** : Comprehensive information on individual tickers, enabling detailed analysis at the stock level.

**Ticker Overview** : Market Capitalization, Standard Industrial Classification (SIC), and other high-level financial data to assist in fundamental analysis.

**SIC Details** : Detailed industry classification data, supporting deeper sector and industry-level analysis.

These datasets together form a robust foundation for the platform, offering users a detailed, multi-dimensional understanding of stock market fundamentals.

## Architecture
The project is structured around two distinct architectures:

- *Data Flow Architecture*, illustrating data ingestion, processing, and visualization pipelines.
- *LLM Agent Architecture*, depicting the design and workflow of the LLM-powered interactive chat feature.

### Data Flow Architecture

![image](https://github.com/user-attachments/assets/16b567c5-863c-447b-a2c6-fcad226af7f9)

**Ingestion Layer**
- Historical data ingestion from AWS S3 using AWS Glue and PySpark, orchestrated by Apache Airflow.
- Daily batch ingestion managed via AWS Glue with PySpark, also orchestrated using Apache Airflow.

**Database Layer (Medallion Approach)**
- Bronze Layer: Raw data ingestion stored in the monk_data_lake schema, containing tables such as daily stock prices, news, ticker details, ticker types, ticker overview, and SIC descriptions.
- Silver Layer: Refined, structured data stored in the monk_data_warehouse schema, containing fact and dimension tables (fct_daily_stock_prices, fct_daily_news, dim_tickers).
- Gold Layer: Analytical-friendly tables in schema monishk37608, containing derived metrics (e.g., moving averages, MACD crossovers, annualized volatility).

**Visualization Layer**
- Built using Streamlit, DuckDB, and PyIceberg for interactive dashboard visualizations and analytics.
- Enables intuitive visual exploration and querying of stock market data.

This follows the Medallion Architecture, ensuring structured data progression from raw ingestion (bronze) through refined processing (silver) to analytics-ready data (gold).

### LLM Agent Architecture

![image](https://github.com/user-attachments/assets/2a1cba45-85ec-458e-bfe3-12a69a98e6ca)

FastAPI-Based Communication Layer - Provides a robust API to interact with all LLM agents and manage user queries efficiently.

**Intent Detection Agent** <br>
Classifies user queries into categories:
- General Knowledge (GK): Definition of trading terms and technical indicators.
- Market Analysis (MA): Analysis of specific stocks/groups based on dashboard data, market cap, industry, and performance over time.
- Trading Advice (TA): Direct trading recommendations (filtered out to keep platform educational).
- Irrelevant Information (II): Non-equity/trading-related queries. <br>

Supports combined categories (e.g., GK, MA) for nuanced queries.

**Data Retrieval Planner Agent** <br>
Plans and lists all necessary data-fetching steps based on query intent and metadata.

**PrestoSQL Query Builder Agent** <br>
Automatically generates SQL queries to fetch relevant data as defined by the planner. The generated PrestSQL is executed using a trino query engine to fetch the relevant data from the data warehouse and data marts.

**Education Consultant Agent** <br>
Uses the retrieved data to generate educational, explanatory responses. Ensures answers are strictly educational—avoiding profit-focused or speculative advice.

## Technologies Used
The end-to-end flow of the project is built using a comprehensive set of tools and technologies. 

**Apache Spark (with AWS Glue)**
- *Purpose:* Handles both historical and daily batch processing of large datasets, as well as data transformation.
- *Reason for Choice:* Spark offers robust distributed data processing, and AWS Glue manages, scales, and orchestrates ETL workflows with minimal overhead.

**Apache Airflow**
- *Purpose:* Manages, schedules, and monitors complex data pipelines.
- *Reason for Choice:* Airflow’s flexibility and reliability enable seamless workflow orchestration across all stages of the data pipeline.

**Tabular Iceberg**
- *Purpose:* Stores ingested and processed data using the open-source Iceberg table format.
- *Reason for Choice:* Iceberg’s ACID compliance, schema evolution, and partitioning features make it ideal for large-scale, analytical data storage.

**Streamlit**
- *Purpose:* Provides the interactive, visual dashboard for end users.
- *Reason for Choice:* Streamlit allows rapid application development in Python, making it easy to build rich, interactive UIs—including chat interfaces—without frontend expertise.

**PyIceberg + DuckDB**
- *Purpose:* Enables efficient access, filtering, and analysis of large Iceberg tables directly in Python.
* *Reason for Choice:*
  - PyIceberg: Lets you filter and load only necessary data from massive datasets, optimizing memory usage.
  - DuckDB: Allows SQL-based analytics on in-memory data, integrates seamlessly with Python (e.g., Pandas DataFrames), and supports advanced operations like top-N queries, statistical tests, and outlier detection—all without heavy infrastructure. To perform aggrgation at industry level, I used duckdb which allowed simple SQL based aggregation.

**Langchain**
- *Purpose:* Provides a pre-built framework to quickly get started with AI agent development.
- *Reason for Choice:* Langchain allows rapid LLM application development in Python, making it easy to get started with AI agent building.

**FastAPI**
*Purpose:* Powers the backend API for LLM agent orchestration and user interactions.
*Reason for Choice:* FastAPI provides high-performance, asynchronous API endpoints, making it ideal for serving AI-driven chat features with low latency. Also to simulate a real chat like scenario, FastAPI has streaming features which allows to generate responses and simulataneously show on the streamlit Ui.

## Historical Data Processing

**Initial Data Loading**
* Imported historical OHLCV data from flat CSV files dating back to January 2021.
* Created the core fact tables (e.g., daily prices) using a one time data loader script
* code for creating a clean fact table can be found in  in : ```include/eczachly/scripts/create_fct_prices_table.py```

**Simple Moving Averages (SMA)**
* Calculated historical SMA values directly using Trino queries, leveraging SQL window functions for efficient computation over the historical data.

  ```
        create or replace table monishk37608.dm_simple_moving_averages
        WITH (
          format = 'PARQUET',
          partitioning = ARRAY['date']
        ) AS
        WITH base AS (
          SELECT
            ticker,
            date,
            aggregates['close'] as close
          FROM monk_data_warehouse.fct_daily_stock_prices
          where ticker is not null
        ),
        windowed AS (
          SELECT
            ticker,
            date,
            array_agg(close)
          OVER (
            PARTITION BY ticker
            ORDER BY date DESC
            ROWS BETWEEN CURRENT ROW AND 199 FOLLOWING
          ) AS cumulative_200_day_close_price,
            -- 5-day moving average
            avg(close)
              OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
              ) AS cumulative_5_day_ma,
        
            -- 20-day moving average
            avg(close)
              OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
              ) AS cumulative_20_day_ma,
        
            -- 50-day moving average
            avg(close)
              OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
              ) AS cumulative_50_day_ma,
        
            -- 100-day moving average
            avg(close)
              OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
              ) AS cumulative_100_day_ma,
        
            -- 200-day moving average
            avg(close)
              OVER (
                PARTITION BY ticker
                ORDER BY date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
              ) AS cumulative_200_day_ma
        
          FROM base
        )
        SELECT
          ticker,
          date,
          cumulative_200_day_close_price,
          cumulative_5_day_ma,
          cumulative_20_day_ma,
          cumulative_50_day_ma,
          cumulative_100_day_ma,
          cumulative_200_day_ma
        FROM windowed
        ORDER BY ticker, date desc
  ```
**Complex Technical Indicators**
* For more advanced indicators—Exponential Moving Average (EMA), Annualized Volatility, and MACD—direct SQL approaches proved insufficient due to the complexity of recursive calculations and Starburst’s query limitations.
* Addressed this by developing a dedicated Airflow DAG, processing data from April 1, 2025, to June 30, 2025.
* This approach enabled robust, scalable computation of advanced indicators over a three-month window, ensuring accuracy and reliability for downstream analytics and visualization.

<img width="648" alt="Screenshot 2025-07-07 at 12 27 17 PM" src="https://github.com/user-attachments/assets/996adace-9b78-45ed-9ff3-fb64b093993d" />

*figure desc: code snippet of data ingestion*

code for the historical ingestion can be found in : ```include/eczachly/scripts/historical/ingest_polygon_daily_aggregate_historical.py```

code for the dag can be found in : ```dags/technical_indicators.py```

![image](https://github.com/user-attachments/assets/87d531d5-e9b4-4f67-a279-39f5593113ca) 

*figure desc: Historical technical indicator successful DAG runs*


![image](https://github.com/user-attachments/assets/51542ad0-b33d-4644-9550-627a83e6f8cd) 

*figure desc: Historical technical indicator DAG graph*

## Daily Data Processing

### Overview 
Daily data is fetched using Polygon's daily aggregate API which provides the daily OLHCV data using an API endpoint. Daily news articles are also fetched using the same method. 

![image](https://github.com/user-attachments/assets/fb4ca4a2-930f-4319-a35e-9a348865f723)
*figure desc: Daily DAG run of stocks*
code for the daily stock DAG can be found in : ```dags/daily_run_dag.py```

![image](https://github.com/user-attachments/assets/9cc8afa6-d9ce-463f-b539-26a4e0e8465f)
*figure desc: Daily DAG run of news*
code for the DAG can be found in : ```dags/fetch_daily_news.py```

The daily stock data DAG is scheduled to run exclusively on weekdays, since markets are closed on weekends. To account for public `holidays`, the holidays Python package is used to identify non-trading days. A custom macro ensures that the correct date from the last successful DAG run is selected, with logic to differentiate between skipped and successful runs. Existing macros are leveraged to retrieve the last skipped DAG date when building cumulative tables. Additionally, the DAGs are configured with retry logic to handle failures gracefully.

![image](https://github.com/user-attachments/assets/26a262cd-0127-4a3e-aaa9-44ba8321fa53)

*figure desc: logic to check if the previous run date* 

![image](https://github.com/user-attachments/assets/082f8a36-698e-4724-a0f2-c950b340e396)

*figure desc: DAG level configurations*

### Data Quality Checks
To ensure that errorneous data is not ingested in the data warehouse, comprehensive and elegant test cases are implemented.

![image](https://github.com/user-attachments/assets/a4956d7a-228e-4361-b1c2-957ca8ed067d)

*figure desc: Data Quality Checks implemented*

*1. Check for Data Presence (there_is_data)*
  - Why it’s necessary: Ensures that the input table actually contains data for the specified run date. Loading empty or missing data into the warehouse could result in misleading reports, incomplete analytics, or system errors in downstream processes.
* Impact if skipped:
  - Downstream consumers may be unaware that data is missing, which can lead to faulty insights and erode trust in the data platform.

*2. Deduplication Check (deduplicated)*
* Why it’s necessary:
  - Confirms that there is exactly one unique record per ticker for the specified date. Duplicated records inflate metrics, skew technical indicators, and compromise all downstream aggregations and analyses.
* Impact if skipped:
  - Can lead to data inflation, misleading trend analyses, and flawed decision-making—especially in financial analytics where each record must uniquely represent a security on a given date.

*3. Negative Price Check (no_negative_prices)*
* Why it’s necessary:
  - Ensures that no negative values exist in the core price fields (open, high, low, close). Stock prices cannot be negative; negative values typically indicate corruption, ingestion issues, or bugs in upstream data sources.
* Impact if skipped:
  - Negative prices will invalidate all technical calculations (e.g., moving averages, volatility), can trigger system errors, and may propagate erroneous data to end-users.

*4. Null Price Check (no_null_prices)*
* Why it’s necessary:
  - Verifies that all essential price fields are present and non-null. Nulls in these columns prevent reliable calculation of indicators, may cause exceptions in analytics pipelines, and leave dashboards incomplete.
* Impact if skipped:
  - Nulls can cause runtime errors, block reporting, result in incomplete data visualizations, and ultimately diminish user confidence in the platform.

### Write-Audit-Publish Pattern 
To ensure data reliability and enable safe, atomic updates, Write Audit Publish (WAP) pattern is implemented with Iceberg’s branching strategy. This approach allows new data to be written and thoroughly validated on an isolated branch before being published to the main production table. As a result, only data that passes all quality and audit checks is merged, significantly reducing the risk of data corruption or incomplete ingestions in the warehouse.

**PR:** https://github.com/DataExpert-io/airflow-dbt-project/pull/264

## Technical Indicators
This project emphasizes five foundational technical indicators, each serving as a building block for traders aiming to develop a strong understanding of market dynamics before engaging in active trading:

**1. Candlestick Charts**
- Visualize daily price action through open, high, low, and close values.
- Help traders quickly identify patterns and sentiment shifts in the market.

**2. Simple Moving Average (SMA)**
- Smooths out price data to highlight longer-term trends.
- Useful for identifying support and resistance levels, trend direction, and potential entry/exit points.

**3. Exponential Moving Average (EMA)**
- Gives more weight to recent prices, responding more quickly to new information.
- Helps traders catch trend changes earlier than the SMA.

**4. MACD (Moving Average Convergence Divergence) Indicator**
- Combines EMAs to measure momentum and trend strength.
- The MACD line and its signal line help in spotting bullish/bearish crossovers and divergences.

**5. Volatility**
- Quantifies how much a stock's price fluctuates over an observable period of time.
- Helps traders assess risk and determine appropriate position sizing and stop-loss levels.

### Using Indicators in Combination
**1. Candlesticks + Moving Averages:**
- Overlaying SMA or EMA on candlestick charts enables traders to visualize trend strength in the context of real price movements. This helps in filtering out noise and confirming trend directions, making it easier to spot reversals or continuation patterns.

![image](https://github.com/user-attachments/assets/b75c1299-9fe7-42c2-b63a-b8d8d28f52ad)
*figure desc: Chart depicting candlesticks + SMA*

**2. Candlesticks + EMA + MACD:**
- Combining candlesticks with EMA and the MACD indicator allows for a nuanced view of both price action and momentum. For example, when candlestick patterns suggest a reversal and the MACD shows a bullish crossover above the signal line, traders gain stronger confirmation for their decisions.
- Some of the most commonly used EMAs are 12 day and 26 day EMA. As it rightly captures the trends.
- The MACD Indicators chart also highlights the Bullish and Bearish signal lines

![image](https://github.com/user-attachments/assets/8e314e02-f38d-4000-ad03-95eed09968dd)
**figure desc: Chart depicting candlesticks + EMA**

![image](https://github.com/user-attachments/assets/4449ac9e-98ca-47bf-888d-4836bb3b5fcf)
*figure desc: Chart depicting MACD Indicators*

**3. Annualised Volatlity:**
- While annualized volatility is commonly applied in derivative trading, it remains an important metric for stock traders as well.
- Incorporating volatility alongside other indicators enables traders to factor in risk, rather than relying solely on price trends. For example, a strong upward trend accompanied by high volatility may signal the need for increased caution or smaller trade sizes.
- In stock trading, volatility is calculated as the standard deviation of returns over a given period (7-day and 30-day windows in this project), and then annualized by multiplying by the square root of 252.
- When you calculate volatility over a shorter window (like 7 or 30 days), that standard deviation is on a daily scale. The number 252 represents the average number of trading days in a year (since stock markets are generally open around 252 days annually).
- The volatility chart in this dashboard is designed to present volatility at the industry level, rather than for individual tickers. This approach offers a broader perspective on sector-wide performance and risk, helping users understand market dynamics beyond single stocks. 

![image](https://github.com/user-attachments/assets/9f9df8ea-e8c3-49ed-9cbc-8c20574020f8)
*figure desc: Chart depicting Annualised Volatlity*






















