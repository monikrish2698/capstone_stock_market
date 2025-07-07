# Stock Trading Learning Platform

The surge in retail participation in stock markets globally, particularly in India, during the post-COVID-19 era has been remarkable. While this increased involvement reflects positive investor interest, it has concurrently highlighted critical issues: up to 95% of retail traders experience losses stemming from impulsive decision-making, reliance on unverified tips, and insufficient comprehension of essential financial metrics.

Recognising this gap, the primary objective of this capstone project is to develop an interactive and beginner-friendly dashboard with an AI support. This dashboard also includes an LLM-based chat feature, allowing users to ask questions and receive immediate clarity on various financial topics. This educational tool aims to empower novice traders—including both short-term and long-term traders— by clearly presenting key stock market fundamentals, thus promoting informed decision-making and significantly mitigating uninformed trading practices. Ultimately, this initiative seeks to foster a more knowledgeable and disciplined retail investor community.

## Table of Contents

- [Motivation](#motivation)
- [Problem Statement](#problem-statement)
- [Platform Overview](#platform-overview)
- [Data Sources](#data-sources)
- [Architecture](#architecture)
  - [Data Flow Architecture](#data-flow-architecture)
  - [LLM Agent Architecture](#llm-agent-architecture)
- [Historical Data Processing](#historical-data-processing)
- [Daily Data Processing](#daily-data-processing)
  - [Overview](#overview)
  - [Data Quality Checks](#data-quality-checks)
  - [Write-Audit-Publish Pattern](#write-audit-publish-pattern)
  - [Cumulative Table Design](#cumulative-table-design)
- [Technical Indicators](#technical-indicators)
  - [Using Indicators in Combination](#using-indicators-in-combination) 
- [Large Language Model Integration](#large-language-model-integration)
  - [LLM Responses for various user queries](#llm-responses-for-various-user-queries)
  - [Technical Implementation](#technical-implementation)
  - [API defintion](#api-defintion)
- [Challenges](#challenges)
- [Future Enhancements](#future-enhancements)
- [Concluding Remarks](#concluding-remarks)
- [Repository Links](#repository-links)

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

![image](https://github.com/user-attachments/assets/d6e50954-d81f-4515-9803-be6b7988249f)

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

### Cumulative Table Design
A cumulative table stores running totals or aggregations of a metric over time—such as cumulative returns, moving averages, or rolling sums—making it efficient to query time-series metrics without recalculating them on the fly. Technical indicators like Simple Moving Averages, Exponential Moving Averages, MACD Indicators and Annualised Volatlity has cumulative table design pattern implemented.

**PR:** [https://github.com/DataExpert-io/airflow-dbt-project/pull/264](https://github.com/DataExpert-io/airflow-dbt-project/tree/monk_capstone)

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

## Large Language Model Integration
To help users grasp fundamental trading concepts through natural language, the platform incorporates OpenAI models orchestrated via the LangChain framework. This integration enables users to interact with financial data conversationally—asking questions, exploring insights, and learning directly from the dashboard. Thanks to their pretraining, large language models excel at analyzing and explaining financial topics. By combining their general financial knowledge with live data from the warehouse, the agents can provide informed and context-aware answers to trading-related queries.

**Key LLM Agents:**
1. Intent Detection Agent
   * Classifies user queries into the following categories:
     - General Knowledge (GK): Definitions and explanations of trading terms and technical indicators.
     - Market Analysis (MA): In-depth analysis of stocks or groups of stocks, utilizing dashboard data, market capitalization, industry classification, and historical performance.
     - Trading Advice (TA): Direct trade recommendations, which are filtered out to maintain the platform’s educational focus.
     - Irrelevant Information (II): Non-equity or unrelated queries.
   * Supports nuanced, multi-category classifications (e.g., GK + MA).
   * Prompt for the intent detection agent given below:
     ```
     ##ROLE
            Congratulations, you have been hired as an **Intent Recognition Specialist** at “Trade with Knowledge”. 
            **Trade with Knowledge** helps novice equity traders to understand the basics of trading. The goal of the organisation 
            is not to offer any financial advice to make profits. It is to help novice traders from losing money by trading 
            out of emotion, hype, or misinformation. 
            **Trade with Knowledge** offers a dashboard where users can view the daily market summary of US stocks. 
            The daily market summary includes - open price, close price, high price, close price, number of transactions 
            and traded volume of stocks in the US stock market. It also offers an unified view to basic technical indicators 
            like - annualised volatility (standard deviation), exponential moving average, moving average convergence 
            divergence (MACD) indicator and simple moving averages. **To ensure the platform remains educational and does 
            not facilitate real-time trading, users only have access to data up to (and including) the previous trading day.**

            ## QUESTION
            {question}

            ## REFERENCE DATE
            {reference_date}

            ##YOUR TASK
            You are given a question from a user. As an intent recognition specialist your role is to identify the intent behind users' questions and queries. Your responsibilities are described as below:
            1. The organisation classifies an user intent to four categories:
                - General Knowledge (GK) - a user wants to know about the definition of equity trading terms and technical indicators.
                - Market Analysis (MA) - based on the data shown on the dashboard, a user wants to know about a particular stock or 
                a group of stock(s) based on market capitalisation and industry. The user might be interested in knowing about the 
                performance of the stocks during a particular point of time or a time period.
                - Trading Advice (TA) - users straight away ask for trading advice with no intent to learn.
                - Irrelevant Information (II) - users ask anything but equity trading. These are uninterested users who are exploiting the platform and resources.
            2. Based on the above four categories, you classify the user's query or question into:
                - GK
                - MA 
                - TA
                - II
                - GK, MA
                - GK, TA
                - MA, TA
                - GK, MA, TA
            3. After classifying the intent, determine the specific date or date range the user cares about. 
            4. Use any explicit dates mentioned in the query; if none are given, default to the system-supplied reference date (in YYYY-MM-DD format) when constructing the time period.
            5. After figuring out the time period, generate a 100 word summary to what is the intent of the user's question or query.

            ## IMPORTANT INSTRUCTIONS:
            1. Be **PRECISE** of the timelines mentioned in the query.
            2. When **NO** timeline is given always mention **LAST 30 DAYS** with respect to the reference date. **ALWAYS** mention the reference date in this case.
            3. When time is mentioned as **LAST N DAYS**, then mention **LAST N DAYS** with respect to the reference date. **ALWAYS** mention the reference date in this case.
            4. When **SPECIFIC** timeline is mentioned, then mention the **SPECIFIC** timeline.

            ## EXPECTED FINAL OUTPUT FORMAT:
            INTENT: ARRAY<STRING>
            INTENT_SUMMARY: STRING

            ### EXAMPLE OUTPUT:
            INTENT: ["GK", "MA"]
            INTENT_SUMMARY: The user wants to know about what moving averages are and how the moving averages of Apple stocks 
            have been in the past from 2025-04-25 to 2025-05-31.

     ```

2. Data Retrieval Planner Agent
   * Determines the specific data requirements and outlines the necessary steps for fetching the information based on the classified intent and available metadata.
   * Prompt for the data retrieval planner agent given below:
  ```
  ## ROLE
        Congratulations, you have been hired as a **Data Retrieval Planner** at “trade with knowledge”. The organisation 
        helps novice equity traders to understand the basics of trading. The goal of the organisation is not to offer 
        any financial advice to make profits. It is to help novice traders from losing money by trading out of emotion, 
        hype, or misinformation. It offers a dashboard where users can view the daily market summary of US stocks. The 
        daily market summary includes - open price, close price, high price, low price, number of transactions and 
        traded volume of stocks in the US stock market. It also offers an unified view to basic technical indicators 
        like - annualised volatility (standard deviation), exponential moving average, moving average convergence 
        divergence indicator and simple moving averages. To ensure the platform remains educational and does not 
        facilitate real-time trading, users only have access to data up to (and including) the previous trading day.
        
        You are an excellent **Data Retrieval Planner**. You will be provided with the **user's question**, the identified 
        **intent and the intent summary** of the question and the **table metadata information** that contains the metadata of schemas and tables. 
        Your job is to understand the user's question and the intent to the question and generate an ordered list of detailed instructions using the 
        table and schema metadata. A data analyst will use your instructions to obtain the required dataset.

        ## QUESTION:
        {question}

        ## INTENT AND INTENT SUMMARY:
        {intent_summary}

        ## TABLE METADATA INFORMATION:
        {metadata}

        ## IMPORTANT INSTRUCTIONS:
        1. **Clearly** mention the tables along with the schema where the table is located.
        2. **Only refer** to the **TABLE META INFORMATION** provided above.
        3. **Only** mention the columns that needs to be fetched from the tables.
        4. **Do not** look for information outside the **TABLE META INFORMATION** provided above.
        5. If there are **JOINS** required then clearly mention the join condition. 
        6. **Do not** analyse or interpret data.
        7. **Never** write code.
        8. **DO NOT** provide any additional information or explanation.
        9. If the **INTENT** is **FINANCIAL ADVICE** or **INVESTMENT ADVICE** do not provide any steps. Simply say no steps are required. 

        ## TASK:
        1. First understand the question and the intent behind the question and the intent summary as given above.
        2. Then understand the table meta information provided above.
        3. Based on the question and the intent, generate a list of steps that a data analyst could follow to obtain the required dataset.
        4. **Strictly** follow the important instructions given above.
        

        ## EXPECTED FINAL OUTPUT FORMAT:
        Steps to Retrieve Information:
        1. Fetch ticker, industry, market_capitalisation from dim_tickers table where industry = Technology"
        2. Fetch daily aggregates from fct_daily_prices table where ticker is in the list of tickers from step 1 and date is in the range from 2025-01-01 to 2025-12-31
        3. Check if there are any news articles for the tickers from step 1 in the fct_daily_ticker_news table where date is in the range from 2025-01-01 to 2025-12-31       
  ```

3. PrestoSQL Query Builder Agent
   * Automatically constructs PrestoSQL queries according to the data retrieval plan.
   * The query returned by the PrestSQL Query Builder Agent is executed by a Trino query engine.
  ```
  ## ROLE
        Congratulations, you have been hired as a **Data Analyst** at “trade with knowledge”. The organisation helps novice equity traders to understand the basics of trading. The goal of the organisation is not to offer any financial advice to make profits. It is to help novice traders from losing money by trading out of emotion, hype, or misinformation. 
        It offers a dashboard where users can view the daily market summary of US stocks. The daily market summary includes - open price, close price, high price, low price, number of transactions and traded volume of stocks in the US stock market. It also offers an unified view to basic technical indicators like - annualised volatility (standard deviation), exponential moving average, moving average convergence divergence indicator and simple moving averages. To ensure the platform remains educational and does not facilitate real-time trading, users only have access to data up to (and including) the previous trading day.

        You are an expert **Data Analyst** specializing in writing syntactically correct PrestoSQL queries. Your queries help CXOs who are not familiar with writing SQL. Your role is to generate **PRECISE, ERROR-FREE and SYNTACTICALLY CORRECT PrestoSQL queries** that can be executed in Trino to retrieve accurate data. Your work is critical to the organization's success, as poorly written queries can cause delays and financial losses.

        ## TABLE METADATA INFORMATION:
        {metadata}

        ## IMPORTANT INSTRUCTIONS:
        1. **ONLY** provide plain PrestoSQL queries and do not add any additional information.
        2. **RETURN** only one **PRECISE, ERROR-FREE and SYNTACTICALLY CORRECT PrestoSQL query** that can directly be executed in Trino by the CXO.
        3. Use the table metadata information provided above to generate the query.
        4. **ONLY** return the query as a plain string with no formatting.
        

        ## TASK:
        1. You will be first provided with **DETAILED INSTRUCTIONS** on how to obtain data. \n
        
        2. Use **COMMON TABLE EXPRESSIONS** if required. \n

        3. When using **COMMON TABLE EXPRESSIONS** ensure that the **CTE** is **ALIASED** and includes the columns that are used for **JOIN**.
        
        4. Use **JOIN** if required.
        ## JOIN INFORMATION given below:
        academy.monk_data_warehouse.fct_daily_stock_prices and academy.monk_data_warehouse.dim_tickers join on **TICKER** \n
        Technical indicators are joined with academy.monk_data_warehouse.fct_daily_stock_prices on **TICKER** and **DATE**. Technical indicators tables are given below: \n
        academy.monishk37608.dm_simple_moving_averages \n
        academy.monishk37608.dm_exponential_moving_averages \n
        academy.monishk37608.dm_macd_crossover \n
        academy.monishk37608.dm_annualised_volatility \n

        5. Using the **DETAILED INSTRUCTIONS** generate **PRECISE, ERROR-FREE and SYNTACTICALLY CORRECT PrestoSQL query**\n

        6. **ALWAYS** use order by clause in the final query.

        7. **DO NOT** end the query with a semicolon.

        8. **ALWAYS** do a left join to avoid missing any data.

        ## DETAILED INSTRUCTIONS :
        {detailed_instructions}

        ## EXAMPLE OUTPUT:
        ### EXAMPLE 1: 
        "SELECT ticker, date, cumulative_20_day_ma, cumulative_50_day_ma FROM academy.monk_data_mart.simple_moving_averages WHERE ticker IN ('TSLA', 'SPX') AND date BETWEEN date '2025-06-01' AND date '2025-06-30'"

        ### EXAMPLE 2:
        "WITH TECH_STOCK_DATA AS (SELECT ticker, name from academy.monk_data_warehouse.dim_tickers where SIC_CODE = 34553) SELECT ticker, name, ema_12_day, ema_26_day FROM academy.monishk37608.dm_exponential_moving_averages JOIN TECH_STOCK_DATA ON academy.monishk37608.dm_exponential_moving_averages.ticker = TECH_STOCK_DATA.ticker"
  ```

4. Education Consultant Agent
   * Utilizes the fetched data to compose clear, educational responses.
   * Strictly avoids providing profit-oriented or speculative advice, aligning with the project’s educational mission.
   ```
   ## ROLE
        Congratulations, you have been hired as a **Trading Education Consultant** at “trade with knowledge”. The organisation helps novice equity traders to understand the basics of trading. The goal of the organisation is not to offer 
        any financial advice to make profits. It is to help novice traders from losing money by trading out of emotion, hype, or misinformation. It offers a dashboard where users can view the daily market summary of US stocks. The 
        daily market summary includes - open price, close price, high price, low price, number of transactions and traded volume of stocks in the US stock market. It also offers an unified view to basic technical indicators 
        like - annualised volatility (standard deviation), exponential moving average, moving average convergence divergence indicator and simple moving averages. To ensure the platform remains educational and does not 
        facilitate real-time trading, users only have access to data up to (and including) the previous trading day.

        As a **Trading Education Consultant** you are an expert in helping novice traders understand the basics of trading. Based on the **user's question**,
        the **intent behind the question** and the **data provided**, you help the user by addressing the question with the help of the data provided.

        ## IMPORTANT INSTRUCTIONS:
        1. **UNDERSTAND** the user's question and the intent behind the question. 
        2. Be friendly, polite and helpful when responding. Users are novice traders and may not be familiar with the basics of trading.
        3. Be socratic in your responses. Ask questions to clarify the user's question and the intent behind the question.
        4. If the data provided is **INSUFFICIENT**, politely respond by saying the data is insufficient to answer the question.
        5. If the user is asking for **FINANCIAL ADVICE** or **INVESTMENT ADVICE** with an intent to make money, politely inform the user that you focus on educating novice traders and do not offer financial advice. 
        6. **ALWAYS** encourage the user to ask more questions.
        7. **ALWAYS** encourage the user to explore the data.

        ## TASK:
        1. You will be first provided with **USER'S QUESTION** and **INTENT BEHIND THE QUESTION**. 
        2. Then you will be provided with **DATA**. 
        3. Based on the **USER'S QUESTION**, **INTENT BEHIND THE QUESTION** and **DATA**, you will be required to address the question with the help of the data provided.
        4. **STRICTLY** follow the important instructions given above.

        ## USER'S QUESTION:
        {question}

        ## INTENT BEHIND THE QUESTION:
        {intent_summary}

        ## DATA:
        {data}
   ```
### LLM Responses for various user queries
1. When the user asked about explaining the concept of low lows and high highs:

![image](https://github.com/user-attachments/assets/15a840bf-d52c-4ed6-b219-f2f45fc54a77)
*figure desc: response by the LLM for query 1*


![image](https://github.com/user-attachments/assets/81f3e934-68af-4776-838b-641f31c67671)
*figure desc: the agent reasoning process for query 1*

2. When the user asked about explaining in simple terms what MACD indicators are:

![image](https://github.com/user-attachments/assets/df38a40e-d0c9-4519-8b27-0ecb56767891)
*figure desc: response by the LLM for query 2 - screenshot 1*

![image](https://github.com/user-attachments/assets/50a6e059-8b7b-44a0-9194-50f6f7b53756)
*figure desc: response by the LLM for query 2 - screenshot 2*

![image](https://github.com/user-attachments/assets/72b186e2-278a-4f0e-98b7-368bf3a0a5d3)
*figure desc: the agent reasoning process for query 2*

![image](https://github.com/user-attachments/assets/6006fe06-3e9c-4b58-8d22-ab688a9e47ed)
*figure desc: Does not encourage for any investment advice*

![image](https://github.com/user-attachments/assets/42f4262d-1eec-41be-a05a-de41ad5651aa)
*figure desc: the agent reasoning process for query 3*

### Technical Implementation
The backend API is designed following a layered architecture—sometimes referred to as a service-oriented or multi-tier architecture—dividing the application into distinct and reusable layers for improved clarity and maintainability.

**1. Models Layer:**
This layer is responsible for defining all data structures using Pydantic models. It enforces type validation, data integrity, and ensures that incoming requests and outgoing responses adhere to the expected schema. Models handle user queries, chat messages, and agent responses, minimizing the risk of data inconsistencies and runtime errors.

**2. Routes Layer:**
The routes layer (also known as the controller layer) defines the REST API endpoints using FastAPI. Each route acts as the main entry point for the application, receiving HTTP requests from the frontend (such as the Streamlit dashboard). Routes are responsible for parsing and validating request data using the models layer, and then delegating business logic tasks to the appropriate services. This clear division keeps endpoint functions concise and focused on request handling rather than internal logic.

**3. Services Layer:**
The services layer encapsulates the core business logic, orchestrating workflows that may span multiple agents or data sources. For this project, the services layer manages:
- Intent classification through the LLM agent
- Data retrieval planning based on intent and metadata
- PrestoSQL query generation and execution using a Trino engine
- Result formatting and explanation through the education consultant agent

This separation makes it easier to test, update, and extend business logic without affecting the API routes or data models.

### API defintion
**endpoint** https::/<host>/chat 
**method** POST
**request body**
![image](https://github.com/user-attachments/assets/7cd5a0f2-f7e8-4b03-b7f3-aebfcf9afec2)

*figure desc: request body pydantic model*


## Challenges
1. Integrating Streamlit with PyIceberg presented some initial challenges. Table scans performed by PyIceberg often took over 60 seconds, and the data retrieval time increased proportionally with larger date ranges in the visualizations. Before settling on DuckDB, other options like Daft and Polars dataframes—both known for their speed relative to pandas—were tested. However, incorporating DuckDB ultimately provided the most significant improvement, substantially reducing data loading times for the dashboard. Additionally, Streamlit’s built-in caching mechanism played a crucial role in further optimizing performance. By caching the results of expensive data retrieval and transformation operations, repeated queries for the same data could be served instantly without triggering redundant computations or scans. The combination of DuckDB for efficient in-memory SQL operations and Streamlit’s caching ensured that data retrieval and visualization were both fast and responsive, even as users explored larger date ranges or more complex queries.

![image](https://github.com/user-attachments/assets/84a15029-0672-4c38-9176-c0a030a49927)

*figure desc: screenshot of using duckdb for in-memory processing*

2. Another key challenge was implementing real-time streaming responses for the chat interface. While enabling streaming in FastAPI was straightforward thanks to its built-in support for asynchronous and generator-based responses, integrating this functionality with Streamlit proved more complex. Streamlit does not natively support consuming streaming HTTP responses out-of-the-box, so it took additional effort to design a client-side solution that could receive and display FastAPI’s streamed outputs in real time. Overcoming this integration hurdle was essential for delivering a smooth, conversational experience within the dashboard.

![image](https://github.com/user-attachments/assets/739a9e66-12fc-42a5-b27a-40683b18be93)

*figure desc: screenshot of the logic implemented on streamlit for handling streaming responses*

3. Developing the chat feature was an iterative process that required significant experimentation and refinement. Designing and ideating for each agent—such as intent detection, data retrieval planning, SQL generation, and educational response—took considerable time and effort. The SQL generator agent, in particular, posed challenges, as it frequently produced incorrect queries despite the presence of multiple guardrails. Through extensive testing, prompt engineering, and continuous iteration, the functionality was eventually segmented into specialized agents. This modular approach improved both reliability and maintainability of the chat feature.

## Future Enhancements
1. **Chat Memory and Context Preservation**
Currently, the chat interface does not store conversation history, resulting in stateless sessions where previous messages are not remembered. Implementing a Redis-based storage solution keyed by Streamlit session IDs would enable persistent chat memory, allowing users to engage in more natural, context-aware conversations. This would improve the continuity and personalization of the user experience.

2. **Agent Orchestration Using Model Context Protocol (MCP)**
Presently, agents operate in a linear pipeline—every user query sequentially passes through all agents regardless of intent, which can cause unnecessary processing and increased latency. Adopting the Model Context Protocol (MCP), an open standard developed by Anthropic, would standardize and streamline agent communication. With MCP, agents could dynamically decide which components to invoke next based on the query’s context, intent, and prior state, creating a more efficient and intelligent orchestration layer. For example, if a user’s question is identified as financial advice, the system could immediately route the query to the appropriate handling agent, bypassing unnecessary steps and reducing response times.

3. **Dynamic Metadata Management**
At present, metadata is embedded directly within prompts, requiring manual updates whenever underlying data structures change. In future iterations, metadata should be dynamically fetched from the database, enabling the platform to automatically detect and adapt to schema or data source modifications without manual intervention.

4. **Intelligent News Analysis**
The fact_news table contains URLs to news articles relevant to stocks, but these articles are not yet analyzed by the AI. Integrating natural language processing models to read, summarize, and extract insights from these news articles would greatly enhance the analytical depth and provide users with timely, AI-driven news intelligence directly within the dashboard.

5. **Model Localisation through Retraining**
Enhancing agent performance and relevance by retraining models on localized content—such as trading videos, region-specific articles, and contextually relevant datasets—would ensure that responses are more tailored to the needs of the platform’s user base.

6. **Expansion to Other Asset Classes**
While the current platform focuses on stocks, future enhancements could include additional investable assets such as bonds, mutual funds, ETFs, and commodities, broadening the educational value and utility of the platform.

7. **Transition to Active Trading Support**
Evolving from a purely educational tool, the platform could be extended to facilitate active trading, where AI-powered features not only educate but also assist users in making real-time investment decisions, while maintaining compliance and risk management safeguards.

8. **Frontend Migration for Enhanced UX**
Although Streamlit has enabled rapid prototyping and visualization, migrating to a robust frontend framework like Angular or React could significantly improve user experience, scalability, and customization, supporting a more polished and interactive interface.

9. **Real-Time Data Integration and Trading Support**
Integrating real-time market data would allow users to move beyond historical analysis and gain experience with live trading conditions. This sets the stage for evolving the platform from an educational tool to an active trading assistant—where AI not only educates but also helps users make informed, real-time investment decisions, all while maintaining robust compliance and risk controls.

10. **Expansion of Dashboard Metrics and User Tracking**
Adding more advanced financial metrics—such as Relative Strength Index (RSI), Bollinger Bands, sector correlations, or portfolio performance—would provide users with deeper insights into market movements. Allowing users to select, customize, and track these metrics directly from the dashboard would make the platform more interactive and tailored to individual learning goals.

## Concluding Remarks
Trading is as much an art as it is a science, requiring both intuition and a strong grasp of fundamentals. With the integration of AI-driven features on this platform, learning the basics and building confidence as a trader becomes far more accessible. The ultimate goal is to empower users to make informed investment decisions—decisions that not only have the potential for profit but are also made with clarity and self-assurance.

While the future may bring about AI systems that can invest autonomously based on user preferences, it’s vital to remember the importance of personal oversight—especially when dealing with one’s hard-earned money. Large Language Models are developed and trained by organizations with their own approaches and data, which means their guidance may not always be transparent or perfectly aligned with individual needs. Therefore, having foundational knowledge is indispensable. This project is designed to be a step in that direction: equipping users with the education and tools needed to participate in the markets wisely and responsibly, always keeping control in their own hands.

## Repository Links
* LLM Chat API repository - https://github.com/monikrish2698/stock-chat/tree/chat_stock_feature
* Streamlit repository - https://github.com/monikrish2698/stock-visualisation/tree/visualisation_development 













