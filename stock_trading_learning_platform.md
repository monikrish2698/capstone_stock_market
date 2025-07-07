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

![Screenshot 2025-07-07 at 9 53 38 AM](https://github.com/user-attachments/assets/62be331b-72ba-43a8-a1ad-da072c7d2ecc)

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

**FastAPI**
*Purpose:* Powers the backend API for LLM agent orchestration and user interactions.
*Reason for Choice:* FastAPI provides high-performance, asynchronous API endpoints, making it ideal for serving AI-driven chat features with low latency. Also to simulate a real chat like scenario, FastAPI has streaming features which allows to generate responses and simulataneously show on the streamlit Ui.

## Historical Data Processing

**Initial Data Loading**
* Imported historical OHLCV data from flat CSV files dating back to January 2021.
* Created the core fact tables (e.g., daily prices) using a one time data loader script
* code can be found in : ```include/eczachly/scripts/create_fct_prices_table.py```

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
* code can be found in : ```dags/technical_indicators.py```

![image](https://github.com/user-attachments/assets/87d531d5-e9b4-4f67-a279-39f5593113ca)







