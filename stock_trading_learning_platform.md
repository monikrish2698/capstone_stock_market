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


