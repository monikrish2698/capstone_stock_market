# capstone_stock_market

## Introduction

In the post-COVID-19 era, retail participation in stock markets globally, and specifically in India, has surged significantly. However, this increased participation has revealed concerning trends—up to 95% of retail traders lose money due to impulsive decisions, reliance on unverified tips, and inadequate foundational understanding of financial metrics.

The primary goal of this capstone project is to create an interactive, beginner-friendly dashboard designed specifically to educate novice traders, improving their understanding of stock market fundamentals and reducing uninformed trading practices.

## Rationale for Using U.S. Stock Data

For this Proof of Concept (POC), data from the U.S. stock markets is utilized due to:

- **Availability:** Extensive APIs, particularly from Polygon.io, offer rich, granular historical and near-real-time datasets.
- **Data Richness:** The comprehensive nature of U.S. stock market data, including historical OHLC (Open, High, Low, Close) prices, trading volume data, and corporate actions such as stock splits, exceeds the availability and granularity found in Indian market datasets.
- **Standardization and Documentation:** Well-documented and standardized APIs facilitate initial development and validation.

## Educational Outcome Emphasis

This dashboard explicitly targets novice traders with educational goals:

- **Data Literacy:** Teaching fundamental metrics such as volatility, moving averages, and valuation ratios.
- **Self-awareness & Discipline:** Encouraging users to analyze patterns and historical trends, promoting informed observation over impulsive decisions.
- **Risk Awareness:** Providing clear visualizations and interactive elements that build understanding of market dynamics, risk metrics, and sentiment analysis.
  
It must be emphasized that the platform does not provide direct financial advice nor encourages immediate trading decisions. The objective is purely educational, designed to enhance traders’ analytical skills and knowledge.

## Data Sources

Primary data sources include:
**Polygon.io API:** Offers comprehensive historical data, OHLC, trading volume, splits ratio, and news feeds, crucial for both data analysis and visualization.

## Metrics of Interest

1. Simple Moving Average (SMA)
2. Exponential Moving Average (EMA)
3. Volume Moving Average (VMA)
4. Moving Average Convergence Divergence (MACD)
5. 52-week High/Low Prices
6. Annualized Volatility
7. Stock Sentiment Analysis

## Basic Data Quality Checks

Robust data quality is critical for the dashboard’s educational purpose. The following data quality checks will be rigorously implemented:

1. Completeness Checks: Ensuring all essential fields (OHLC, volume, splits, ticker metadata) are consistently present.
2. Accuracy Checks: Validating data against authoritative benchmarks like official closing prices.
3. Consistency Checks: Confirming temporal data integrity by identifying and rectifying anomalies such as missing or duplicated dates.
4. Timeliness Checks: Ensuring reliable and prompt daily data updates.

## Detailed Technology Stack

**Data Platform:**
Tabular: Optimized analytical data store suitable for efficient querying and scalable analytics.

**ETL and Data Catalog:**
AWS Glue: Enables seamless ETL operations and metadata management using Glue Data Catalog.

**Data Transformation:**
PySpark: Facilitates efficient, scalable data processing, particularly for complex metric calculations like SMA, EMA, and volatility.

**Workflow Orchestration:**
Apache Airflow: Orchestrates data workflows reliably and ensures timely execution.

**Visualization and User Interface:**
Streamlit / Grafana: Provides intuitive, interactive visualizations, enhancing user experience for exploring historical data and metrics.

**Language Model Integration:**
FastAPI and GPT-4o: Powers an interactive, AI-driven chatbot to answer user queries contextually based on historical data.

## Conclusion
By leveraging detailed, comprehensive data and advanced analytical tools, this capstone project aims to bridge the gap between novice retail traders' enthusiasm and the foundational understanding necessary for responsible investing practices. The deliverable—a user-friendly educational dashboard—seeks to significantly enhance the data literacy and market awareness of new traders, ultimately fostering more disciplined and informed participation in financial markets.

