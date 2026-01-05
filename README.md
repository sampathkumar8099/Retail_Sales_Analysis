🛒 Retail Sales Analytics using PySpark
📌 Project Overview

This project demonstrates an end-to-end retail data analytics pipeline built using PySpark.
The objective is to perform exploratory data analysis (EDA), clean and transform large-scale retail data, extract meaningful business insights, and store analytics-ready datasets in HDFS using Parquet, with Hive external tables for efficient querying.

The project is designed to reflect real-world data engineering and analytics workflows.

📂 Dataset

Source: Kaggle – Brazilian E-commerce Dataset (Olist)

Data Includes:

Customers

Orders

Order items

Products

Sellers

Payments

Reviews

🏗️ Architecture & Flow
CSV Files
   ↓
PySpark (EDA & Cleaning)
   ↓
Joins & Transformations
   ↓
Business Insights
   ↓
Parquet Files (HDFS)
   ↓
Hive External Tables
   ↓
Analytical Queries

🔍 Key EDA & Data Engineering Tasks

Performed schema analysis, row counts, and null value analysis

Cleaned and standardized data (duplicates, missing values)

Analyzed relationships across multiple datasets using joins

Used aggregations and window functions to derive insights

Identified:

Top-performing products and sellers

Revenue trends

Customer purchasing behavior

Payment patterns

Optimized storage by writing curated datasets in Parquet format

Created Hive external tables for easy querying and analysis

📊 Sample Insights

Top-selling products and sellers by revenue

Revenue contribution by product category

Customer order frequency and repeat behavior

Payment method distribution and installment trends

🧰 Technologies Used

PySpark

Python

HDFS

Apache Hive

Parquet

SQL
