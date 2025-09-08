# Automation script for creating vendor summary table in database

# Importing required modules
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time
from automate_ingestion import ingest_db
import sqlite3


# Creating vendor summary table
def create_vendor_summary_table(database):
    """Merge tables (Purchase transactions made by vendors, Sales transaction data and Freight costs for each vendor) to create vendor summary table and returns the created table"""

    vendor_sales_summary = pd.read_sql_query("""
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC""", database)

    return vendor_sales_summary



# Data Cleaning
def data_cleaning(df):
    """Cleans the vendor summary table data by eliminating empty values, replacing whitespaces and assigning correct format to the tables"""

    # Replaces missing values with 0
    df.fillna(0, inplace= True)

    # Removes unnecassary whitespaces from VendorName and Description field
    df["VendorName"].str.strip()
    df["Description"].str.strip()

    # Formats Volume field from object to float
    df["Volume"] = df["Volume"].astype("float64")

    return df



# Data Preprocessing
def data_preprocess(df):
    """Calculates Gross Profit, Profit Margin, Stock Turnover and Sales-to-Price Ratio and adds it to the table"""

    # Calculates Gross Profit
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]

    # Calculates Profit Margin
    df["ProfitMargin"] = (df["GrossProfit"] / df["TotalSalesDollars"]) * 100

    # Calculates Stock Turnover
    df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"]

    # Calculates Sales-to-Price Ratio
    df["SalesToPriceRatio"] = df["TotalSalesDollars"]/df["TotalPurchaseDollars"]

    return df



# Setting up log for monitoring progress
logging.basicConfig(
    filename= "logs/get_vendor_summary.logs",
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode= "a"
)



if __name__ == "__main__":

    # Saves the start time for the operation
    start_time= time.time()

    # Connects to  inventory.db
    database = sqlite3.connect("inventory.db")

    # Creates the vendor summary table
    logging.info("Creating Vendor Summary Table...")
    summary = create_vendor_summary_table(database)
    logging.info(summary.head())

    # Cleans the vendor summary table
    logging.info("Cleaning Data...")
    clean_df = data_cleaning(summary)
    logging.info(clean_df.head())

    # Preprocesses the vendor summary table
    logging.info("Preprocessing Data...")
    preprocessed_df = data_preprocess(clean_df)
    logging.info(preprocessed_df.head())

    # Updates inventory.db by adding vendor summary table
    logging.info("Ingesting Data...")
    ingest_db(preprocessed_df, "vendor_sales_summary", database)
    logging.info("Ingestion Complete...")

    # Saves the end time for the operation
    end_time= time.time()

    # Calculates the time taken for the operation in minutes
    total_time = (end_time - start_time) / 60
    logging.info(f"Total Time Taken: {total_time} minutes")