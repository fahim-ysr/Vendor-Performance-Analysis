# Automation script for data ingestion into database and log operation

# Importing required modules
import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Setting up database using SQLAlchemy
engine = create_engine('sqlite:///inventory.db')

# Function for ingestion
def ingest_db(df, table_name, engine):
    """Ingests dataframe into database"""
    df.to_sql(table_name, con= engine, if_exists= 'replace', index= False)

# Setting up log for monitoring progress
logging.basicConfig(
    filename= "logs/database.logs",
    level= logging.DEBUG,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    filemode= "a"
)

# Pipeline for data ingestion automation for keeping database up to date with the datasets
def load_data_to_db():
    """Loads CSVs as dataframe and ingests into database"""
    # Saves the start time for ingestion
    start_time = time.time()
    
    for file in os.listdir():
        if ".csv" in file:
            df = pd.read_csv(file)
            # Logs progress during ingestion
            logging.info(f"Ingesting {file} in inventory.db")
            ingest_db(df, file[:-4], engine)
            
    # Saves the end time for ingestion
    end_time= time.time()
    
    # Calculates the time taken in minutes for ingestion
    total_time = (end_time - start_time) / 60
    
    logging.info("Ingestion Completed...")
    logging.info(f"Total Time Taken: {total_time} minutes")

if __name__ == "__main__":
    load_data_to_db()