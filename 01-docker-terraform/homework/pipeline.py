import pandas as pd
from sqlalchemy import create_engine
import os
from time import time


def ready_taxi_trips_dataset(url: str = None):
    if not url:
        url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
    
    zip_file_name = "green_tripdata_2019-09.csv.gz"
    zip_path = os.path.join(os.getcwd(), zip_file_name)
    if os.path.exists(zip_path):
        print("Old zip file detected, deleting...")
        os.remove(zip_path)
        print("Old zip file deleted!")
    
    print(f"Downloading data from {url}...")
    os.system(f"wget {url}")
    print("Download completed")
    
    file_name = "green_tripdata_2019-09.csv"
    file_path = os.path.join(os.getcwd(), file_name)
    if os.path.exists(file_path):
        print("Old file detected, deleting...")
        os.remove(file_path)
        print("Old file deleted!")
    
    print("unziping file...")
    os.system(f"gzip -d {zip_path}")
    print("file unzipped!")


def ready_taxi_zones_dataset(url: str = None):
    if not url:
        url = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
    
    file_name = "taxi+_zone_lookup.csv"
    file_path = os.path.join(os.getcwd(), file_name)
    if os.path.exists(file_path):
        print("Old file detected, deleting...")
        os.remove(file_path)
        print("Old file deleted!")
    
    print(f"Downloading data from {url}...")
    os.system(f"wget {url}")
    print("Download completed")


def ingest_data(credentials: dict):
    user = credentials.get("user")
    password = credentials.get("password")
    host = credentials.get("host") 
    port = credentials.get("port") 
    db = credentials.get("db")
    table_name = credentials.get("table_name")
    url = credentials.get("url")
    
    if table_name == "taxi_trips":
        # download data
        ready_taxi_trips_dataset(url)
        
        # Connect to postgres
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        
        tripdata = pd.read_csv("green_tripdata_2019-09.csv", iterator=True, chunksize=20000)
        df = next(tripdata)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        while True: 
            try:
                t_start = time()

                df = next(tripdata)

                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break
        return True
    
    elif table_name == "taxi_zones":
        # download data
        ready_taxi_zones_dataset(url)
        
        # Connect to postgres
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        
        taxi_zones = pd.read_csv("taxi+_zone_lookup.csv")
        taxi_zones.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        taxi_zones.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        
        return True
    
    return False


def main():
    credentials = {
        "user": "root",
        "password": "root",
        "host": "localhost",
        "port": "5432",
        "db": "ny_taxi",
        "table_name": "",
        "url": ""
    }
    
    # ingest taxi trips data
    credentials["table_name"] = "taxi_trips"
    if ingest_data(credentials):
        print("Taxi trips data ingested successfullly")
        
    # ingest zones
    credentials["table_name"] = "taxi_zones"
    if ingest_data(credentials):
        print("Taxi zones data ingested successfullly")


if __name__ == "__main__":
    main()
