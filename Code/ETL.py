import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from dotenv import load_dotenv
import pandas as pd
from Utils.datasetup import *
from Utils.dimension_classes import *

load_dotenv()
staff = ModelAbstract()
class MainETL():
    # List of columns need to be replaced
    def __init__(self) -> None:
        self.drop_columns = []
        self.dimension_tables = []
    
    def extract(self, csv_file="domain_properties.csv"):
        # Step 1: Extract: use pandas read_csv to open the csv file and extract data
        print(f'Step 1: Extracting data from csv file')
        self.fact_table = df
        print(f'We find {len(self.fact_table.index)} rows and {len(self.fact_table.columns)} columns in csv file: {csv_file}')
        print(f'Step 1 finished')
        self.fact_table.to_csv('overview.csv')
        
        
    def transform(self):
        # transform data types
        self.fact_table[['km_from_cbd']] = self.fact_table[['km_from_cbd']].astype(float)
        int_cols = ['num_bath', 'num_bed', 'num_parking', 'property_size', 'suburb_population', 'suburb_median_income']
        self.fact_table[int_cols] = self.fact_table[int_cols].astype(int)
        self.fact_table[['suburb', 'type']] = self.fact_table[['suburb', 'type']].astype(str)
        
        # fetch suburb dimension table
        dim_suburb = DimSuburb()
        self.drop_columns += dim_suburb.columns
        self.dimension_tables.append(dim_suburb)
        
        #fetch customer dimension table
        dim_customer = DimCustomer()
        self.drop_columns += dim_customer.columns
        self.dimension_tables.append(dim_customer)
        
        # fetch date_sold dimension
        dim_date_sold = DimDateSold()
        self.drop_columns += dim_date_sold.columns
        self.dimension_tables.append(dim_date_sold)
        new_col = dim_date_sold.dimension_table['date_sold']
        
        # Convert the date column to datetime format
        new_col = pd.to_datetime(new_col, format = 'mixed')
        dim_date_sold.dimension_table['date_sold'] = new_col
        self.fact_table['date_sold'] = pd.to_datetime(self.fact_table['date_sold'], format='mixed')
        
        
        #Get the Price of Properties per Square Meter
        price_per_sqm = self.fact_table['price'] / self.fact_table['property_size']
        self.fact_table['price_per_sqm'] = price_per_sqm
        
         # Replace columns in fact table with respective foreign keys
        for dim in self.dimension_tables:
            self.fact_table = pd.merge(self.fact_table, dim.dimension_table, on=dim.columns, how='left')
        self.fact_table = self.fact_table.drop(columns=self.drop_columns)
        self.fact_table.to_csv("Fact_Table.csv")
        dim_customer.dimension_table.to_csv('DimCustomer.csv')
        dim_suburb.dimension_table.to_csv('DimSuburb.csv')
        dim_date_sold.dimension_table.to_csv('DimDateSold.csv')
        print(f'Step 2 finished')
        
    def load(self):
        for table in self.dimension_tables:
            table.load()
        with engine.connect() as con:
            trans = con.begin()
            self.fact_table['Fact_table_id'] = range(1, len(self.fact_table) + 1)
            database.upload_dataframe_sqldatabase(f'Fact_table', blob_data=self.fact_table)
            
            self.fact_table.to_csv('Fact_table.csv')

            for table in self.dimension_tables:
                con.execute(text(f'ALTER TABLE [dbo].[Fact_table] WITH NOCHECK ADD CONSTRAINT [{table.name}_id] FOREIGN KEY ([{table.name}_id]) REFERENCES [dbo].[{table.name}_dim] ([{table.name}_id]) ON UPDATE CASCADE ON DELETE CASCADE;'))
            trans.commit()
            
        print(f'Step 3 finished')
        
        
        
        
        
        
    
    

