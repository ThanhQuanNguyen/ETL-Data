from Utils.datasetup import *
import pandas as pd

blob_name="domain_properties.csv" 
database= AzureDB()
database.access_container("assignment-2")
df = database.access_blob_csv(blob_name=blob_name)
postcodes = pd.read_csv(r"C:\Users\DELL_PC\Documents\School UTS\Data System\Assignment 2\Code\Data\Sydney Suburbs Reviews.csv")
df = pd.merge(df,postcodes , on=['suburb'], how = 'left')
class ModelAbstract():
    def __init__(self):
        self.columns = None
        self.dimension_table = None
    
    def dimension_generator(self, name:str, columns:list):
        dim = df[columns]
        dim = dim.drop_duplicates()
        # Creating primary key for dimension table
        dim[f'{name}_id'] = range(1, len(dim) + 1)

        self.dimension_table = dim
        self.name = name
        self.columns = columns
    
    def load(self):
        if self.dimension_table is not None:
            # Upload dimension table to data warehouse
            database.upload_dataframe_sqldatabase(f'{self.name}_dim', blob_data=self.dimension_table)
        
            # Saving dimension table as separate file
            self.dimension_table.to_csv(f'./data/{self.name}_dim.csv')
        else:
            print("Please create a dimension table first using dimension_generator") 

class DimSuburb(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Suburb', ['suburb','Postcode','Region', 'suburb_population', 'suburb_median_income', 'km_from_cbd','Ethnic Breakdown 2016', 'Median House Price (2020)',
                                            'Median House Price (2021)', '% Change', 'Median House Rent (per week)', 'Median Apartment Price (2020)','Median Apartment Rent (per week)','Public Housing %','Review Link'])
            
class DimDateSold(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Date_Sold', ['date_sold'])
        
class DimCustomer(ModelAbstract):
    def __init__(self):
        super().__init__()
        self.dimension_generator('Customer', ['First Name', 'Last Name', 'Email'])
        
        
        

        
