from ETL import MainETL
import pandas as pd
data = MainETL()
data.extract()
data.transform()
data.load()