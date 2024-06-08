"# ETL-Data" 
instruction how to use the code:

Step 1: Check and make sure that you have Python installed on your local device.
Step 2: Download Github Repository
Step 3: Download all the packages in the requirements.txt (pip3 install - r equirement.txt)
Step 4: Download everything in Github.
Step 5: Change .env into your own Microsoft Azure Account  (my Azure Account already exists the tables so It may generate some errors if you run the code on my account information. Besides that you can not check the result in my personal account without my Outlook password given)
Step 7: connect to your Azure Account by Az login.
Step 8: Download the domain properties to your chosen blob for the Assignment. Change all the names in lines of code that relate to access the blob to your blob name.
Step 9: Download Sydney Suburb Review to your local computer and paste the path to “pd.read_csv” line of code.
Step 9: Run the main.py 
Step 10: Wait until all the terminals have done and come to the SQL Database that you had connected with in .env before to check the result.
Step 11: If you opened the Azure before, just refresh it and check the result.

Result: the result should be dimension tables and a fact tables as single star schema with Primary keys and Foreign keys. IF the database is connected with the Power BI or some other BI platform, the ERD should have the relationships between the table.
