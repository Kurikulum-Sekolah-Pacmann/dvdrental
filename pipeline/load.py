import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from extract import Extract
from utils.db_conn import db_connection
from utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(luigi.Task):
    
    def requires(self):
        return Extract()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read query to truncate dvdrental schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/dvdrental-truncate_tables.sql'
            )
            
            
            logging.info("Read Load Query - SUCCESS")
            
        except Exception:
            logging.error("Read Load Query - FAILED")
            raise Exception("Failed to read Load Query")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read Data to be load
        try:
            # Read csv
            actor = pd.read_csv(self.input()[0].path)
            address = pd.read_csv(self.input()[1].path)
            category = pd.read_csv(self.input()[2].path)
            city = pd.read_csv(self.input()[3].path)
            country = pd.read_csv(self.input()[4].path)
            customer = pd.read_csv(self.input()[5].path)
            film = pd.read_csv(self.input()[6].path)
            film_actor = pd.read_csv(self.input()[7].path)
            film_category = pd.read_csv(self.input()[8].path)
            inventory = pd.read_csv(self.input()[9].path)
            language = pd.read_csv(self.input()[10].path)
            payment = pd.read_csv(self.input()[11].path)
            rental = pd.read_csv(self.input()[12].path)
            staff = pd.read_csv(self.input()[13].path)
            store = pd.read_csv(self.input()[14].path)
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception:
            logging.error(f"Read Extracted Data  - FAILED")
            raise Exception("Failed to Read Extracted Data")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Truncate all tables before load
        # This puropose to avoid errors because duplicate key value violates unique constraint
        try:            
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()

            # Execute each query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
                
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Truncate dvdrental Schema in DWH - SUCCESS")
        
        except Exception:
            logging.error(f"Truncate dvdrental Schema in DWH - FAILED")
            
            raise Exception("Failed to Truncate dvdrental Schema in DWH")
        
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for loading tables
        start_time = time.time()  
        logging.info("==================================STARTING LOAD DATA=======================================")
        # Load to tables to dvdrental schema
        try:
            
            try:
                # Load actor tables    
                actor.to_sql('actor', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.actor' - SUCCESS")
                
                # Load country tables
                country.to_sql('country', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.country' - SUCCESS")
                
                # Load city tables
                city.to_sql('city', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.city' - SUCCESS")
                
                
                # Load address tables
                address.to_sql('address', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.address' - SUCCESS")
                
                
                # Load category tables
                category.to_sql('category', 
                                con = dwh_engine, 
                                if_exists = 'append', 
                                index = False, 
                                schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.category' - SUCCESS")                
                
                # Load customer tables
                customer.to_sql('customer', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.customer' - SUCCESS")
                
                # Load language tables
                language.to_sql('language', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.language' - SUCCESS")
                
                
                # Load film tables
                film.to_sql('film', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.film' - SUCCESS")                
                
                # Load film_actor tables
                film_actor.to_sql('film_actor', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.film_actor' - SUCCESS")
                
                # Load film_category tables
                film_category.to_sql('film_category', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.film_category' - SUCCESS")
                
                # Load inventory tables
                inventory.to_sql('inventory', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.inventory' - SUCCESS")
                
                # Load staff tables
                staff.to_sql('staff', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.staff' - SUCCESS")
                
                # Load rental tables
                rental.to_sql('rental', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.rental' - SUCCESS")
                
                # Load payment tables
                payment.to_sql('payment', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.payment' - SUCCESS")
                
                # Load store tables
                store.to_sql('store', 
                                    con = dwh_engine, 
                                    if_exists = 'append', 
                                    index = False, 
                                    schema = 'dvdrental')
                logging.info(f"LOAD 'dvdrental.film' - SUCCESS")
                
                logging.info(f"LOAD 'dvdrental.store' - SUCCESS")
                logging.info(f"LOAD All Tables To DWH-dvdrental - SUCCESS")
                
            except Exception:
                logging.error(f"LOAD All Tables To DWH-dvdrental - FAILED")
                raise Exception('Failed Load Tables To DWH-dvdrental')        
        
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
                        
        #----------------------------------------------------------------------------------------------------------------------------------------
        except Exception:
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            logging.error("LOAD All Tables To DWH - FAILED")
            raise Exception('Failed Load Tables To DWH')   
        
        logging.info("==================================ENDING LOAD DATA=======================================")
        
    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]
        
# Execute the functions when the script is run
if __name__ == "__main__":
    # Build the task
    luigi.build([Extract(),
                 Load()])