from dotenv import load_dotenv
from src.exception.spamexception import SpamDetectionException
from src.Configuration.mongo_db_connection import MongoDBConnection
from src.Constant.database import DATABASE_NAME, COLLECTION_NAME
from src.logging.spamlogging import logging  
import sys
import os
import pandas as pd

load_dotenv() # Load environment variables from .env file

class SpamHamData:
    def __init__(self,DATABASENAME):
        self.database_name = DATABASENAME
        self.client = None

    def Connect(self,URL):
        '''This function is used to connect to the MongoDB and return the client object'''
        try:
            if self.client is None:
                self.client = MongoDBConnection(URL)
                self.client.connect()
                logging.info("MongoDB connection established successfully.")
            else:
                logging.info("MongoDB connection already exists.")
            return self.client
        
        except Exception as e:
            logging.error(f"Error occurred while connecting to MongoDB: {e}")
            raise SpamDetectionException(e)
        
    def get_all_connection_name(self)->list:
        '''this fcuntion is used to get all the collection names from the MongoDB'''

        try:
            if self.client is None:
                logging.error("MongoDB client is not connected.")
                raise Exception("MongoDB client is not connected.")
            return self.client.client.list_database_names()
        except Exception as e:
            logging.error(f"Error occurred while fetching collection names: {e}")
            raise SpamDetectionException(e)
        
    def get_data(self):
        '''Thus function is used to get the data from the MongoDB and return it as a pandas dataframe'''

        try:
            Client=self.Connect(os.getenv("MONGODBURL"))
            Collection=Client.get_collection()
            data=list(Collection.find())
            data=pd.DataFrame(data)
            return data
        except Exception as e:
            logging.error(f"Error occurred while fetching data from MongoDB: {e}")
            raise SpamDetectionException(e)

