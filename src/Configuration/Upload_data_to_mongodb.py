import os
import sys
from src.exception.spamexception import SpamDetectionException
from src.Configuration.mongo_db_connection import MongoDBConnection
from src.logging.spamlogging import logging
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
class upload_data_to_mongodb:
    def __init__(self,URL):
        self.URL=URL

    def upload_data(self,data):
        try:
            connection = MongoDBConnection(self.URL)
            connection.connect()
            collection = connection.get_collection()
            if "id" in data.columns:
                data = data.drop(columns=["id"])
            collection.insert_many(data.to_dict("records"))
            print("Data uploaded to MongoDB successfully. shape of the data is {}".format(data.shape))

            logging.info("Data uploaded to MongoDB successfully.")
            
        except Exception as e:
            logging.error(f"Error occurred while uploading data to MongoDB: {e}")
            raise SpamDetectionException(e)
        
if __name__=="__main__":
        try:
            data=pd.read_csv("Data/spam.csv",encoding="latin-1")
            uploader=upload_data_to_mongodb(os.getenv("MONGODBURL"))
            uploader.upload_data(data)
        except Exception as e:
            logging.error(f"Error occurred in main block: {e}")
            raise SpamDetectionException(e)
        
