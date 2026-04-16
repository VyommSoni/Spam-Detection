from dotenv import load_dotenv
from src.logging import logging
from src.exception.spamexception import SpamDetectionException
from src.Constant.database import DATABASE_NAME, COLLECTION_NAME
from pymongo import MongoClient
import os
import sys

load_dotenv()  

class MongoDBConnection:
    def __init__(self, url):
        self.url= url
        self.client = None

    def connect(self):
        try:
            self.client = MongoClient(self.url)
            logging.info("Successfully connected to MongoDB.")
        except Exception as e:
            logging.error(f"Error connecting to MongoDB: {e}")
            raise e

    def get_collection(self):
        if self.client is None:
            logging.error("MongoDB client is not connected.")
            raise Exception("MongoDB client is not connected.")
        return self.client[DATABASE_NAME][COLLECTION_NAME]
    

if __name__ == "__main__":
    try:
        connection = MongoDBConnection(os.getenv("MONGODBURL"))
        connection.connect()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    