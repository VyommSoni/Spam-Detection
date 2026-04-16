
import os
import sys
from src.exception.spamexception import SpamDetectionException
from src.logging.spamlogging import logging
from dotenv import load_dotenv
load_dotenv()
from sklearn.model_selection import train_test_split
from src.Constant.training_pipeline import DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO,FILE_NAME,TEST_FILE_NAME,TRAIN_FILE_NAME
from src.Data_access.Data_access import SpamHamData
from src.Constant.database import DATABASE_NAME, COLLECTION_NAME
import pandas as pd
from src.entity.artifacts_entity import DataIngestionArtifact

class DataIngestion:
    def __init__(self):
        self.data_ingestion_train_test_split_ratio = DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.file_name = FILE_NAME
        self.test_file_Path=DataIngestionArtifact.test_file_path
        self.train_file_Path=DataIngestionArtifact.trained_file_path
        self.ingested_data_dir=os.path.join("artifacts", "data_ingestion", "Ingested data")

    def split_train_test_data(self,data:pd.DataFrame)->tuple[pd.DataFrame,pd.DataFrame]:
        '''This function is used to split the data into train and test data and return it as a tuple'''

        try:
            Train_Data,Test_Data=train_test_split(data,test_size=self.data_ingestion_train_test_split_ratio,random_state=42)
            Ingested_Data=self.ingested_data_dir

            #make folder of ingested data dir if not exist
            os.makedirs(Ingested_Data,exist_ok=True)
            Train_Data.to_csv(os.path.join(Ingested_Data,self.train_file_name),index=False,header=True)
            Test_Data.to_csv(os.path.join(Ingested_Data,self.test_file_name),index=False,header=True)

            logging.info(f"Data split into train and test data with test size {self.data_ingestion_train_test_split_ratio} and random state 42")
        except Exception as e:
            raise SpamDetectionException(e)
        
    def export_data_as_feature(self)->pd.DataFrame:
        '''This function is used to export the data as a feature file in the local file system'''
        try:
            dataframe=SpamHamData(DATABASENAME=DATABASE_NAME)
            data=dataframe.get_data()
            logging.info(f"Got the data from mongodb of len {len(data)}")
            print("Got the data from mongodb of len {len(data)")

            feature_store_filepath=self.feature_store_filepath
            dir_path=os.path.dirname(feature_store_filepath)
            os.makedirs(dir_path,exist_ok=True)
            print(f"saved the file into {dir_path}")
            logging.info(f"saved the file into {dir_path}")

            data.to_csv(feature_store_filepath,index=False,header=True)

            return data
        except Exception as e :
            raise SpamDetectionException(e,sys)
        
    def initiate_data_ingestion(self):
        '''This function will initiate the data ingestion process'''
        try:
            logging.info("Started data ingestion process...")
            dataframe=self.export_data_as_feature()

            logging.info("Got the data from mongodb and store it ")

            #split the data
            self.split_train_test_data(dataframe)

            #save this in train and test file separately
            logging.info("Saving train and test data into artifacts folder..")

            data_ingestion_artifact=DataIngestionArtifact(
                train_file_path=self.train_file_Path,
                test_file_Path=self.test_file_Path
            )

            print(f"save file it to the artifacts",data_ingestion_artifact)
        except Exception as e:
            raise SpamDetectionException(e,sys)





           
      
    