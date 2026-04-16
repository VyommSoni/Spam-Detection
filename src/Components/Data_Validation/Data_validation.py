from pandas import DataFrame
import os
import sys
from src.exception.spamexception import SpamDetectionException
from src.logging.spamlogging import logging
from src.Components.Data_ingestion.Data_ingestion import DataIngestion
from src.entity.artifacts_entity import DataIngestionArtifact
from src.entity.config_entity import DataValidationConfig
from src.Utils.main_utils import SpamUtils
import pandas as pd
from src.Constant.training_pipeline import SCHEMA_FILE_PATH

class DataValidation:

    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,data_validation_config:DataValidationConfig):
        self.data_ingestion_artifact=data_ingestion_artifact
        self.data_validation_config=data_validation_config
        self.schema_config=SpamUtils.read_yaml_file(SCHEMA_FILE_PATH)
        self.utils=SpamUtils()

    def validate_number_of_columns(self,dataframe:DataFrame)->bool:
        try:
            logging.info("Validating number of columns from schema and data")
            return len(dataframe.columns)==len(self.schema_config["columns"])
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
    def validate_schema_columns(self,dataframe:DataFrame)->bool:
        try:
            logging.info("Validating schema columns")
            dataframe_columns=list(dataframe.columns)
            schema_columns=list(self.schema_config["columns"].keys())

            schema_columns.sort()
            dataframe_columns.sort()

            return dataframe_columns==schema_columns
        except Exception as e:
            raise SpamDetectionException(e,sys)
    
    @staticmethod
    def read_data(filepath:str)->DataFrame:
        try:
          return pd.read_csv(filepath)
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
    def initiate_data_validation(self)->DataIngestionArtifact:
        try:
            train_df=DataValidation.read_data(self.data_ingestion_artifact.train_file_path)
            test_df=DataValidation.read_data(self.data_ingestion_artifact.test_file_path)

            logging.info("Validating number of columns from schema and data")
            logging.info("Validating schema columns")

            train_df_columns_status=self.validate_number_of_columns(train_df)
            test_df_columns_status=self.validate_number_of_columns(test_df)

            train_schema_columns_status=self.validate_schema_columns(train_df)
            test_schema_columns_status=self.validate_schema_columns(test_df)
            
            validation_status=False

            if train_df_columns_status and test_df_columns_status and train_schema_columns_status and test_schema_columns_status:
                logging.info("Data validation is successful")
                validation_status=True 

            data_validation_artifact=DataIngestionArtifact(
                validation_status=validation_status,
                valid_train_file_path=self.data_ingestion_artifact.train_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=self.data_validation_config.invalid_train_file_path,
                invalid_test_file_path=self.data_validation_config.invalid_test_file_path,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )

            return data_validation_artifact

        except Exception as e:
            raise SpamDetectionException(e,sys)
            
             

        