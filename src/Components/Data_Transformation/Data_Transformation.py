import sys
from datetime import datetime
import numpy as np
import os
import pandas as pd
import re
import pickle
from typing import Union
from src.logging.spamlogging import logging
from src.exception.spamexception import SpamDetectionException
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.base import BaseEstimator, TransformerMixin
import nltk
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from spacy.lang.en import English
from src.entity.artifacts_entity import DataTransformationArtifact,DataValidationArtifact,DataIngestionArtifact
from src.entity.config_entity import DataTransformationConfig
from src.Components.Data_ingestion.Data_ingestion import DataIngestion
import warnings
from pandas import DataFrame
from sklearn.preprocessing import OrdinalEncoder
warnings.filterwarnings("ignore")

class DataTransformation:
    def __init__(self, data_transformation_config: DataTransformationConfig, data_ingestion_artifact: DataIngestionArtifact
                 , data_validation_artifact: DataValidationArtifact):
        self.data_transformation_config = data_transformation_config
        self.data_ingestion_artifact = data_ingestion_artifact
        self.data_validation_artifact = data_validation_artifact
        self.data_ingestion=DataIngestion()

    @staticmethod
    def read_data(filepath:str)->pd.DataFrame:
        try:
            return pd.read_csv(filepath)
        except Exception as e:
            raise SpamDetectionException(e,sys)
    
    def Tokenize_Lemmitization(self,data:DataFrame)->list:
        '''This function will Tokenize and Lemitize the data and return it as a list of tokens'''
        try:
            NLP=spacy.load("en_core_web_sm")
            doc=NLP(data)
            for token in doc:
                token.lemma_=token.lemma_.lower()
            return [token.lemma_ for token in doc if token.lemma_ not in STOP_WORDS]
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
    def Convert_to_vector(self,train_df:DataFrame,test_df:DataFrame,vectorizer:CountVectorizer)->Union[pd.DataFrame,pd.Series]:
        ''''This function will convert the data into a vector'''
        try:
            X_train=self.Tokenize_Lemmitization(train_df)
            X_test=self.Tokenize_Lemmitization(test_df)
            logging.info("Applying CountVectorizer on train and test data..")

            vectorize_X_train=vectorizer.fit_transform(X_train)
            vectorize_X_test=vectorizer.transform(X_test)
            X_train=vectorize_X_train.toarray()
            X_test=vectorize_X_test.toarray()

            return X_train,X_test,vectorizer
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
    def encode_label(self,train_df:pd.DataFrame,test_df:pd.DataFrame,encoder:OrdinalEncoder):
        try:
           Y_train=train_df['label']
           Y_test=test_df['label']

           Y_train=encoder.fit_transform(np.array(Y_train).reshape(-1,1))
           Y_test=encoder.transform(np.array(Y_test).reshape(-1,1))

           return Y_train,Y_test,encoder
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
    def initiate_data_transformation(self)->DataTransformationArtifact:
        try:
            train_df=self.read_data(filepath=self.data_ingestion_artifact.train_file_path)
            test_df=self.read_data(filepath=self.data_ingestion_artifact.test_file_path)

            X_train,X_test,vectorizer=self.Convert_to_vector(train_df=train_df,test_df=test_df,vectorizer=CountVectorizer)
            Y_train,Y_test,encoder=self.encode_label(train_df=train_df,test_df=test_df)

            preprocessor_dir=os.path.dirname(self.data_transformation_config.transformed_vectorizer_object_file_path)
            os.makedirs(preprocessor_dir,exist_ok=True)

            #save encode file
            encoder_file_path=self.data_transformation_config.transformed_encoder_object_file_path
            self.utils.save_object(file_path=encoder_file_path,obj=encoder) 

            #save vectorizer file
            vectorizer_file_path=self.data_transformation_config.transformed_vectorizer_object_file_path
            self.utils.save_object(file_path=vectorizer_file_path,obj=vectorizer)

            #concatinating input features and targets
            train_arr = np.c_[X_train, np.array(Y_train)]

            test_arr = np.c_[X_test, np.array(Y_test)]
                                

            logging.info(f"Saving transformed training and testing array.")
                
            self.utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_train_file_path,array=train_arr)
            self.utils.save_numpy_array_data(file_path=self.data_transformation_config.transformed_test_file_path,array=test_arr)


            data_transformation_artifact=DataTransformationArtifact(
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path,
                transformed_vectorizer_object_file_path=self.data_transformation_config.transformed_vectorizer_object_file_path,
                transformed_encoder_object_file_path=self.data_transformation_config.transformed_encoder_object_file_path
            )

            return data_transformation_artifact
        
        except Exception as e:
            raise SpamDetectionException(e,sys)   
