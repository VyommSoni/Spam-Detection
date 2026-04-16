import os
import boto3
import sys
from src.exception.spamexception import SpamDetectionException
from src.logging.spamlogging import logging
from dotenv import load_dotenv
load_dotenv()
from src.Configuration.AWS_Connection import S3bucket
from io import StringIO
from pandas import read_csv, DataFrame
from typing import Union, List
import pickle
from botocore.exceptions import ClientError

'''This module is used to interact with the AWS S3 bucket and perform various operations like uploading files, reading files, creating folders etc.'''


'''
1.) Connection
2.) Check if the s3 key path is available in the bucket or not
3.)read file from s3 bucket
4.) get bucket object
5.)load model from s3 bucket
6.) create folder in s3 bucket
7.) upload file to s3 bucket
8.) upload dataframe as csv file to s3 bucket
9.) get dataframe from object
10.) read csv file from s3 bucket'''


class SimpleStorageService:

    def __init__(self):
        s3_client = S3bucket()
        self.s3_resource = s3_client.s3_resource
        self.s3_client = s3_client.s3_client

    def s3_key_path_available(self,bucket_name,s3_key)->bool: # this fucntion is used to check if the s3 key path is available in the bucket or not, it returns true if the path is available and false if the path is not available
        try:
            bucket = self.get_bucket(bucket_name)
            file_objects = [file_object for file_object in bucket.objects.filter(Prefix=s3_key)]
            if len(file_objects) > 0:
                return True
            else:
                return False
        except Exception as e:
            raise SpamDetectionException(e,sys)
        
        

    @staticmethod
    def read_object(object_name: str, decode: bool = True, make_readable: bool = False) -> Union[StringIO, str]:
        """
        this method is used to read the object from the s3 bucket and return it as a string or StringIO object based on the decode and make_readable parameters
        """
        logging.info("Entered the read_object method of S3Operations class")

        try:
            func = (
                lambda: object_name.get()["Body"].read().decode()
                if decode is True
                else object_name.get()["Body"].read()
            )
            conv_func = lambda: StringIO(func()) if make_readable is True else func()
            logging.info("Exited the read_object method of S3Operations class")
            return conv_func()

        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def get_bucket(self, bucket_name: str):
        """
        this method is used to get the bucket object from the s3 resource based on the bucket_name parameter
        """
        logging.info("Entered the get_bucket method of S3Operations class")

        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of S3Operations class")
            return bucket
        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def get_file_object( self, filename: str, bucket_name: str) -> Union[List[object], object]:
        """
        this method is used to get the file object from the s3 bucket based on the filename and bucket_name parameters, it returns a list of objects if there are multiple objects with the same filename in the bucket and it returns a single object if there is only one object with the same filename in the bucket
        """
        logging.info("Entered the get_file_object method of S3Operations class")

        try:
            bucket = self.get_bucket(bucket_name)

            file_objects = [file_object for file_object in bucket.objects.filter(Prefix=filename)]

            func = lambda x: x[0] if len(x) == 1 else x

            file_objs = func(file_objects)
            logging.info("Exited the get_file_object method of S3Operations class")

            return file_objs

        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None) -> object:
        """
        this method is used to load the model from the s3 bucket based on the model_name, bucket_name and model_dir parameters, it returns the model object
        """
        logging.info("Entered the load_model method of S3Operations class")

        try:
            func = (
                lambda: model_name
                if model_dir is None
                else model_dir + "/" + model_name
            )
            model_file = func()
            file_object = self.get_file_object(model_file, bucket_name)
            model_obj = self.read_object(file_object, decode=False)
            model = pickle.loads(model_obj)
            logging.info("Exited the load_model method of S3Operations class")
            return model

        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def create_folder(self, folder_name: str, bucket_name: str) -> None:
        """
        This method creates a folder in the specified S3 bucket if it does not already exist.
        """
        logging.info("Entered the create_folder method of S3Operations class")

        try:
            self.s3_resource.Object(bucket_name, folder_name).load()

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                folder_obj = folder_name + "/"
                self.s3_client.put_object(Bucket=bucket_name, Key=folder_obj)
            else:
                pass
            logging.info("Exited the create_folder method of S3Operations class")

    def upload_file(self, from_filename: str, to_filename: str,  bucket_name: str,  remove: bool = True):
        """
       this method is used to upload the file from the local system to the s3 bucket based on the from_filename, to_filename, bucket_name and remove parameters, it uploads the file to the s3 bucket and removes the file from the local system if the remove parameter is set to True
        """
        logging.info("Entered the upload_file method of S3Operations class")

        try:
            logging.info(
                f"Uploading {from_filename} file to {to_filename} file in {bucket_name} bucket"
            )

            self.s3_resource.meta.client.upload_file(
                from_filename, bucket_name, to_filename
            )

            logging.info(
                f"Uploaded {from_filename} file to {to_filename} file in {bucket_name} bucket"
            )

            if remove is True:
                os.remove(from_filename)

                logging.info(f"Remove is set to {remove}, deleted the file")

            else:
                logging.info(f"Remove is set to {remove}, not deleted the file")

            logging.info("Exited the upload_file method of S3Operations class")

        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def upload_df_as_csv(self,data_frame: DataFrame,local_filename: str, bucket_filename: str,bucket_name: str,) -> None:
        """
        this method is used to upload the dataframe as a csv file to the s3 bucket based on the data_frame, local_filename, bucket_filename and bucket_name parameters, it uploads the dataframe as a csv file to the s3 bucket
        """
        logging.info("Entered the upload_df_as_csv method of S3Operations class")

        try:
            data_frame.to_csv(local_filename, index=None, header=True)

            self.upload_file(local_filename, bucket_filename, bucket_name)

            logging.info("Exited the upload_df_as_csv method of S3Operations class")

        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def get_df_from_object(self, object_: object) -> DataFrame:
        """
       this method is used to get the dataframe from the object_name object, it reads the object and returns the dataframe
        """
        logging.info("Entered the get_df_from_object method of S3Operations class")

        try:
            content = self.read_object(object_, make_readable=True)
            df = read_csv(content, na_values="na")
            logging.info("Exited the get_df_from_object method of S3Operations class")
            return df
        except Exception as e:
            raise SpamDetectionException(e, sys) from e

    def read_csv(self, filename: str, bucket_name: str) -> DataFrame:
        """
        this method is used to read the csv file from the s3 bucket based on the filename and bucket_name parameters, it returns the dataframe
        """
        logging.info("Entered the read_csv method of S3Operations class")

        try:
            csv_obj = self.get_file_object(filename, bucket_name)
            df = self.get_df_from_object(csv_obj)
            logging.info("Exited the read_csv method of S3Operations class")
            return df
        except Exception as e:
            raise SpamDetectionException(e, sys) from e