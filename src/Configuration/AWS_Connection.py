import boto3
import os
import sys
from src.exception.spamexception import SpamDetectionException
from src.logging.spamlogging import logging
from dotenv import load_dotenv  
load_dotenv()

"""If you want to set all Aws config through termianl then use 
set    AWS_ACCESS_KEY_ID=your_access_key_id="Your_access_key_id"
set    AWS_SECRET_ACCESS_KEY=your_secret_access_key="Your_secret_access_key"
set    AWS_REGION_NAME=your_region_name="your_region_name"
Then you are good to go.....
"""

class S3bucket:
    s3_client = None
    s3_resource = None

    def S3Connection(self,region_name=os.getenv("AWS_REGION_NAME")):
        '''This function is used to connect to the AWS S3 bucket and return the client and resource objects'''
        try:
            if self.s3_client is None or self.s3_resource is None:
                __AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
                __AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

                if __AWS_ACCESS_KEY_ID is None or __AWS_SECRET_ACCESS_KEY is None:
                    logging.error("AWS credentials are not set in environment variables.")
                    raise Exception("AWS credentials are not set in environment variables.")
                
                S3bucket.s3_client = boto3.client('s3', aws_access_key_id=__AWS_ACCESS_KEY_ID, aws_secret_access_key=__AWS_SECRET_ACCESS_KEY, region_name=region_name)
                S3bucket.s3_resource = boto3.resource('s3', region_name=region_name)
                logging.info("Successfully connected to AWS S3.")

                self.s3_client = S3bucket.s3_client
                self.s3_resource = S3bucket.s3_resource
                return self.s3_client, self.s3_resource  
            
            else:
                logging.info("AWS S3 connection already exists.")
        
        except Exception as e:
            logging.error(f"Error connecting to AWS S3: {e}")
            raise SpamDetectionException(e,sys)
 
if __name__ == "__main__":
    try:
        s3_connection = S3bucket()
        s3_client, s3_resource = s3_connection.S3Connection()
    except Exception as e:
        raise SpamDetectionException(e,sys) 
