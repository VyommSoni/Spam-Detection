import os
from src.Constant.s3_bucket import (PREDICTION_BUCKET_NAME,
                                       TRAINING_BUCKET_NAME)
PRED_SCHEMA_FILE_PATH = os.path.join('config', 'prediction_schema.yml')

PREDICTION_DATA_BUCKET = PREDICTION_BUCKET_NAME
PREDICTION_INPUT_FILE_NAME = "spam.csv"
PREDICTION_OUTPUT_FILE_NAME = "spam_pred.csv"
MODEL_BUCKET_NAME = TRAINING_BUCKET_NAME