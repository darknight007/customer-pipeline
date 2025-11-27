import argparse
import pandas as pd
import joblib
from google.cloud import storage
from sklearn.ensemble import RandomForestRegressor
import os

def train_model(train_uri, model_uri):
    # Load training data
    df = pd.read_csv(train_uri)

    X = df.drop(columns=["expected_spend"])
    y = df["expected_spend"]

    model = RandomForestRegressor()
    model.fit(X, y)

    joblib.dump(model, "model.joblib")

    # Upload trained model
    storage_client = storage.Client()
    bucket_name, blob_path = model_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename("model.joblib")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_data")
    parser.add_argument("--model_output")
    args = parser.parse_args()

    train_model(args.train_data, args.model_output)
