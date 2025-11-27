from kfp.v2.dsl import component, Output

@component(
    base_image="python:3.10",
    packages_to_install=[
        "pandas",
        "google-cloud-storage",
        "gcsfs"  # required
    ]
)
def merge_predictions(input_data_uri: str, prediction_dir_uri: str, output_uri: str):
    import pandas as pd
    from google.cloud import storage
    import json
    import os

    # ----- Load input data from GCS -----
    input_df = pd.read_csv(input_data_uri)

    # ----- Download prediction JSONL files from GCS -----
    storage_client = storage.Client()
    bucket_name, prefix = prediction_dir_uri.replace("gs://", "").split("/", 1)
    bucket = storage_client.bucket(bucket_name)

    prediction_frames = []
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".jsonl"):
            content = blob.download_as_text().splitlines()
            records = [json.loads(line) for line in content]
            prediction_frames.append(pd.DataFrame(records))

    pred_df = pd.concat(prediction_frames, ignore_index=True)

    # ----- Combine -----
    result_df = pd.DataFrame({
        "email": input_df["email"],
        "expected_spending": pred_df["predictions"]
    })

    result_df.to_csv("final.csv", index=False)

    # ----- Upload output to GCS -----
    out_bucket_name, out_blob_path = output_uri.replace("gs://", "").split("/", 1)
    out_bucket = storage_client.bucket(out_bucket_name)
    out_blob = out_bucket.blob(out_blob_path)
    out_blob.upload_from_filename("final.csv")
