from kfp import dsl
from google_cloud_pipeline_components import aiplatform as gcc_aiplatform

from kfp.v2.dsl import Output, Dataset
from .components import merge_predictions

@dsl.pipeline(
    name="customer_spending_pipeline",
)
def customer_spend_pipeline(
    pipeline_bucket: str,                # NEW PARAMETER
    input_uri: str,
    model_output_uri: str,
    batch_output_prefix: str,
    final_output_uri: str,
):

    # Construct pipeline_root dynamically
    pipeline_root = f"gs://{pipeline_bucket}/pipeline-root"

    # ----- Training -----
    train_task = gcc_aiplatform.CustomTrainingJobOp(
        display_name="customer-spend-train",
        script_path="trainer/task.py",
        container_uri="us-docker.pkg.dev/vertex-ai/training/scikit-learn-cpu",
        model_serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu",
        args=[
            "--train_data", input_uri,
            "--model_output", model_output_uri
        ]
    )

    # ----- Batch prediction -----
    batch_task = gcc_aiplatform.ModelBatchPredictOp(
        model=train_task.outputs["model"],
        job_display_name="customer-spend-batch",
        gcs_source_uris=[input_uri],
        gcs_destination_output_uri_prefix=batch_output_prefix,
    )

    # ----- Merge -----
    merge_predictions(
        input_data_uri=input_uri,
        prediction_dir_uri=batch_task.outputs["gcs_output_directory"],
        output_uri=final_output_uri,
    )
