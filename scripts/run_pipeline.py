from google.cloud import aiplatform

# --------------------------
# EDIT THESE VALUES
# --------------------------
PROJECT_ID = "agents-universe"
REGION = "us-central1"
PIPELINE_BUCKET = "your-bucket"
INPUT_URI = "gs://your-bucket/data/input.csv"
MODEL_OUTPUT_URI = "gs://your-bucket/model/"
BATCH_OUTPUT_PREFIX = "gs://your-bucket/batch_predictions/"
FINAL_OUTPUT_URI = "gs://your-bucket/final_output/output.csv"

# --------------------------
# RUN THE PIPELINE JOB
# --------------------------
aiplatform.init(project=PROJECT_ID, location=REGION)

job = aiplatform.PipelineJob(
    display_name="customer-spend-pipeline",
    template_path="pipeline/pipeline.json",
    pipeline_root=f"gs://{PIPELINE_BUCKET}/pipeline-root",
    parameter_values={
        "pipeline_bucket": PIPELINE_BUCKET,
        "input_uri": INPUT_URI,
        "model_output_uri": MODEL_OUTPUT_URI,
        "batch_output_prefix": BATCH_OUTPUT_PREFIX,
        "final_output_uri": FINAL_OUTPUT_URI,
    },
)

job.run()
