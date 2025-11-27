from kfp.v2 import compiler
from pipeline import customer_spend_pipeline

if __name__ == "__main__":
    try:
        compiler.Compiler().compile(
            pipeline_func=customer_spend_pipeline,
            package_path="customer_pipeline.json",
        )
        print("Compilation successful!")
    except Exception as e:
        print(f"Compilation failed: {e}")