"""
Pipeline entry point.

Runs the medallion pipeline in order:
1. Bronze ingestion
2. Silver transformation
3. Gold provisioning

Each stage is run in a separate Python process so that Spark sessions do not
conflict. This is important because the Gold layer uses Delta Lake.
"""

import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_stage(script_path):
    full_path = PROJECT_ROOT / script_path

    print("=" * 80)
    print(f"Running stage: {script_path}")
    print("=" * 80)

    result = subprocess.run(
        [sys.executable, str(full_path)],
        cwd=str(PROJECT_ROOT)
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Stage failed: {script_path} with exit code {result.returncode}"
        )

    print(f"Completed stage: {script_path}")


if __name__ == "__main__":
    run_stage("pipeline/ingest.py")
    run_stage("pipeline/transform.py")
    run_stage("pipeline/provision.py")

    print("Full pipeline completed successfully.")
