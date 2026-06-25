import subprocess
import time
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("PipelineOrchestrator")

SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_MASTER = "spark://spark-master:7077"
EXECUTOR_MEM = "512m"
DRIVER_MEM   = "512m"
NUM_EXECUTORS = "2"

STAGES = [
    {
        "name"   : "Bronze Layer",
        "script" : "/bronze_layer.py",
        "use_spark": True,
    },
    {
        "name"   : "Silver Layer",
        "script" : "/silver_layer_fixed.py",
        "use_spark": True,
    },
    {
        "name"   : "Gold Layer",
        "script" : "/gold_layer.py",
        "use_spark": True,
    },
    {
        "name"   : "Power BI Export",
        "script" : "/powerbi_export.py",
        "use_spark": True,
    },
]


def run_stage(stage):
    name      = stage["name"]
    script    = stage["script"]
    use_spark = stage["use_spark"]

    logger.info(f"Starting stage: {name}")
    start_time = time.time()

    if use_spark:
        cmd = [
            SPARK_SUBMIT,
            "--master",          SPARK_MASTER,
            "--executor-memory", EXECUTOR_MEM,
            "--driver-memory",   DRIVER_MEM,
            "--num-executors",   NUM_EXECUTORS,
            script
        ]
    else:
        cmd = [sys.executable, script]

    result = subprocess.run(cmd, capture_output=False, text=True)

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    if result.returncode == 0:
        logger.info(f"Stage complete: {name} | Time: {minutes}m {seconds}s")
        return True
    else:
        logger.error(f"Stage FAILED: {name} | Return code: {result.returncode}")
        return False


def run_full_pipeline():
    logger.info("= = = =")
    logger.info("Retail Analytics Pipeline - Full Run")
    logger.info("= = = =")

    pipeline_start = time.time()
    results = {}

    for stage in STAGES:
        success = run_stage(stage)
        results[stage["name"]] = success

        if not success:
            logger.error(f"Pipeline halted at stage: {stage['name']}")
            break

        logger.info("= = = =")

    pipeline_elapsed = time.time() - pipeline_start
    total_minutes    = int(pipeline_elapsed // 60)
    total_seconds    = int(pipeline_elapsed % 60)

    logger.info("= = = =")
    logger.info("Pipeline Execution Summary")
    logger.info(""= = = =")

    all_passed = True
    for stage_name, success in results.items():
        status = "PASSED" if success else "FAILED"
        if not success:
            all_passed = False
        logger.info(f"  {stage_name:<25}: {status}")

    logger.info(f"Total pipeline time  : {total_minutes}m {total_seconds}s")
    logger.info(f"Pipeline status      : {'SUCCESS' if all_passed else 'FAILED'}")
    logger.info("= = = =")


if __name__ == "__main__":
    run_full_pipeline()