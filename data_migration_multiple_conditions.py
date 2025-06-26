import json
import os
import subprocess
import datetime

JAR_PATH = "lib/cassandra-data-migrator-5.4.0.jar"
PROPERTIES_FILE = "config/clusters_clone.properties"
MASTER = "local[*]"
DRIVER_MEM = "8G"
EXEC_MEM = "8G"
CLASS_NAME = "com.datastax.cdm.job.Migrate"
LOG_DIR = "logs"
KEYSPACE = "ks"  # Modify for your environment
INPUT_FILE = "table_copy_jobs.json"  # Must be in the same folder

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, INPUT_FILE)
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

def run_spark_job(origin_table, target_table, where_condition):
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"cdm_{target_table}_{timestamp}.log")

    cmd = [
        "spark-submit",
        "--properties-file", PROPERTIES_FILE,
        "--master", MASTER,
        "--driver-memory", DRIVER_MEM,
        "--executor-memory", EXEC_MEM,
        "--class", CLASS_NAME,
        JAR_PATH,
        "--conf", f"spark.cdm.schema.origin.keyspaceTable={origin_table}",
        "--conf", f"spark.cdm.schema.target.keyspaceTable={target_table}",
        "--conf", f'spark.cdm.filter.cassandra.whereCondition=as_of_date {where_condition}'
    ]

    print(f"Running: {origin_table} → {target_table} | as_of_date {where_condition}")
    with open(log_file, "w") as log:
        subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)
    print(f"→ Logged to: {log_file}")

def main():
    with open(INPUT_PATH) as f:
        job_list = json.load(f)

    for entry in job_list:
        target_table = entry["table_name"]
        conditions = entry["as_of_date_conditions"]

        for condition in conditions:
            origin_table = f"{KEYSPACE}.{target_table.replace('_copy', '')}"
            target_full = f"{KEYSPACE}.{target_table}"
            run_spark_job(origin_table, target_full, condition)

if __name__ == "__main__":
    main()
