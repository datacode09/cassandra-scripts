import json
import os
import subprocess
import datetime

# === CONFIG ===
JAR_PATH = "lib/cassandra-data-migrator-5.4.0.jar"
PROPERTIES_FILE = "config/clusters_clone.properties"
MASTER = "local[*]"
DRIVER_MEM = "8G"
EXEC_MEM = "8G"
CLASS_NAME = "com.datastax.cdm.job.Migrate"
KEYSPACE = "ks"
INPUT_FILE = "table_copy_jobs.json"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, INPUT_FILE)
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
SUMMARY_LOG = os.path.join(LOG_DIR, f"summary_{datetime.datetime.now():%Y%m%d_%H%M%S}.log")

os.makedirs(LOG_DIR, exist_ok=True)

def timestamp():
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def run_spark_job(origin_table, target_table, where_condition):
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"cdm_{target_table}_{timestamp_str}.log")

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

    msg_start = f"{timestamp()} Starting: {origin_table} → {target_table} | WHERE as_of_date {where_condition}"
    print(msg_start)

    with open(log_file, "w") as log:
        result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)

    status = "SUCCESS" if result.returncode == 0 else f"FAILED (exit {result.returncode})"
    msg_result = f"{timestamp()} {status}: {origin_table} → {target_table} | as_of_date {where_condition} | Log: {os.path.basename(log_file)}"
    
    print(msg_result)
    with open(SUMMARY_LOG, "a") as summary_log:
        summary_log.write(msg_result + "\n")

def main():
    if not os.path.exists(INPUT_PATH):
        msg = f"{timestamp()} ERROR: Input file not found: {INPUT_PATH}"
        print(msg)
        with open(SUMMARY_LOG, "a") as f:
            f.write(msg + "\n")
        return

    try:
        with open(INPUT_PATH) as f:
            job_list = json.load(f)
    except Exception as e:
        msg = f"{timestamp()} ERROR: Failed to read input JSON: {e}"
        print(msg)
        with open(SUMMARY_LOG, "a") as f:
            f.write(msg + "\n")
        return

    for entry in job_list:
        target_table = entry.get("table_name")
        conditions = entry.get("as_of_date_conditions", [])

        if not target_table or not conditions:
            msg = f"{timestamp()} SKIPPED: Incomplete entry: {entry}"
            print(msg)
            with open(SUMMARY_LOG, "a") as f:
                f.write(msg + "\n")
            continue

        origin_table = f"{KEYSPACE}.{target_table.replace('_copy', '')}"
        target_full = f"{KEYSPACE}.{target_table}"

        for condition in conditions:
            run_spark_job(origin_table, target_full, condition)

if __name__ == "__main__":
    main()
