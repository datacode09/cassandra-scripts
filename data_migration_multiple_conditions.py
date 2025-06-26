import os
import json
import subprocess
import datetime
import shutil

# === ENVIRONMENT SETUP ===
JAVA_HOME = "/usr/lib/jvm/jre-11-openjdk"
SPARK_HOME = "/app/Data1/CDM/spark-3.5.4-bin-hadoop3-scala2.13"
CQLSH = "/app/Data1/CDM/cqlsh-5.1.47"

# Set environment variables
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["CQLSH"] = CQLSH

# Build full PATH
original_path = os.environ.get("PATH", "")
os.environ["PATH"] = (
    f"{os.path.expanduser('~')}/bin:"
    f"{JAVA_HOME}/bin:"
    f"{SPARK_HOME}/bin:"
    f"{CQLSH}/bin:"
    f"{original_path}"
)

# === CONFIG ===
JAR_PATH = "/app/Data1/CDM/cassandra-data-migrator-5.3.1.jar"
PROPERTIES_FILE = "/app/Data1/CDM/migration.properties"  # Intra-cluster config
MASTER = "local[*]"
DRIVER_MEM = "8G"
EXEC_MEM = "8G"
CLASS_NAME = "com.datastax.cdm.job.Migrate"
KEYSPACE = "rnc_trw"  # Replace with your actual keyspace
INPUT_FILE = "table_copy_jobs.json"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
INPUT_PATH = os.path.join(SCRIPT_DIR, INPUT_FILE)
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")
SUMMARY_LOG = os.path.join(LOG_DIR, f"summary_{datetime.datetime.now():%Y%m%d_%H%M%S}.log")

os.makedirs(LOG_DIR, exist_ok=True)

def timestamp():
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

def find_spark_submit():
    path = shutil.which("spark-submit")
    if path:
        return path
    fallback = os.path.join(SPARK_HOME, "bin", "spark-submit")
    if os.path.exists(fallback):
        return fallback
    raise FileNotFoundError("spark-submit not found. Ensure SPARK_HOME is correct.")

def run_spark_job(spark_submit, origin_table, target_table, where_condition):
    log_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"cdm_{target_table}_{log_ts}.log")

    cmd = [
        spark_submit,
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

    print(f"{timestamp()} STARTING: {origin_table} → {target_table} | WHERE as_of_date {where_condition}")
    with open(log_file, "w") as log:
        result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)

    status = "SUCCESS" if result.returncode == 0 else f"FAILED (exit {result.returncode})"
    msg = f"{timestamp()} {status}: {origin_table} → {target_table} | as_of_date {where_condition} | Log: {os.path.basename(log_file)}"
    print(msg)
    with open(SUMMARY_LOG, "a") as summary:
        summary.write(msg + "\n")

def main():
    try:
        spark_submit = find_spark_submit()
    except Exception as e:
        print(f"{timestamp()} ERROR: {e}")
        return

    if not os.path.exists(INPUT_PATH):
        print(f"{timestamp()} ERROR: Input file not found: {INPUT_PATH}")
        return

    try:
        with open(INPUT_PATH) as f:
            job_list = json.load(f)
    except Exception as e:
        print(f"{timestamp()} ERROR: Failed to load JSON: {e}")
        return

    for entry in job_list:
        target_table = entry.get("table_name")
        conditions = entry.get("as_of_date_conditions", [])

        if not target_table or not conditions:
            msg = f"{timestamp()} SKIPPED: Invalid entry → {entry}"
            print(msg)
            with open(SUMMARY_LOG, "a") as f:
                f.write(msg + "\n")
            continue

        origin_table = f"{KEYSPACE}.{target_table.replace('_copy', '')}"
        target_full = f"{KEYSPACE}.{target_table}"

        for condition in conditions:
            run_spark_job(spark_submit, origin_table, target_full, condition)

if __name__ == "__main__":
    main()
