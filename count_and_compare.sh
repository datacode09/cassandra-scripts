#!/bin/bash

# --- Configuration ---
SPARK_HOME="/path/to/your/spark"        # Set your Spark installation directory
JAVA_HOME="/path/to/your/java"          # Set your Java installation directory
CASSANDRA_HOST="localhost"              # Your Cassandra host
CASSANDRA_USERNAME="your_username"      # Your Cassandra username
CASSANDRA_PASSWORD="your_password"      # Your Cassandra password
KEYSPACE1="keyspace1"                   # First keyspace
TABLE1="table1"                         # First table
KEYSPACE2="keyspace2"                   # Second keyspace
TABLE2="table2"                         # Second table
CONNECTOR_JAR="/path/to/spark-cassandra-connector-assembly_2.12-3.5.1.jar" # Connector JAR
LOG_FILE="spark_cassandra_count_compare.log"   # Log file

# Spark memory settings
DRIVER_MEMORY="2g"                      # Driver memory (e.g., "2g" for 2GB)
EXECUTOR_MEMORY="2g"                    # Executor memory (e.g., "2g" for 2GB)

# --- Environment Variable Checks ---

if [ ! -d "$SPARK_HOME" ]; then
    echo "ERROR: SPARK_HOME directory not found. Please set SPARK_HOME."
    exit 1
fi

if [ ! -d "$JAVA_HOME" ]; then
    echo "ERROR: JAVA_HOME directory not found. Please set JAVA_HOME."
    exit 1
fi

if [ ! -f "$CONNECTOR_JAR" ]; then
    echo "ERROR: Connector JAR not found at $CONNECTOR_JAR"
    exit 1
fi

export SPARK_HOME
export JAVA_HOME
export PATH="$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH"

# --- Logging Function ---

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Redirect all output and errors to the log file
exec >> "$LOG_FILE" 2>&1

log_message "Starting Spark Cassandra count and compare script..."

# --- Pre-check Spark-shell Launch ---

log_message "Testing spark-shell launch..."

if ! command -v spark-shell &> /dev/null; then
    log_message "spark-shell could not be found. Please check your PATH or Spark installation."
    exit 1
fi

timeout 10 spark-shell -i /dev/null
if [ $? -ne 0 ]; then
    log_message "spark-shell failed to launch. Please check your setup."
    exit 1
fi

log_message "spark-shell launch test successful."

# --- Launch spark-shell with all configurations ---

log_message "Launching spark-shell with Cassandra connector JAR, authentication, and memory settings..."

spark-shell \
  --jars "$CONNECTOR_JAR" \
  --driver-memory "$DRIVER_MEMORY" \
  --executor-memory "$EXECUTOR_MEMORY" \
  --conf spark.cassandra.connection.host="$CASSANDRA_HOST" \
  --conf spark.cassandra.auth.username="$CASSANDRA_USERNAME" \
  --conf spark.cassandra.auth.password="$CASSANDRA_PASSWORD" \
  << EOF | tee -a "$LOG_FILE"

import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

logMessage("Reading from Cassandra tables...")

// Read first table
val df1 = spark.read
  .cassandraFormat("$TABLE1", "$KEYSPACE1")
  .load()

// Read second table
val df2 = spark.read
  .cassandraFormat("$TABLE2", "$KEYSPACE2")
  .load()

// Example condition: count rows where column 'status' equals 'active'
val condition = "status = 'active'"

// Count rows in each table based on condition
val count1 = df1.filter(condition).count()
val count2 = df2.filter(condition).count()

println("Count of $KEYSPACE1.$TABLE1 where " + condition + ": " + count1)
println("Count of $KEYSPACE2.$TABLE2 where " + condition + ": " + count2)

if (count1 == count2) {
  println("Counts are equal.")
} else {
  println("Counts are different by " + (count1 - count2).abs + ".")
}

System.exit(0)
EOF

# Capture exit status
SPARK_EXIT=$?

if [ $SPARK_EXIT -eq 0 ]; then
    log_message "Spark-shell completed successfully."
else
    log_message "Spark-shell failed with exit code $SPARK_EXIT."
fi

log_message "End of script."


