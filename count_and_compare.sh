#!/bin/bash

# Configuration
CASSANDRA_HOST="localhost"
CASSANDRA_USERNAME="your_username"
CASSANDRA_PASSWORD="your_password"
KEYSPACE1="keyspace1"
TABLE1="table1"
KEYSPACE2="keyspace2"
TABLE2="table2"
LOG_FILE="spark_cassandra_conditional_count_compare.log"

# Function to log messages with timestamp
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Redirect all output and errors to the log file
exec >> "$LOG_FILE" 2>&1

log_message "Starting Spark Cassandra conditional count and compare script..."

# Spark Cassandra Connector package (adjust version as needed)
CONNECTOR_PKG="com.datastax.spark:spark-cassandra-connector_2.12:3.4.0"

# Launch spark-shell with Cassandra connector and auth, feed commands via heredoc
log_message "Launching spark-shell with Cassandra connector and authentication..."

spark-shell \
  --packages "$CONNECTOR_PKG" \
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
// Replace with your actual column name and value
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
