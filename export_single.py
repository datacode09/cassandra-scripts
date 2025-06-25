import subprocess

# Cassandra connection details (fill in if needed)
CQLSH_PATH = 'cqlsh'  # full path if not in $PATH
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_PORT = '9042'
CASSANDRA_USERNAME = 'your_user'
CASSANDRA_PASSWORD = 'your_password'

# Table info
KEYSPACE = 'rnc_trw'
TABLE = 'trade_pnl_f'
DDL_OUTPUT = 'ddl_output.cql'

def run_cqlsh_query(query):
    try:
        cmd = [
            CQLSH_PATH,
            CASSANDRA_HOST,
            CASSANDRA_PORT,
            '-u', CASSANDRA_USERNAME,
            '-p', CASSANDRA_PASSWORD,
            '-e', query
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=10
        )

        if result.returncode != 0:
            print(f"❌ Query failed: {result.stderr.strip()}")
            return None

        return result.stdout.strip()

    except Exception as e:
        print(f"❌ Error running query: {e}")
        return None

def main():
    print(f"📥 Extracting DDL for: {KEYSPACE}.{TABLE}")
    ddl_query = f"DESCRIBE TABLE {KEYSPACE}.{TABLE};"
    ddl = run_cqlsh_query(ddl_query)

    if ddl:
        with open(DDL_OUTPUT, 'w') as ddlfile:
            ddlfile.write(f"-- DDL for {KEYSPACE}.{TABLE}\n")
            ddlfile.write(ddl + "\n")
        print(f"✅ DDL written to: {DDL_OUTPUT}")
    else:
        print("❌ Failed to extract DDL.")

if __name__ == '__main__':
    main()
