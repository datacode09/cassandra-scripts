import subprocess

# Cassandra connection info
CQLSH_PATH = 'cqlsh'
CASSANDRA_HOST = '127.0.0.1'
CASSANDRA_PORT = '9042'
CASSANDRA_USERNAME = 'your_user'
CASSANDRA_PASSWORD = 'your_password'

# Source and target details
DDL_INPUT_FILE = 'ddl_output.cql'
NEW_TABLE_NAME = 'trade_pnl_f_copy'
ORIGINAL_TABLE_NAME = 'trade_pnl_f'

def run_cqlsh_command(cql_command):
    try:
        cmd = [
            CQLSH_PATH,
            CASSANDRA_HOST,
            CASSANDRA_PORT,
            '-u', CASSANDRA_USERNAME,
            '-p', CASSANDRA_PASSWORD,
            '-e', cql_command
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=10
        )

        if result.returncode != 0:
            print(f"❌ Command failed: {result.stderr.strip()}")
            return False

        print("✅ Table created successfully.")
        return True

    except subprocess.TimeoutExpired:
        print("❌ Timeout: Cassandra query took too long.")
        return False
    except Exception as e:
        print(f"❌ Error executing CQL command: {e}")
        return False

def main():
    try:
        with open(DDL_INPUT_FILE, 'r') as f:
            ddl = f.read()

        if not ddl.strip():
            print("❌ DDL file is empty.")
            return

        # Remove comments and prepare DDL
        ddl_lines = ddl.strip().splitlines()
        ddl_core = "\n".join(line for line in ddl_lines if not line.strip().startswith("--"))

        # Replace table name (first occurrence only)
        if ORIGINAL_TABLE_NAME not in ddl_core:
            print(f"❌ Table name '{ORIGINAL_TABLE_NAME}' not found in DDL.")
            return

        modified_ddl = ddl_core.replace(ORIGINAL_TABLE_NAME, NEW_TABLE_NAME, 1)

        print(f"📤 Creating table `{NEW_TABLE_NAME}` based on `{ORIGINAL_TABLE_NAME}`...")

        success = run_cqlsh_command(modified_ddl)
        if not success:
            print("❌ Table creation failed.")

    except FileNotFoundError:
        print(f"❌ File not found: {DDL_INPUT_FILE}")
    except PermissionError:
        print(f"❌ Permission denied when reading: {DDL_INPUT_FILE}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == '__main__':
    main()
