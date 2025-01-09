#!/usr/bin/env python3

import os
import subprocess
import signal
import time
import random
import sys
import select
import yaml
sys.path.append(os.path.join(os.getcwd(), 'migtests/lib'))
import yb
import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description="YB Voyager Resumption Test")
    parser.add_argument('config_file', metavar='config.yaml', type=str, 
                        help="Path to the YAML configuration file")
    return parser.parse_args()

def load_config(config_file):
    """Load the configuration from the provided YAML file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)        
    return config

def prepare_import_data_file_command(config):
    """
    Prepares the yb-voyager import data file command based on the given configuration.
    """
    file_table_map = config['file_table_map']
    additional_flags = config.get('additional_flags', {})

    args = [
        'yb-voyager', 'import', 'data', 'file',
        '--export-dir', os.getenv('EXPORT_DIR', ''),
        '--target-db-host', os.getenv('TARGET_DB_HOST', ''),
        '--target-db-port', os.getenv('TARGET_DB_PORT', ''),
        '--target-db-user', os.getenv('TARGET_DB_USER', ''),
        '--target-db-password', os.getenv('TARGET_DB_PASSWORD', ''),
        '--target-db-schema', os.getenv('TARGET_DB_SCHEMA', ''),
        '--target-db-name', os.getenv('TARGET_DB_NAME', ''),
        '--disable-pb', 'true',
        '--send-diagnostics', 'false',
        '--data-dir', os.getenv('DATA_DIR', ''),
        '--file-table-map', file_table_map
    ]

    if os.getenv('RUN_WITHOUT_ADAPTIVE_PARALLELISM') == 'true':
        args.extend(['--enable-adaptive-parallelism', 'false'])

    for flag, value in additional_flags.items():
        args.append(flag)
        args.append(value)

    return args


def prepare_import_data_command(config):
    """
    Prepares the yb-voyager import data command based on the given configuration.
    """

    additional_flags = config.get('additional_flags', {})

    args = [
        'yb-voyager', 'import', 'data',
        '--export-dir', os.getenv('EXPORT_DIR', ''),
        '--target-db-host', os.getenv('TARGET_DB_HOST', ''),
        '--target-db-port', os.getenv('TARGET_DB_PORT', ''),
        '--target-db-user', os.getenv('TARGET_DB_USER', ''),
        '--target-db-password', os.getenv('TARGET_DB_PASSWORD', ''),
        '--target-db-name', os.getenv('TARGET_DB_NAME', ''),
        '--disable-pb', 'true',
        '--send-diagnostics', 'false',
    ]
    
    if os.getenv('SOURCE_DB_TYPE') != 'postgresql':
        args.extend(['--target-db-schema', os.getenv('TARGET_DB_SCHEMA', '')])

    if os.getenv('RUN_WITHOUT_ADAPTIVE_PARALLELISM') == 'true':
        args.extend(['--enable-adaptive-parallelism', 'false'])

    for flag, value in additional_flags.items():
        args.append(flag)
        args.append(value)

    return args


def run_and_resume_voyager(command, resumption):
    """
    Runs the yb-voyager command with support for resumption testing.
    """
    for attempt in range(1, resumption['max_restarts'] + 1):
        print(f"\n--- Attempt {attempt} of {resumption['max_restarts']} ---")
        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print("Running command:", ' '.join(command), flush=True)

            start_time = time.time()
            full_output = ''

            while True:
                rlist, _, _ = select.select([process.stdout, process.stderr], [], [], 5)
                for ready in rlist:
                    output = ready.readline()
                    if not output:  # Exit if output is empty (end of process output)
                        break
                    full_output += output
                if time.time() - start_time > 5:
                    break

            if full_output:
                print(full_output.strip(), flush=True)

            while True:
                if process.poll() is not None:
                    break  # Process has ended, exit loop

                interrupt_interval_seconds = random.randint(
                    resumption['min_interrupt_seconds'], 
                    resumption['max_interrupt_seconds']
                )
                print(f"\nProcess will be interrupted in {interrupt_interval_seconds // 60}m {interrupt_interval_seconds % 60}s")
                time.sleep(interrupt_interval_seconds)
                print(f"\nInterrupting the import process (PID: {process.pid})")
                process.send_signal(signal.SIGINT)

                restart_wait_time_seconds = random.randint(
                    resumption['min_restart_wait_seconds'], 
                    resumption['max_restart_wait_seconds']
                )
                print(f"\nWaiting for {restart_wait_time_seconds // 60}m {restart_wait_time_seconds % 60}s before resuming...")
                time.sleep(restart_wait_time_seconds)

        except Exception as e:
            print(f"Error occurred during import: {e}")
            if process:
                process.kill()
            raise e
        
        finally:
            if process and process.poll() is None:
                print(f"Terminating process (PID: {process.pid})")
                process.kill()
                process.wait(timeout=30)

    # Final import retry logic
    print("\n--- Final attempt to complete the import ---")
    
    for _ in range(2): 
        try:
            print("\nVoyager command output:")

            process = subprocess.Popen(
                command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                text=True
            )

            # Capture and print output
            for line in iter(process.stdout.readline, ''):
                print(line.strip())
                sys.stdout.flush()

            process.wait()

            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, command)

            break
        except subprocess.CalledProcessError as e:
            print("\nVoyager command error:")
            for line in iter(process.stderr.readline, ''):
                print(line.strip())
                sys.stdout.flush()
            time.sleep(30)
    else:
        print("Final import failed after 2 attempts.")
        sys.exit(1)

def validate_row_counts(row_count, export_dir):
    """
    Validates the row counts of the target tables after import.
    If the row count validation fails, it logs details and exits.
    """
    failed_validations = []

    for table_identifier, expected_row_count in row_count.items():
        print(f"\nValidating row count for table '{table_identifier}'...")

        if '.' in table_identifier:
            schema, table_name = table_identifier.split('.', 1)
        else:
            schema = "public"
            table_name = table_identifier

        tgt = None
        try:
            tgt = yb.new_target_db()
            tgt.connect()
            print(f"Connected to target database. Using schema: {schema}")
            actual_row_count = tgt.get_row_count(table_name, schema)

            if actual_row_count == expected_row_count:
                print(f"\u2714 Validation successful: {table_identifier} - Expected: {expected_row_count}, Actual: {actual_row_count}")
            else:
                print(f"\u274C Validation failed: {table_identifier} - Expected: {expected_row_count}, Actual: {actual_row_count}")
                failed_validations.append((table_identifier, expected_row_count, actual_row_count))
        except Exception as e:
            print(f"Error during validation for table '{table_identifier}': {e}")
            failed_validations.append((table_identifier, expected_row_count, "Error"))
        finally:
            if tgt:
                tgt.close()
                print("Disconnected from target database.")

    if failed_validations:
        print("\nValidation failed for the following tables:")
        for table, expected, actual in failed_validations:
            print(f"  Table: {table}, Expected: {expected}, Actual: {actual}")
        print(f"\nFor more details, check {export_dir}/logs")
        sys.exit(1)
    else:
        print("\nAll table row counts validated successfully.")



def run_import_with_resumption(config):
    
    import_type = config.get('import_type', 'file')  # Default to 'file' if not specified

    if import_type == 'file':
        command = prepare_import_data_file_command(config)
    elif import_type == 'offline':
        command = prepare_import_data_command(config)
    else:
        raise ValueError(f"Unsupported import_type: {import_type}")

    run_and_resume_voyager(command, config['resumption'])

    validate_row_counts(config['row_count'], os.getenv('EXPORT_DIR', ''))


if __name__ == "__main__":
    try:
        args = parse_arguments()
        config = load_config(args.config_file)

        print(f"Loaded configuration from {args.config_file}")

        run_import_with_resumption(config)
        
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)
