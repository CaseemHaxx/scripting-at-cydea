from opensearchpy import OpenSearch, exceptions as OpenSearchExceptions
import os
import urllib3
import datetime
import time
import shutil
import warnings
import configparser
import argparse
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.exceptions import RequestsDependencyWarning

# Suppress specific warnings
warnings.filterwarnings("ignore", category=RequestsDependencyWarning)
warnings.filterwarnings(
    "ignore",
    message="Connecting to https://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5} using SSL with verify_certs=False is insecure.",
)

# Suppress InsecureRequestWarning for unverified HTTPS requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Function to check disk space
def check_disk_space(path="/"):
    total, used, free = shutil.disk_usage(path)
    used_percent = (used / total) * 100
    return used_percent

# Function to list, backup, and delete old indices
def list_backup_delete_old_indices(
    host,
    port,
    username,
    password,
    repository_name,
    redundancy_period_days,
    disk_usage_threshold,
    snapshot_retry_interval,
    snapshot_max_retries,
):
    try:
        os_client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,  # Disable SSL verification (not recommended for production)
        )

        indices = os_client.indices.get_alias("*")
        current_date = datetime.datetime.now()
        old_indices = []

        # Extract and sort indices by creation date in descending order (oldest first)
        for index_name, index_info in indices.items():
            # Extract date from index name (assuming it ends with YYYY.MM.DD)
            index_date_str = index_name.rsplit("-", 1)[-1]
            try:
                creation_date = datetime.datetime.strptime(index_date_str, "%Y.%m.%d")
                if (current_date - creation_date).days > redundancy_period_days:
                    old_indices.append((index_name, creation_date))
            except ValueError:
                # Skip indices without valid date format
                pass

        # Sort indices by creation date (oldest first)
        old_indices.sort(key=lambda x: x[1])

        print(f"List of indices older than {redundancy_period_days} days:")
        for index_name, creation_date in old_indices:
            print(f"Index: {index_name}")

        # Check disk space before starting the snapshot process
        if check_disk_space("/") > disk_usage_threshold:
            print(
                f"Warning: Disk usage exceeded {disk_usage_threshold}% before starting snapshot process. Exiting."
            )
            return

        # Create snapshots for each index and delete them afterward
        for index_name, creation_date in old_indices:
            snapshot_name = (
                f"snapshot-{index_name}-{current_date.strftime('%Y%m%d%H%M%S')}"
            )
            try:
                # Create snapshot
                response = os_client.snapshot.create(
                    repository=repository_name,
                    snapshot=snapshot_name,
                    body={
                        "indices": index_name,
                        "ignore_unavailable": True,
                        "include_global_state": False,
                    },
                )
                print(f"Snapshot created for index {index_name}: {snapshot_name}")

                # Wait for snapshot to complete
                retries = 0
                while retries < snapshot_max_retries:
                    snapshot_status = os_client.snapshot.status(
                        repository=repository_name, snapshot=snapshot_name
                    )
                    if snapshot_status["snapshots"][0]["state"] == "SUCCESS":
                        break
                    elif snapshot_status["snapshots"][0]["state"] in ["FAILED", "PARTIAL"]:
                        raise Exception(
                            f"Snapshot creation failed for index {index_name}: {snapshot_status['snapshots'][0]['state']}"
                        )
                    print(f"Waiting for snapshot {snapshot_name} to complete...")
                    time.sleep(snapshot_retry_interval)
                    retries += 1

                if retries >= snapshot_max_retries:
                    print(f"Snapshot creation timed out for index {index_name}")
                    continue

                # Delete index after successful snapshot creation
                os_client.indices.delete(index=index_name)
                print(f"Index {index_name} deleted after snapshot creation.")

                # Check disk space usage after deleting indices
                if check_disk_space("/") > disk_usage_threshold:
                    print(
                        f"Warning: Disk usage exceeded {disk_usage_threshold}% after processing index {index_name}. Exiting."
                    )
                    return

            except OpenSearchExceptions.RequestError as e:
                print(f"Failed to create snapshot for index {index_name}: {e}")
            except Exception as e:
                print(f"Error processing index {index_name}: {e}")

    except OpenSearchExceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except OpenSearchExceptions.AuthorizationException as e:
        print(f"Authorization exception: {e}")
    except OpenSearchExceptions.RequestError as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"Error retrieving indices: {e}")

# Function to list and delete old indices
def list_delete_old_indices(host, port, username, password, days):
    try:
        os_client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=(username, password),
            use_ssl=True,
            verify_certs=False,  # Disable SSL verification (not recommended for production)
        )

        indices = os_client.indices.get_alias("*")
        current_date = datetime.datetime.now()
        old_indices = []

        # Extract and sort indices by creation date in descending order (oldest first)
        for index_name, index_info in indices.items():
            # Extract date from index name (assuming it ends with YYYY.MM.DD)
            index_date_str = index_name.rsplit("-", 1)[-1]
            try:
                creation_date = datetime.datetime.strptime(index_date_str, "%Y.%m.%d")
                if (current_date - creation_date).days > days:
                    old_indices.append((index_name, creation_date))
            except ValueError:
                # Skip indices without valid date format
                pass

        # Sort indices by creation date (oldest first)
        old_indices.sort(key=lambda x: x[1])

        print(f"List of indices older than {days} days:")
        for index_name, creation_date in old_indices:
            print(f"Index: {index_name}")

        # Delete each old index
        for index_name, creation_date in old_indices:
            try:
                os_client.indices.delete(index=index_name)
                print(f"Index {index_name} deleted.")
            except OpenSearchExceptions.RequestError as e:
                print(f"Failed to delete index {index_name}: {e}")
            except Exception as e:
                print(f"Error deleting index {index_name}: {e}")

    except OpenSearchExceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except OpenSearchExceptions.AuthorizationException as e:
        print(f"Authorization exception: {e}")
    except OpenSearchExceptions.RequestError as e:
        print(f"Request error: {e}")
    except Exception as e:
        print(f"Error retrieving indices: {e}")

# Main function to read configurations and arguments
def main():
    print("OpenSearch Indices Manager")

    # Load configurations from config file
    config = configparser.ConfigParser()
    config_file = 'config.ini'

    # Check if the config file exists
    if not os.path.exists(config_file):
        print(f"Configuration file '{config_file}' not found. Please create it and try again.")
        return

    # Read the config file
    config.read(config_file)

    # Get OpenSearch configuration values
    try:
        host = config.get("OpenSearch", "host")
        port = config.getint("OpenSearch", "port")
        username = config.get("OpenSearch", "username")
        password = config.get("OpenSearch", "password")
        repository_name = config.get("OpenSearch", "repository_name")
        snapshot_retry_interval = config.getint("Snapshot", "snapshot_retry_interval")
        snapshot_max_retries = config.getint("Snapshot", "snapshot_max_retries")
        disk_usage_threshold = config.getint("Snapshot", "disk_usage_threshold")
    except configparser.NoSectionError as e:
        print(f"Configuration error: {e}")
        return
    except configparser.NoOptionError as e:
        print(f"Missing configuration option: {e}")
        return

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Manage OpenSearch indices.")
    parser.add_argument(
        "--days",
        type=int,
        required=True,
        help="Number of redundancy days for old indices.",
    )
    parser.add_argument(
        "--action",
        choices=["backup_delete", "delete"],
        required=True,
        help="Action to perform: 'backup_delete' or 'delete'.",
    )
    args = parser.parse_args()

    redundancy_period_days = args.days
    action = args.action

    # Perform actions based on the selected option
    try:
        if action == "backup_delete":
            list_backup_delete_old_indices(
                host,
                port,
                username,
                password,
                repository_name,
                redundancy_period_days,
                disk_usage_threshold,
                snapshot_retry_interval,
                snapshot_max_retries,
            )
        elif action == "delete":
            list_delete_old_indices(host, port, username, password, redundancy_period_days)
    except Exception as e:
        print(f"An error occurred while performing the action: {e}")

if __name__ == "__main__":
    main()