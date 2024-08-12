import os
import subprocess
import sys


# ----------------------------------------------------------------------
def close_connections(port):
    """"""
    print(f"Cleaning connections on port {port}...")

    try:
        # Run the lsof command to get the PIDs of active connections on the specified port
        result = subprocess.run(
            ["lsof", "-i", f":{port}", "-t"],
            stdout=subprocess.PIPE,
            text=True,
        )
        pids = result.stdout.strip().splitlines()

        if not pids:
            print(f"No active connections on port {port}.")
        else:
            # Close all active connections
            for pid in pids:
                print(f"Closing connection with PID {pid}...")
                os.kill(int(pid), 9)

    except Exception as e:
        print(
            f"An error occurred while trying to close connections on port {port}: {e}"
        )


def main():
    if len(sys.argv) != 2:
        sys.exit(1)

    try:
        port_range = sys.argv[1]
        START_PORT, END_PORT = map(int, port_range.split('-'))
        if START_PORT > END_PORT:
            raise ValueError(
                "The start port must be less than or equal to the end port."
            )
    except ValueError as e:
        print(f"Error in port range: {e}")
        sys.exit(1)

    # Iterate over the range of ports and close active connections
    for port in range(START_PORT, END_PORT + 1):
        close_connections(port)
