import subprocess

# List of client scripts
clients = [
    "Client1_1_1.js",
    "Client1_1_2.js",
    "Client1_2_1.js",
    "Client1_2_2.js",
    "Client2_1_1.js",
    "Client2_1_2.js",
    "Client3_1_1.js",
    "Client3_1_2.js",
    "Client3_2_1.js",
]

# Start each client script
processes = []
for client in clients:
    proc = subprocess.Popen(["node", client])
    processes.append(proc)

# Wait for all processes to finish
for proc in processes:
    proc.wait()

