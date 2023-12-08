import subprocess
import os

# Directories containing the query scripts
#directories = ['oltp_queries', 'olap_queries']
directories = ['olap_queries']

# Scale factpr arguments to pass to the scripts
arguments = ['sf1', 'sf2', 'sf3', 'sf4', 'sf5']

for directory in directories:
    if not os.path.isdir(directory):
        print(f"Directory '{directory}' not found.")
        continue
    python_files = [file for file in os.listdir(directory) if file.endswith('.py')]

    for file in python_files:
        for arg in arguments:
            file_path = os.path.join(directory, file)
            # Construct the command to run the Python file with the argument
            command = ['python', file_path, arg]
            print(f"Running: {command}")
            subprocess.run(command)

print("All scripts executed.")
