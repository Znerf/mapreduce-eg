import subprocess
import os
import tempfile
import time
import re

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(func.__name__, " took ",end_time - start_time," seconds to run.")
        return result
    return wrapper

@measure_time
def run_hadoop_job(jar_file, input_file):
    hdfs_input_path = "/tmp/temp_input_file.txt"  
    hdfs_output_dir = "/tmp/hadoop_output" 

    try:
        upload_start = time.time()

        upload_cmd = ["hdfs", "dfs", "-put", input_file, hdfs_input_path]
        upload_process = subprocess.Popen(upload_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        upload_stdout, upload_stderr = upload_process.communicate()


        if upload_process.returncode != 0:
            print("Failed to upload file to HDFS.")
            print("Errors:\n", upload_stderr.decode())
            return
        upload_end = time.time()
        print("Upload time::", upload_end-upload_start)
        

        hadoop_cmd = [
            'hadoop', 'jar', jar_file, 'Sentiment',
            hdfs_input_path,
            hdfs_output_dir
        ]

        process_start = time.time()
        process = subprocess.Popen(hadoop_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        stdout, stderr = process.communicate()
        process_end = time.time()

        print("Mapreduce Job time::", process_end-process_start)

        clearing_start = time.time()
        if process.returncode == 0:
            print("Hadoop job completed successfully.")

            metrics = {}
            for line in stdout.decode('utf-8').splitlines():
                match = re.match(r"\s*(.+?)=(\d+)", line)
                if match:
                    key, value = match.groups()
                    metrics[key.strip()] = int(value)

            # Print variables serially
            for i, (key, value) in enumerate(metrics.items(), start=1):
                print( key ,":",  value)


            output_files_cmd = ["hdfs", "dfs", "-ls", hdfs_output_dir]
            output_files_process = subprocess.Popen(output_files_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output_files_stdout, output_files_stderr = output_files_process.communicate()

            if output_files_process.returncode == 0:
                print("Success")
            else:
                print("Failed to list output files.")
                print("Errors:\n", output_files_stderr.decode())
        else:
            print("Hadoop job failed with return code:", process.returncode)
            print("Errors:\n", stderr.decode())
        
        cleanup_input_cmd = ["hdfs", "dfs", "-rm", hdfs_input_path]
        
        cleanup_output_cmd = ["hdfs", "dfs", "-rm", "-r", hdfs_output_dir]
        
        cleanup_output_process = subprocess.Popen(cleanup_output_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cleanup_output_stdout, cleanup_output_stderr = cleanup_output_process.communicate()

        if cleanup_output_process.returncode == 0:
            print("Successfully removed output directory from HDFS:",hdfs_output_dir)
        else:
            print("Failed to remove output directory from HDFS.")
            print("Errors:\n", cleanup_output_stderr.decode())

        cleanup_output_process = subprocess.Popen(cleanup_input_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cleanup_output_stdout, cleanup_output_stderr = cleanup_output_process.communicate()

        if cleanup_output_process.returncode == 0:
            print("Successfully removed output directory from HDFS:",hdfs_output_dir)
        else:
            print("Failed to remove output directory from HDFS.")
            print("Errors:\n", cleanup_output_stderr.decode())
        clearing_end = time.time()

        print("Clearing time::", clearing_end-clearing_start)
    except Exception as e:
        print("An error occurred:", str(e))

        cleanup_input_cmd = ["hdfs", "dfs", "-rm", hdfs_input_path]
        
        cleanup_output_cmd = ["hdfs", "dfs", "-rm", "-r", hdfs_output_dir]
        
        cleanup_output_process = subprocess.Popen(cleanup_output_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cleanup_output_stdout, cleanup_output_stderr = cleanup_output_process.communicate()

        if cleanup_output_process.returncode == 0:
            print("Successfully removed output directory from HDFS:",hdfs_output_dir)
        else:
            print("Failed to remove output directory from HDFS.")
            print("Errors:\n", cleanup_output_stderr.decode())

        cleanup_output_process = subprocess.Popen(cleanup_input_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        cleanup_output_stdout, cleanup_output_stderr = cleanup_output_process.communicate()

        if cleanup_output_process.returncode == 0:
            print("Successfully removed output directory from HDFS:",hdfs_output_dir)
        else:
            print("Failed to remove output directory from HDFS.")
            print("Errors:\n", cleanup_output_stderr.decode())


# main
jar_file = "sentiment/sentiment.jar"

for x in ["4.txt","8.txt","12.txt","16.txt"]:
    print("Executing file ::",x)
    input_text = "txt/"+x
    run_hadoop_job(jar_file, input_text)

