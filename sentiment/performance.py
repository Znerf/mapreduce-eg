import subprocess
import os
import tempfile
import time

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(func.__name__, " took ",end_time - start_time," seconds to run.")
        return result
    return wrapper

@measure_time
def run_hadoop_job(jar_file, input_text):
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_input_file:
            upload_start = time.time()
            temp_input_file.write(input_text.encode('utf-8'))
            temp_input_file.close()

            hdfs_input_path = "/tmp/temp_input_file.txt"  
            hdfs_output_dir = "/tmp/hadoop_output"         

            upload_cmd = ["hdfs", "dfs", "-put", temp_input_file.name, hdfs_input_path]
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
            process = subprocess.Popen(hadoop_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            process_end = time.time()

            print("Mapreduce Job time::", process_end-process_start)

            clearing_start = time.time()
            if process.returncode == 0:
                print("Hadoop job completed successfully.")

                output_files_cmd = ["hdfs", "dfs", "-ls", hdfs_output_dir]
                output_files_process = subprocess.Popen(output_files_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output_files_stdout, output_files_stderr = output_files_process.communicate()

                if output_files_process.returncode == 0:

                    output_files = [line.split()[-1] for line in output_files_stdout.decode().splitlines() if line]
                    for output_file in output_files:
                        print("Output from {}:".format(output_file))
                        
                        cat_cmd = ["hdfs", "dfs", "-cat", output_file]
                        cat_process = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        cat_stdout, cat_stderr = cat_process.communicate()
                        print(cat_stdout.decode())
                else:
                    print("Failed to list output files.")
                    print("Errors:\n", output_files_stderr.decode())
            else:
                print("Hadoop job failed with return code:", process.returncode)
                print("Errors:\n", stderr.decode())

            
            os.remove(temp_input_file.name)

            
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


# main
jar_file = "sentiment/sentiment.jar"
input_text = "Hello world Hello Hadoop\nThis is a simple test\nHello again"
run_hadoop_job(jar_file, input_text)

