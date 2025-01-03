import subprocess
import os
import tempfile

def run_hadoop_job(jar_file, input_text):
    output = ""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as temp_input_file:
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


            hadoop_cmd = [
                'hadoop', 'jar', jar_file, 'MostFrequentWord',
                hdfs_input_path,
                hdfs_output_dir
            ]


            process = subprocess.Popen(hadoop_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()


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
                        output = cat_stdout.decode()
                else:
                    print("Failed to list output files.")
                    print("Errors:\n", output_files_stderr.decode())
            else:
                print("Hadoop job failed with return code:", process.returncode)
                print("Errors:\n", stderr.decode())

            # VALIDATION
            expected = "the   38"
            print()
            print("VALIDATION CHECK")
            print("\tEXPECTED OUTPUT: " + expected)
            print("\tACTUAL OUTPUT: " + output)
            print()
            ############

            
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

    except Exception as e:
        print("An error occurred:", str(e))


# main
jar_file = "mostfrequentword/frequent.jar"
with open("txt/passage1.txt", "r", encoding="utf-8") as f:
    input_text = f.read()
run_hadoop_job(jar_file, input_text)

