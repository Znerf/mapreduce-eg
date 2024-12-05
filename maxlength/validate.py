import os
import subprocess
import tempfile


def run_hadoop_job(jar_file, input_text):
    output_text = ""

    try:
        # Create a temporary file for input
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt", mode='w', encoding='utf-8') as temp_input_file:
            temp_input_file.write(input_text)
            temp_input_path = temp_input_file.name

        hdfs_input_path = "/tmp/temp_input_file.txt"
        hdfs_output_dir = "/tmp/hadoop_output"

        # Upload file to HDFS
        upload_cmd = ["hdfs", "dfs", "-put", temp_input_path, hdfs_input_path]
        if subprocess.run(upload_cmd).returncode != 0:
            print("Failed to upload file to HDFS.")
            return ""

        # Run Hadoop job
        hadoop_cmd = ["hadoop", "jar", jar_file, "MaxSentenceLength", hdfs_input_path, hdfs_output_dir]
        if subprocess.run(hadoop_cmd).returncode == 0:
            print("Hadoop job completed successfully.")

            # List output files from HDFS
            output_files_cmd = ["hdfs", "dfs", "-ls", hdfs_output_dir]
            output_files_process = subprocess.Popen(output_files_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            stdout, stderr = output_files_process.communicate()
            if output_files_process.returncode == 0:
                output_files = stdout.splitlines()
                for output_file in output_files:
                    if hdfs_output_dir in output_file:
                        try:
                            output_file_path = output_file.split()[-1]

                            # Read and print output
                            cat_cmd = ["hdfs", "dfs", "-cat", output_file_path]
                            cat_process = subprocess.Popen(cat_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
                            cat_stdout, cat_stderr = cat_process.communicate()
                            if cat_process.returncode == 0:
                                output_text = cat_stdout
                                print("Output from " + output_file_path + ":\n" + output_text)
                            else:
                                print("Failed to read output file from HDFS.")
                        except IndexError:
                            print("Unexpected output format for file listing.")
            else:
                print("Failed to list output files.")
        else:
            print("Hadoop job failed.")

        # Clean up HDFS and local files
        os.remove(temp_input_path)
        cleanup_hdfs(hdfs_input_path)
        # cleanup_hdfs(hdfs_output_dir)
    except Exception as e:
        print("An error occurred: " + str(e))

    return output_text


def cleanup_hdfs(path):
    cleanup_cmd = ["hdfs", "dfs", "-rm", "-r", path]
    if subprocess.run(cleanup_cmd).returncode == 0:
        print("Successfully removed " + path + " from HDFS.")
    else:
        print("Failed to remove " + path + " from HDFS.")


def get_levenshtein_distance(str1, str2):
    len1, len2 = len(str1), len(str2)
    dp = [[0] * (len2 + 1) for _ in range(len1 + 1)]

    for i in range(len1 + 1):
        for j in range(len2 + 1):
            if i == 0:
                dp[i][j] = j
            elif j == 0:
                dp[i][j] = i
            else:
                dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1,
                               dp[i - 1][j - 1] + (0 if str1[i - 1] == str2[j - 1] else 1))

    return dp[len1][len2]


def similarity(str1, str2):
    ground_truth = str1.lower()
    experimental = str2.lower()

    # Extract length and sentence
    length1 = int(ground_truth.split("longest sentence length: ")[1].split(",")[0].strip())
    sentence1 = ground_truth.split("Sentence: ")[0]

    length2 = int(experimental.split("longest sentence length: ")[1].split(",")[0].strip())
    sentence2 = experimental.split("Sentence: ")[0]

    sentence_diff = (1.0 - get_levenshtein_distance(sentence1, sentence2) / max(len(sentence1), len(sentence2))) * 100.0
    length_diff = abs(length1 - length2) / ((length1 + length2) / 2.0) * 100.0

    return (sentence_diff + (100.0 - length_diff)) / 2.0


def main():
    try:
        with open("maxlength/validate-passage1.txt", "r", encoding="utf-8") as f:
            count_actual = f.read()

        jar_file = "maxlength/maxsentencelen.jar"
        with open("txt/passage1.txt", "r", encoding="utf-8") as f:
            input_text = f.read()
        
        expected = "Max Sentence    Longest sentence length: 250, Sentence: I rather think the latter. Clive Thompson wrote last month in the NYT Magazine that constant digital updates, after a day, can begin 'to feel like a short story; follow it for a month, and it's a novel.' He was right to see the bits as part of a larger whole."
        result = run_hadoop_job(jar_file, input_text)
        print("\n\nEXPECTED: "+ expected +"\nRESULT: " + str(similarity(count_actual, result)) + "% Similarity\n\n")

        cleanup_hdfs("/tmp/hadoop_output")

    except Exception as e:
        print("Error: " + str(e))


if __name__ == "__main__":
    main()
