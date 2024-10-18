import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.nio.file.*;
import java.util.stream.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// Used chatGPT to help translate the original python script to Java, since the docker image was only able to use the JVM.
// Also used to speed along development


public class ValidateCoOccur {

    public static String runHadoopJob(String jarFile, String inputText) {
        ProcessBuilder processBuilder;
        Process process;
        String output_text = "";

        try {
            // Create temporary file for input
            Path tempInputFile = Files.createTempFile("temp_input_file", ".txt");
            Files.write(tempInputFile, inputText.getBytes(StandardCharsets.UTF_8));

            String hdfsInputPath = "/tmp/temp_input_file.txt";
            String hdfsOutputDir = "/tmp/hadoop_output";

            // Upload file to HDFS
            List<String> uploadCmd = Arrays.asList("hdfs", "dfs", "-put", tempInputFile.toString(), hdfsInputPath);
            processBuilder = new ProcessBuilder(uploadCmd);
            process = processBuilder.start();
            int uploadExitCode = process.waitFor();


            if (uploadExitCode != 0) {
                printErrors(process, "Failed to upload file to HDFS.");
                return "";
            }

            // Run Hadoop job
            List<String> hadoopCmd = Arrays.asList(
                    "hadoop", "jar", jarFile, "WordCoOccurrence", hdfsInputPath, hdfsOutputDir
            );
            processBuilder = new ProcessBuilder(hadoopCmd);
            process = processBuilder.start();
            int hadoopExitCode = process.waitFor();

            if (hadoopExitCode == 0) {
                System.out.println("Hadoop job completed successfully.");

                // List output files from HDFS
                List<String> outputFilesCmd = Arrays.asList("hdfs", "dfs", "-ls", hdfsOutputDir);
                processBuilder = new ProcessBuilder(outputFilesCmd);
                process = processBuilder.start();
                int outputFilesExitCode = process.waitFor();

                if (outputFilesExitCode == 0) {
                    List<String> outputFiles = readProcessOutput(process);
                    for (String outputFile : outputFiles) {
                        if (outputFile.contains(hdfsOutputDir)) {

                            int lastSpaceIndex = outputFile.lastIndexOf(' ');
                            String outputFilePath = outputFile.substring(lastSpaceIndex + 1);

                            System.out.println("Output from " + outputFile + ":");
                            List<String> catCmd = Arrays.asList("hdfs", "dfs", "-cat", outputFilePath);
                            processBuilder = new ProcessBuilder(catCmd);
                            String out = readCatOutput(processBuilder);
                            System.out.println(out);
                            output_text = out;
                        }
                    }
                } else {
                    printErrors(process, "Failed to list output files.");
                }
            } else {
                printErrors(process, "Hadoop job failed with return code: " + hadoopExitCode);
            }

            // Clean up HDFS and local files
            Files.deleteIfExists(tempInputFile);
            cleanupHDFS(hdfsInputPath);
            // cleanupHDFS(hdfsOutputDir);
        } catch (Exception e) {
            System.out.println("An error occurred: " + e.getMessage());
        }
        return output_text;
    }

    private static void cleanupHDFS(String path) throws IOException, InterruptedException {
        List<String> cleanupCmd = Arrays.asList("hdfs", "dfs", "-rm", "-r", path);
        Process process = new ProcessBuilder(cleanupCmd).start();
        int cleanupExitCode = process.waitFor();

        if (cleanupExitCode == 0) {
            System.out.println("Successfully removed " + path + " from HDFS.");
        } else {
            printErrors(process, "Failed to remove " + path + " from HDFS.");
        }
    }

    private static List<String> readProcessOutput(Process process) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            return reader.lines().collect(Collectors.toList());
        }
    }

    private static String readCatOutput(ProcessBuilder pb) throws IOException {
        
        Process p;
        pb.redirectErrorStream(true);
        p = pb.start();
        InputStream inputStream = p.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        StringBuilder output = new StringBuilder();
        try {
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            } 
        
            int exitCode = p.waitFor();
                if (exitCode == 0) {
                    return output.toString();
                } else {
                    System.err.println("Output Read failed with error code " + exitCode  + "\n" + output.toString());
                }

            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally {
                reader.close();
                inputStream.close();
        }
        return "";

    }

    private static void printErrors(Process process, String errorMessage) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
            System.out.println(errorMessage);
            reader.lines().forEach(System.out::println);
        }
    }

    public static double similarity(String str1, String str2) {
        String[] words1 = str1.toLowerCase().split("\\n");
        String[] words2 = str2.toLowerCase().split("\\n");

        // Skip the first line of words2
        words2 = Arrays.copyOfRange(words2, 1, words2.length);

        // Create maps to store words and their associated numbers
        Map<String, Integer> map1 = new HashMap<>();
        for (String line : words1) {
            // Trim and split the line by spaces
            String[] parts = line.trim().replaceAll("\\s+", " ").split(" ");
            if (parts.length == 2) {  // Ensure the line has exactly two parts: word and number
                String word = parts[0];  // Extract the word
                try {
                    int number = Integer.parseInt(parts[1]);  // Extract the number
                    map1.put(word, number);
                } catch (NumberFormatException e) {
                    System.out.println("Error parsing number in line: " + line);
                }
            } else {
                System.out.println("Line format invalid: " + line);
            }
        }

        Map<String, Integer> map2 = new HashMap<>();
        for (String line : words2) {
            // Trim and split the line by spaces
            String[] parts = line.trim().replaceAll("\\s+", " ").split(" ");
            if (parts.length == 2) {  // Ensure the line has exactly two parts: word and number
                String word = parts[0];  // Extract the word
                try {
                    int number = Integer.parseInt(parts[1]);  // Extract the number
                    map2.put(word, number);
                } catch (NumberFormatException e) {
                    System.out.println("Error parsing number in line: " + line);
                }
            } else {
                System.out.println("Line format invalid: " + line);
            }
        }

        // Print map1
        System.out.println("Map 1 (str1):");
        for (Map.Entry<String, Integer> entry : map1.entrySet()) {
            System.out.println("Words1: " + entry.getKey() + ", Number: " + entry.getValue());
        }

        // Print map2
        System.out.println("\nMap 2 (str2):");
        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
            System.out.println("Words2: " + entry.getKey() + ", Number: " + entry.getValue());
        }

        // Count matching words and compare numbers based on closeness
        double percentage = 0.0;
        int total_words = Math.max(map1.size(), map2.size());
        
        for (String word : map1.keySet()) {
            System.out.println("ATTEMPTING TO MATCH: " + word + "\n");
            if (map2.containsKey(word)) {  // Check if the word exists in both maps
                int num1 = map1.get(word);
                int num2 = map2.get(word);

                System.out.println("MATCH FOUND: " + word);
                System.out.println("num1: " + num1 + "\tnum2: " + num2);

                if (num1 == 0 && num2 == 0) {
                    return 100.0;
                }
                
                // Calculate absolute difference and normalize by the larger number
                int max = Math.max(num1, num2);
                int absDifference = Math.abs(num1 - num2);
                
                // Calculate percentage similarity
                double similarity = (1 - ((double) absDifference / max)) * 100;
                percentage += similarity;
                System.out.println("Similiarity: " + similarity + "%");
            }
        }

        return percentage / total_words;
    }
    

    public static void main(String[] args) {
        // String countActual = "";
        // try {
        //     countActual = new String(Files.readAllBytes(Paths.get("validate-passage1.txt")), StandardCharsets.UTF_8);
        // } catch (IOException e) {
        //     e.printStackTrace();
        // }

        String jarFile = "wordcoocur.jar";

        try {
            String inputText = new String(Files.readAllBytes(Paths.get("../txt/passage1.txt")), StandardCharsets.UTF_8);
            String result = runHadoopJob(jarFile, inputText);

            // System.out.println("\n\nRESULT: " + similarity(countActual, result) + "% Similarity\n\n");

            cleanupHDFS("/tmp/hadoop_output");

        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
