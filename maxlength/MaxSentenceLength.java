import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxSentenceLength {

    // Mapper Class
    public static class MaxLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final Text sentenceKey = new Text("max_sentence");

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (!line.isEmpty()) {
                // Split the line into sentences (simple period-based splitting)
                String[] sentences = line.split("(?<=\\.)\\s+");  // Split by period followed by a space.

                for (String sentence : sentences) {
                    // Remove non-alphabetic characters and calculate sentence length
                    int length = sentence.replaceAll("[^a-zA-Z]", "").length();
                    context.write(sentenceKey, new IntWritable(length)); // Emit the length of the sentence
                }
            }
        }
    }

    // Reducer Class
    public static class MaxLengthReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxLength = 0;
            String longestSentence = "";

            // Find the longest sentence by comparing lengths
            for (IntWritable value : values) {
                int length = value.get();
                if (length > maxLength) {
                    maxLength = length;
                    longestSentence = value.toString();  // Store the corresponding sentence.
                }
            }
            result.set("Longest sentence length: " + maxLength + ", Sentence: " + longestSentence);
            context.write(new Text("Max Sentence"), result);  // Output the result
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MaxSentenceLength <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Sentence Length");

        job.setJarByClass(MaxSentenceLength.class);
        job.setMapperClass(MaxLengthMapper.class);
        job.setReducerClass(MaxLengthReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

