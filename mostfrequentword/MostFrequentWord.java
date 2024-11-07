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
import java.util.Iterator;

public class MostFrequentWord {

    // Mapper Class
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the line to a string and split by whitespace to extract words
            String line = value.toString().toLowerCase(); // Optional: convert to lowercase to avoid case-sensitive counting
            String[] words = line.split("\\s+");

            for (String wordText : words) {
                word.set(wordText.replaceAll("[^a-zA-Z]", "")); // Remove non-alphabetic characters
                if (!word.toString().isEmpty()) {
                    context.write(word, one); // Emit word with count 1
                }
            }
        }
    }

    // Reducer Class
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private Text maxWord = new Text();
        private int maxCount = 0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // Calculate the total count for each word
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            // Update the word with the maximum count
            if (sum > maxCount) {
                maxCount = sum;
                maxWord.set(key);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Emit the word with the maximum count
            result.set(maxCount);
            context.write(maxWord, result);
        }
    }

    // Driver Class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MostFrequentWord <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Most Frequent Word");

        job.setJarByClass(MostFrequentWord.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input file path
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

