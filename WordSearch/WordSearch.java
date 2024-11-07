import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordSearch {

    // Mapper Class
    public static class WordSearchMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text lineNum = new Text();
        private final static IntWritable one = new IntWritable(1);

        // Word to search (passed as a command-line argument)
        private String searchWord;

        // Setup method to initialize the search word from the command-line arguments
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            searchWord = conf.get("search.word");  // Getting the word to search from the configuration
        }

        // The map function checks if the search word is in each line
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Get the line of text as a String
            String line = value.toString();
            
            // Check if the search word is present in the line
            if (line.contains(searchWord)) {
                lineNum.set("Line " + key.toString());  // key here is the line number (based on byte offset)
                context.write(lineNum, one);  // Emit line number and a count of 1
            }
        }
    }

    // Reducer Class (Optional)
    public static class WordSearchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        // The reduce function sums up occurrences of the word and outputs the line numbers
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();  // Count the number of occurrences for each line
            }
            result.set(count);
            context.write(key, result);  // Emit the line number and the occurrence count
        }
    }

    // Main driver
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: WordSearch <input path> <output path> <word to search>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("search.word", args[2]);  // Set the word to search from command-line argument

        Job job = Job.getInstance(conf, "Word Search");

        job.setJarByClass(WordSearch.class);
        job.setMapperClass(WordSearchMapper.class);
        job.setReducerClass(WordSearchReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input path (HDFS)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Output path (HDFS)

        // Run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

