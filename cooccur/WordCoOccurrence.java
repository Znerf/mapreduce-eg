import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCoOccurrence {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text wordPair = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // Convert tokens into an array to handle pairs
            String[] tokens = new String[itr.countTokens()];
            int i = 0;
            while (itr.hasMoreTokens()) {
                tokens[i++] = itr.nextToken();
            }

            // Set up the sliding window for word co-occurrence
            int windowSize = 2; // Example window size
            for (i = 0; i < tokens.length; i++) {
                for (int j = i + 1; j < Math.min(i + windowSize, tokens.length); j++) {
                    if (!tokens[i].equals(tokens[j])) {
                        // Sort the pair (optional) to avoid duplicate pairs
                        String pair = (tokens[i].compareTo(tokens[j]) < 0) 
                                        ? tokens[i] + " " + tokens[j] 
                                        : tokens[j] + " " + tokens[i];
                        wordPair.set(pair);
                        context.write(wordPair, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word co-occurrence");
        job.setJarByClass(WordCoOccurrence.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}