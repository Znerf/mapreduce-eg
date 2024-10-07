import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sentiment {

    // Mapper Class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final Set<String> positiveWords = new HashSet<>(Arrays.asList(
            "good", "happy", "joy", "awesome", "excellent", "great", "positive", "love", "like", "amazing"
        ));
        
        private static final Set<String> negativeWords = new HashSet<>(Arrays.asList(
            "bad", "sad", "angry", "terrible", "awful", "hate", "dislike", "horrible", "negative", "poor"
        ));

        private final static IntWritable one = new IntWritable(1);
        private Text sentiment = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] sentences = value.toString().split("\\.");
            for (String str : sentences) {
                // Trim whitespace and avoid empty strings
                String[] words = str.split("\\s+");
                    // Count positive and negative words
                for (String word : words) {
                    String lowerCaseWord = word.toLowerCase(); // Case insensitive comparison
                    if (positiveWords.contains(lowerCaseWord)) {
                        positiveCount++;
                    } else if (negativeWords.contains(lowerCaseWord)) {
                        negativeCount++;
                    }
                }

                // Determine the sentiment based on counts
                if (positiveCount > negativeCount) {
                    sentiment.set("positive");
                } else if (negativeCount > positiveCount) {
                    sentiment.set("negative");
                } else {
                    sentiment.set("neutral");
                }

                // Emit the sentiment with a count of 1
                context.write(sentiment, one);
            }
        }
    }

    // Reducer Class
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

    // Main Method to configure and run the job
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
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