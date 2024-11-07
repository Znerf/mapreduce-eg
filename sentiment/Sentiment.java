import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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
           "Good", "Great", "Excellent", "Fantastic", "Wonderful", "Awesome", "Positive", "Happy", 
            "Joyful", "Amazing", "Inspiring", "Motivating", "Encouraging", "Optimistic", "Successful", 
            "Powerful", "Love", "Caring", "Generous", "Kind", "Affectionate", "Supportive", "Creative", 
            "Bright", "Healthy", "Strong", "Vibrant", "Radiant", "Grateful", "Lucky", "Fortunate", 
            "Peaceful", "Calm", "Confident", "Empowered", "Fulfilling", "Prosperous", "Charming", 
            "Lively", "Warm", "Compassionate", "Resilient", "Inspirational", "Uplifting", "Content", 
            "Successful", "Energetic", "Optimistic", "Hopeful", "Loving", "Appreciative", "Gracious",
            "Brilliant", "Phenomenal", "Fantastic", "Splendid", "Delightful", "Exquisite", "Radiant", 
            "Stellar", "Remarkable", "Wonderful", "Extraordinary", "Majestic", "Peaceful", "Joyous", 
            "Satisfying", "Refreshing", "Motivational", "Empathetic", "Harmonious", "Blessed", "Serene", 
            "Prosperous", "Exciting", "Sublime", "Luminous", "Admired", "Admirable", "Noble", "Benevolent", 
            "Courageous", "Dazzling", "Invigorating", "Gracious", "Joyous", "Affable", "Fun-loving", 
            "Faithful", "Compassionate", "Mindful", "Balanced", "Loyal", "Luminous", "Serene", "Euphoric", 
            "Tender", "Soulful", "Open-hearted", "Flourishing", "Fabulous", "Unique", "Intelligent", 
            "Thoughtful", "Brave", "Enlightened", "Caring", "Trustworthy", "Humble", "Honorable"
        ));
        
        private static final Set<String> negativeWords = new HashSet<>(Arrays.asList(
             "No", "Not", "None", "Never", "Nowhere", "Nothing", "Nobody", "Neither",  // General Negative Words
            "Bad", "Worse", "Worst", "Ugly", "Poor", "Terrible", "Horrible", "Awful", 
            "Dirty", "Mean", "Sad", "Guilty", "Unfair", "Harmful", "Wrong", "Unhappy", 
            "Weak", "Disappointing", "Hopeless",  // Negative Adjectives
            "Hate", "Reject", "Blame", "Criticize", "Fail", "Lose", "Hurt", "Destroy", 
            "Break", "Ruin", "Mislead", "Neglect", "Deny", "Abandon", "Cheat",  // Negative Verbs
            "Problem", "Error", "Failure", "Conflict", "Crisis", "Risk", "Danger", 
            "Loss", "Pain", "Suffering", "Fear", "Mistake", "Misery", "Anger", "Guilt", 
            "Threat", "Obstacle", "Disease",  // Negative Nouns
            "Barely", "Hardly", "Scarcely", "Never", "Poorly", "Badly", "Worse",  // Negative Adverbs
            "Alone", "Unwanted", "Rejected", "Cold", "Stuck", "Broken", "Delayed", 
            "Absent", "Low", "Dead"  // Contextual Negative Words
        ));

        private final static IntWritable one = new IntWritable(1);
        private Text sentiment = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] sentences = value.toString().split("\\.");
            for (String str : sentences) {
                // Trim whitespace and avoid empty strings
                str = str.trim();
                if (str.isEmpty()) {
                    continue;
                }

                // Initialize counters for each sentence
                int positiveCount = 0;
                int negativeCount = 0;
                
                // Split the sentence into words
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
        Job job = Job.getInstance(conf, "sentiment analysis");
        job.setJarByClass(Sentiment.class); // Correct class name used here
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
