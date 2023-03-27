import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import opennlp.tools.stemmer.PorterStemmer;

public class DF {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+");

			PorterStemmer steammer = new PorterStemmer();
			for (int i = 0; i < tokens.length; i++)
				tokens[i] = steammer.stem(tokens[i]).toString();

			StopWords st = new StopWords();
			HashSet<String> words = st.words;

			HashSet<String> set = new HashSet<>();

			for (String token : tokens) {
				if (!(set.contains(token) || words.contains(token) || token.equals(""))) {
					set.add(token);
					word.set(token);
					context.write(word, one); // context is like the output
				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
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
		Job job = Job.getInstance(conf, "df");

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local
		// aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(DFInputFormat.class);
		// job.setInputFormatClass(DFInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}