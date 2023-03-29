import java.io.File;
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

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;

public class CountPosTags {
	
//	This line creates an instance of the POSModel class, which represents a
//	pre-trained model for part-of-speech tagging. The model is loaded from a
//	file on the local file system.
	private static POSModel model = new POSModelLoader().load(new File("/Users/adityav/Desktop/8/NoSQL/assns/assn-2/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin"));
	
	// Object: input key type
	// Text: input value type
	// Text: output key type
	// IntWritable: output value type
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		
//		This line creates an instance of the POSTaggerME class, which is used to
//		perform part-of-speech tagging on a given input sentence. The tagger is
//		initialized with the pre-trained model.
		POSTaggerME tagger = new POSTaggerME(model);

		private final static IntWritable one = new IntWritable(1);
		private Text objKey = new Text();

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+"); // extract words
			
			String[] tags = tagger.tag(tokens);
			for (String posTag : tags) {
				objKey.set(posTag+","+posTag); // a Text object is created using the set() method
				
				context.write(objKey, one); // IntWritable object representing the count is used as the output value.
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
		Job job = Job.getInstance(conf, "countTags_using_pairs_algo");

		job.setMapperClass(TokenizerMapper.class); //.class is a special syntax in Java that retrieves the Class object that represents the class.
		job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
//		In Hadoop MapReduce, the waitForCompletion() method is used to submit the job 
//		to the Hadoop cluster and wait for it to complete. The argument true passed to 
//		the waitForCompletion() method specifies that the driver program should block 
//		until the job completes.
		long startTime = System.currentTimeMillis();
		boolean output = job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		// Calculate the elapsed time for each job
		long elapsedTime = endTime - startTime;
		// Print the elapsed time for each job
		System.out.println("Elapsed time for job: " + elapsedTime + " ms");
		System.exit(output ? 0 : 1);

	}
}