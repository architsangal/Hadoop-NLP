import java.io.File;
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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

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
	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {
		
		private MapWritable row = new MapWritable();
		private Text rowKey = new Text();
		
//		This line creates an instance of the POSTaggerME class, which is used to
//		perform part-of-speech tagging on a given input sentence. The tagger is
//		initialized with the pre-trained model.
		POSTaggerME tagger = new POSTaggerME(model);


		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split("[^\\w']+"); // extract words
			String[] tags = tagger.tag(tokens);
			
			HashMap<String, ArrayList<String>> tagTokenMap = new HashMap<String, ArrayList<String>>();
			for(int i=0; i<tokens.length; i++) {
				String posTag = tags[i];
				String token = tokens[i];
				
				if (!tagTokenMap.containsKey(posTag)) {
					tagTokenMap.put(posTag, new ArrayList<String>());
					tagTokenMap.get(posTag).add(token); 
				}
				
				else {
					tagTokenMap.get(posTag).add(token);
				}
			}
			
			
			for (Map.Entry<String, ArrayList<String>> entry : tagTokenMap.entrySet()) {
			    String tag = entry.getKey(); // get the key
			    ArrayList<String> toks = entry.getValue(); // get the values
			    
			    row.clear();
			    
			    for (String tok : toks) {
			    	Text tokKey = new Text(tok);
                    if (row.containsKey(tokKey)) {
                        IntWritable count = (IntWritable) row.get(tokKey);
                        count.set(count.get() + 1);
                    } else {
                        row.put(tokKey, new IntWritable(1));
                    }
			    }
			    
			    rowKey.set(tag);
			    System.out.println("Hello World!");
			    context.write(rowKey, row);
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			
			int sum = 0;
			for (MapWritable val : values) {
				for (Map.Entry<Writable, Writable> entry : val.entrySet()) {
					IntWritable count = (IntWritable) entry.getValue();
					sum += count.get();
				}
			}
			
			IntWritable total = new IntWritable(sum);
			context.write(key, total);
			
		}

	}

	public static void main(String[] args) throws Exception {
//		System.out.println("Hello");

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "countTags_using_stripes_algo");

		job.setMapperClass(TokenizerMapper.class); //.class is a special syntax in Java that retrieves the Class object that represents the class.
		job.setReducerClass(IntSumReducer.class);
		
		
		/* Following 2 lines correspond to the output key, value classes of MAPPER */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

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