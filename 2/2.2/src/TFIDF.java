import java.io.IOException;
//import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

//@Override
//public void map(Object key, Text value, Context context) throws IOException, InterruptedException
//{
//

public class TFIDF
{
    public static class Map extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {

        	String line = value.toString();
        	String[] tokens = line.split("\t",-1);
        	
        	System.out.println(Arrays.toString(tokens));
        	
        	context.write(new Text(tokens[0]+"\t"+tokens[1]), new Text(tokens[2]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {

        	Top obj = new Top();

        	String[] tokens = key.toString().split("\t",-1);
        	int DF = Top.findDF(tokens[1]);

            for (Text value : values)
            {
            	if(DF == 0)
            		continue;
            	double score = Integer.parseInt(value.toString()) * (Math.log(10000.0/DF+1.0));
        	    context.write(new Text(tokens[0]+"\t"+tokens[1]),new Text(score+""));
            }
        }
    }

 
    public static void main(String[] args) throws Exception
    {
    	    	
        Configuration conf = new Configuration();		
		Job job = Job.getInstance(conf, "Stripes");
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    
    public static void run(String[] args)throws IOException
    {
    	try
    	{
    		main(args);
    	}
    	catch(Exception e)
    	{
    		System.out.println("Some Problem in TFIDF");
    	}
    }
}