import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

//@Override
//public void map(Object key, Text value, Context context) throws IOException, InterruptedException
//{
//

public class TF
{
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable>
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

        	String line = value.toString();
        	String[] tokens = line.split("[^\\w']+");
        	
        	System.out.println(key.toString());
        	System.out.println(value.toString());
        	
        	PorterStemmer steammer = new PorterStemmer();
        	for (int i = 0; i < tokens.length; i++)
        		tokens[i] = steammer.stem(tokens[i]).toString();
        	int[] freq = new int[100];
        	Top obj = new Top();
        	String[] top = obj.top;
        	for(String token : tokens)
        	{
        		for(int i=0;i<100;i++)
        		{
        			if(token.equals(top[i]))
        			{
        				freq[i]++;
        			}
        		}
        	}
        	
            MapWritable map = new MapWritable();
        	for(int i=0;i<100;i++)
        	{
        		map.put(new Text(top[i]),new LongWritable(freq[i]));
        	}
        	
        	context.write(new Text(key.toString()), map);
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, Text>
    {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
        {

            for (MapWritable value : values)
            {
                for(MapWritable.Entry<Writable, Writable> e : value.entrySet())
                {
                	System.out.println(key.toString());
                    context.write(new Text(key.toString()+"_"+e.getKey().toString()),new Text(((LongWritable)e.getValue()).get()+""));
                }

            }
        }
    }

 
    public static void main(String[] args) throws Exception
    {
    	
    	if (args.length < 2) {
            System.err.println("Must pass InputPath and OutputPath.");
            System.exit(1);
        }
    	
        Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Stripes");
        job.setJarByClass(TF.class);

//        job.addCacheFile(null);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class); // enable this to use 'local aggregation'
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(DFInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}