import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import opennlp.tools.stemmer.PorterStemmer;

import org.apache.hadoop.fs.FileSystem;
import java.io.*;
import java.util.*;

public class TFIDF
{
    public static class MapTF extends Mapper<Text, Text, Text, MapWritable>
    {

        @Override
        public void setup(Context context) throws IOException, InterruptedException
        {
        	if(Top.initialized)
        	{
        		return;
        	}
	        // Load DF values from the cached file into a main-memory data structure
	        URI[] cacheFiles = context.getCacheFiles();
	        if (cacheFiles != null && cacheFiles.length > 0) {
	            try
	            {
	            	HashMap<Integer,ArrayList<String>> map = new HashMap<Integer,ArrayList<String>>();
	                PriorityQueue<Integer> pq = new PriorityQueue<>(Collections.reverseOrder());
	                
	                BufferedReader bufferedReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
	                String line;
	                while ((line = bufferedReader.readLine()) != null)
	                {
	                    String[] words = line.trim().split("\t");
	                    String val = words[0];
	                    int DF = Integer.parseInt(words[1]);
	                    pq.add(DF);
	                    if(map.containsKey(DF))
	                        map.get(DF).add(val);
	                    else
	                    {
	                        map.put(DF, new ArrayList<String>());
	                        map.get(DF).add(val);
	                    }

//                        System.out.println(val + " " + DF);
	                }
	                
	                Top obj = new Top();
	                
	                bufferedReader.close();

	                int count = 0;
	                ArrayList<String> answer = new ArrayList<String>();
	                ArrayList<Integer> df = new ArrayList<Integer>();
	                while(count<=100)
	                {
	                    int DF = pq.remove();
	                    try
	                    {
	                        Integer.parseInt(map.get(DF).get(0));
	                    }
	                    catch(Exception e)
	                    {
	                        answer.add(map.get(DF).get(0));
//	                        System.out.print("\"" + map.get(DF).get(0) + "\",");
//	                        df.add(DF);
	                        Top.newtop(map.get(DF).get(0));
	                        Top.newfreq(DF);
	                        count++;
	                    }
	                    map.get(DF).remove(0);
	                }
	                
//	                System.out.println();
//	                for(int DF : df)
//	                {
//	                    System.out.print(DF + ",");
//	                }
	            } catch (Exception e) {
	            	System.out.println("Error while reading cache : " + e.toString());
	            	System.exit(1);
	            }
	        }
	    }
        
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException
        {

        	String line = value.toString();
        	String[] tokens = line.split("[^\\w']+");
        	        	
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

    public static class ReduceTF extends Reducer<Text, MapWritable, Text, Text>
    {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException
        {

            for (MapWritable value : values)
            {
                for(MapWritable.Entry<Writable, Writable> e : value.entrySet())
                {
                	System.out.println(key.toString());
                    context.write(new Text(key.toString()+"\t"+e.getKey().toString()),new Text(e.getValue().toString()));
                }

            }
        }
    }


    public static class Map extends Mapper<Object, Text, Text, Text>
    {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {

        	String line = value.toString();
        	String[] tokens = line.split("\t",-1);
        	        	
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
		
		Job job0 = Job.getInstance(conf, "MR TF");

        job0.addCacheFile(new URI("/Users/architsangal/Data/College/Semester/NoSQL/Assignment/Assignment_2/Hadoop-NLP/TarFiles/output/full/part-r-00000"));
        
        job0.setInputFormatClass(DFInputFormat.class);
        
        job0.setMapperClass(MapTF.class);
        job0.setReducerClass(ReduceTF.class);

        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(MapWritable.class);

        job0.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job0, new Path(args[0]));
        FileOutputFormat.setOutputPath(job0, new Path(args[1]));

    	///////////////////
        Configuration conf1 = new Configuration();		
		Job job1 = Job.getInstance(conf1, "Stripes");
		
		job1.addCacheFile(new URI("/Users/architsangal/Data/College/Semester/NoSQL/Assignment/Assignment_2/Hadoop-NLP/TarFiles/output/full/part-r-00000"));
        
        job1.setMapperClass(Map.class);
        job1.setReducerClass(Reduce.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

		///////////////////

        if(job0.waitForCompletion(true))
        	System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
		
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