import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class DFRecordReader extends RecordReader<LongWritable, Text> {

//	The following doesn't make sense as the entire document
//	is to be read, so start, offset and end doesn't make sense -
//	
//	private long start;
//	private long pos;
//	private long end;

//	private LineReader in;
//	private int maxLineLength;
//	private LongWritable key = new LongWritable();
//	private Text value = new Text();

    private FileSplit fSplit;
    private Configuration conf;
    
    private Text value = new Text();
    // Initialized as -1 but updated later
    private LongWritable key = new LongWritable(-1);
    
    // Is the reading complete, initialized with false.
    private boolean finished = false;

    // This is to initialize the class object with necessary data.
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)throws IOException, InterruptedException
    {
        this.fSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (finished == false)
        {
        	// size of the array can only be an integer
            byte[] inpBytesArray = new byte[(int) fSplit.getLength()];
            
            Path file = fSplit.getPath();
            
            // finding a suitable key
            String k = file.toString();
            String ke = k.substring(k.indexOf("input/")+6);
//            System.out.println("file path : " + ke.substring(0,ke.indexOf(".")));            
            
            int keyTaken = -1;
            try
            {
            	keyTaken = Integer.parseInt(ke.substring(0,ke.indexOf(".")));
            }
            catch(Exception e)
            {
            	// do nothing already -1; // signify some problem
            }
            key.set(keyTaken);
            
            FileSystem fileSytemDF= file.getFileSystem(conf);
            
            FSDataInputStream in = null;
            try
            {
                in = fileSytemDF.open(file);
                IOUtils.readFully(in, inpBytesArray, 0, inpBytesArray.length);
                this.value.set(inpBytesArray, 0, inpBytesArray.length);
                IOUtils.closeStream(in);
            }
            catch(Exception e)
            {
            	System.out.println("Some Error occured here...");
            }

            finished = true;
            return true;
        }
        else
        {
        	return !finished;        	
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException,InterruptedException
    {
        return value;
    }

    // This is for the user's information about the task
    // As we are keeping our key as -1
    // Also the task is binary here, not like line by line.
    @Override
    public float getProgress() throws IOException
    {
        if(finished == true)
        	return 1.0f;
        else
        	return 0.0f;
    }

    // We don't need to do anything as the clean up
    // is taken care of by the .nextKeyValue() function
    // as the variable are local to that function
    // and are close() in final.
    @Override
    public void close() throws IOException{}
}

/*
	To create a custom RecordReader in Hadoop,
	you need to create a new class that extends
	the RecordReader interface and implement 
	the following methods:

	1. initialize(): This method is called once at 
		the beginning of the RecordReader's life cycle.
		It is used to set up the RecordReader and 
		initialize any resources that it needs. This 
		method takes two arguments: the InputSplit 
		that the RecordReader will read and the 
		TaskAttemptContext for the current job.

	2. nextKeyValue(): This method is used to read 
		the next key-value pair from the input split.
		 It returns true if a key-value pair was read
		 successfully and false if there are no more
		 key-value pairs to read. The key and value
		 are stored in class-level variables that
		 can be accessed by other methods.

	3. getCurrentKey(): This method returns the 
		current key that was read by the RecordReader.
		The key is typically used as the input to the
		Map function.

	4. getCurrentValue(): This method returns the 
		current value that was read by the RecordReader.
		The value is typically used as the input to 
		the Map function.

	5. getProgress(): This method returns a float between
		0.0 and 1.0 that indicates the progress of the
		RecordReader in reading the input split.

	6. close(): This method is called once at the end 
		of the RecordReader's life cycle. It is used
		to release any resources that were used by 
		the RecordReader during its operation.

By implementing these methods, you can create a custom 
RecordReader that can read input data in a format that 
is not supported by Hadoop's built-in InputFormat classes.
You can then use your custom RecordReader with a custom
InputFormat class to process your input data in a Hadoop job.
*/