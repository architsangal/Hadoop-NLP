import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;


public class DFInputFormat
        extends FileInputFormat<IntWritable, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<IntWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        DFRecordReader reader = new DFRecordReader();
        reader.initialize(split, context);
        return reader;
    }
}