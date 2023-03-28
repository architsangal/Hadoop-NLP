import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainTF extends Configured implements Tool
{


    public int run(String[] args) throws Exception
    {
        TF.run(args); // run TF function
        TFIDF.run(args); // run TFIDF function
        return 0;
    }
   
    public static void main(String[] args) throws Exception
	{
    	ToolRunner.run(new MainTF(), args);
	}
}