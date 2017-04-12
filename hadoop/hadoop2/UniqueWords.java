package MyPkg;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueWords extends Configured
{
    public static void main(String args[]) throws IOException, InterruptedException
    {
      Job job=new Job();
      job.setJarByClass(UniqueWords.class);
      
      FileInputFormat.setInputPaths(job,new Path(args[0]));
      FileOutputFormat.setOutputPath(job,new Path(args[1]));
      
      job.setMapperClass(UniqueWordsMapper.class);
      job.setReducerClass(UniqueWordsReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      
      job.setOutputKeyClass(DoubleWritable.class);
      job.setOutputValueClass(Text.class);
      
        try 
        {
            System.exit(job.waitForCompletion(true)? 0:-1);
        } 
        catch (ClassNotFoundException ex)
        {
            Logger.getLogger(UniqueWords.class.getName()).log(Level.SEVERE, null, ex);
        }
      
    }
     
}