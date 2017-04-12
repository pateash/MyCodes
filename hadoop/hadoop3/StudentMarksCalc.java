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

public class StudentMarksCalc extends Configured
{
    public static void main(String args[]) throws IOException, InterruptedException
    {
      Job job=new Job();
      job.setJarByClass(StudentMarksCalc.class);
      
      FileInputFormat.setInputPaths(job,new Path(args[0]));
      FileOutputFormat.setOutputPath(job,new Path(args[1]));
      
      job.setMapperClass(StudentMarksCalcMapper.class);
      job.setReducerClass(StudentMarksCalcReducer.class);
      
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      
        try 
        {
            System.exit(job.waitForCompletion(true)? 0:-1);
        } 
        catch (ClassNotFoundException ex)
        {
            Logger.getLogger(StudentMarksCalc.class.getName()).log(Level.SEVERE, null, ex);
        }
      
    }
     
}