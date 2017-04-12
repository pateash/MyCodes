package MyPkg;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
public class Rankings extends Configured
{
 public static void main(String args[]) throws IOException, InterruptedException
 {
   Job job=new Job();
   job.setJarByClass(Rankings.class);
    
   FileInputFormat.setInputPaths(job,new Path(args[0]));
   FileOutputFormat.setOutputPath(job,new Path(args[1]));
   
   job.setMapperClass(RankingsMapper.class);
   job.setReducerClass(RankingsReducer.class);
   job.setPartitionerClass(RankingsPartitioner.class);
   
   job.setNumReduceTasks(4);
   
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(Text.class);
   
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(DoubleWritable.class);
   
   try
   {
    System.exit(job.waitForCompletion(true)? 0:-1);
   }
   catch(ClassNotFoundException e)
   {
     e.printStackTrace();
   }
 }
}
