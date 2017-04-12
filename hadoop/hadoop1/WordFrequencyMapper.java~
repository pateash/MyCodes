package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordFrequencyMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
  @Override
  public void map(LongWritable key,Text value,Context context)
  {
     String str=value.toString();
     String arr[]=str.split(" ");
     for(String i:arr)
     {
       try
       {
        context.write(new Text(i),new IntWritable(1));
       }
       catch(IOException | InterruptedException e)
       {
           System.out.println(e.toString());
       }
     }
  }
}
