package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordFrequencyReducer extends Reducer<Text,IntWritable,Text,LongWritable> 
{  
  
  @Override
  public void reduce(Text key,Iterable<IntWritable> values,Context context)
  {
     long count=0  ;
     while(values.iterator().hasNext())
     {
       count+=values.iterator().next().get();
     }
     try
     {
     context.write(key,new LongWritable(count));
     }
     catch(IOException | InterruptedException e)
     {
       e.printStackTrace();
     }
  }
}
