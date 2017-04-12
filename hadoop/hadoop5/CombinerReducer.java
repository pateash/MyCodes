package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombinerReducer extends Reducer<Text,LongWritable,Text,LongWritable> 
{  
  
  @Override
  public void reduce(Text key,Iterable<LongWritable> values,Context context)
  {
     long count=0 ,mycount=0  ;
     while(values.iterator().hasNext())
     {
         mycount=values.iterator().next().get();
         System.out.println(key+"->"+mycount);
         count+=mycount;
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
