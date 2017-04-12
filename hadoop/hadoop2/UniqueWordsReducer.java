package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueWordsReducer extends Reducer<Text,IntWritable,Text,LongWritable> 
{  
  public static long uniquewords=0;
  @Override
  public void reduce(Text key,Iterable<IntWritable> values,Context context)
  {
     uniquewords++;   
     System.out.println("Total wordcount becomes"+uniquewords);
     String str="Total wordcount becomes";
     try
     {
      context.write(new Text(str),new LongWritable(uniquewords));
     }
     catch(IOException | InterruptedException e)
     {
       e.printStackTrace();
     }
  }
}
