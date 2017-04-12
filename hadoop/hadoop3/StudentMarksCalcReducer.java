package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentMarksCalcReducer extends Reducer<Text,IntWritable,Text,DoubleWritable> 
{  
  @Override
  public void reduce(Text key,Iterable<IntWritable> values,Context context)
  {   
    int totalsum=0,sub=0;
      System.out.println("yet to come");
    while(values.iterator().hasNext())
    {
      totalsum+=values.iterator().next().get();
      sub++;
    }
    try
    {
      context.write(new Text(key.toString()+" has average marks = "),new DoubleWritable(totalsum*(1.0)/sub));
    }
    catch(IOException |InterruptedException e)
    {
      e.printStackTrace();
    }
  }
}
