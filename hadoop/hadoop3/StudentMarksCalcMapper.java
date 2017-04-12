package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author abhijeet
 */
public class StudentMarksCalcMapper extends Mapper<LongWritable,Text,Text,IntWritable>
{
  @Override
  public void map(LongWritable key,Text value,Context context)
  {
     String str=value.toString();
     String arr[]=str.split(",");
       try
       {
           System.out.println("here comes ");   
           context.write(new Text(arr[0]),new IntWritable(Integer.parseInt(arr[1])));
           System.out.println("here falied");
       }
       catch(IOException | InterruptedException e)
       {
           System.out.println(e.toString());
       }
  }
}
