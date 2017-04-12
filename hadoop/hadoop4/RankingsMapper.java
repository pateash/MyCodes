package MyPkg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankingsMapper extends Mapper<LongWritable,Text,Text,Text>
{
 @Override
 public void map(LongWritable key,Text values ,Context context)
 {
   String str=values.toString();
   String arr[]=str.split(",");
   try
   {
     context.write(new Text(arr[1]),new Text(arr[0]+" "+arr[2]));
   }
   catch(IOException |InterruptedException e)
   {
     e.printStackTrace();
   }
 }
}
