package MyPkg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankingsReducer extends Reducer<Text,Text,Text,DoubleWritable>
{
  @Override
  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException
  {
    List l= new ArrayList<MyClass>();
    MyClass obj;
    while(values.iterator().hasNext())
    {
     String str[]=values.iterator().next().toString().split(" ");
     obj=new MyClass();
     obj.name=str[0];
     obj.pointers=Double.parseDouble(str[1]);
     l.add(obj);
    }
    Collections.sort(l);
    Iterator itr=l.iterator();
    while(itr.hasNext())
    {
      MyClass o=(MyClass)(itr.next());
      context.write(new Text(o.name),new DoubleWritable(o.pointers));
    }
  }
}
class MyClass implements Comparable<MyClass>
{
   public String name;
   public double pointers;
   public int compareTo(MyClass t) 
    {
      if((this.pointers>t.pointers))
      {
        return 1;
      }
      else if(this.pointers==t.pointers)
      {
        return 0;
      }
      else
      {
        return -1;
      }
    }
}