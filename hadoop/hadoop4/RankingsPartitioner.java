package MyPkg;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RankingsPartitioner extends Partitioner
{

    @Override
    public int getPartition(Object key, Object value, int numPartitions) 
    {
      String k=key.toString();
      if(k.equals("CSE"))
      {
        return 0;
      }
      else if(k.equals("ECE"))
      {
        return 1;
      }
      else if(k.equals("MECH"))
      {
        return 2;
      }
      else
      {
        return 3;
      } 
    }
}
