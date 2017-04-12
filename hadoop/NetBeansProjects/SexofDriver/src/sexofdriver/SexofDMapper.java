/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sexofdriver;

/**
 *
 * @author jagrat
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class SexofDMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String TempString = value.toString();
		String[] array = TempString.split(",");
                String key=new String("");
                
                if(array[1].length()!=0&&(array[1].equals("-1")!=true)&&(array[1].equals("Accident_Severity")!=true))
                {
                    //FOR DAY:
                    if(array[5].length()!=0&&(array[5].equals("-1")!=true))
                    {
                        
                        if(array[5].equals("1"))
                        {
                            key+="Male, ";
                            
                            
                        }
                        else if (array[5].equals("2"))
                        {
                            key+="Female, ";
                            
                        }
                        else
                        {
                            key+="Not Known, ";
                        }
                        if(array[1].equals("1"))
                            key+="fatal";
                        else if(array[1].equals("2"))
                            key+="serious";
                        else
                            key+="slight";
                        output.collect(new Text(key), one);    
                    }
                    
                    
                }
                
                
	}
}