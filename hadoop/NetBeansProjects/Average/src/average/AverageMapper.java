/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package average;

/**
 *
 * @author jagrat
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class AverageMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {
	

	
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String TempString = value.toString();
		String[] array = TempString.split(",");
                String key=new String("");
                key=array[0];
                output.collect(new Text(key), new Text (array[1])); 
               }
}
                
           