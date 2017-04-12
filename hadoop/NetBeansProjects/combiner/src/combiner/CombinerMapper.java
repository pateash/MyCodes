/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package combiner;

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


public class CombinerMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String TempString = value.toString();
		String[] array = TempString.split(" ");
                String key=new String("");
                int i;
                for(i=0;i<array.length;i++)
                {
                    output.collect(new Text (array[i]), one);
                }
                    
                }
                }
                
            