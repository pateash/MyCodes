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
import java.util.Iterator;
import java.lang.String;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AverageReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text> {

    
	@Override
	public void reduce(Text _key,
			Iterator<Text> values,
			OutputCollector<Text,Text> output, 
			Reporter reporter)
			throws IOException {
		Text key = _key;
                
                if(key.toString().equals("0Student_Id")!=true)
                {
                    int frequencyForYear = 0;
                int f=0;
                double av=0, c=0.0;
                String ans="", t="";
                
		while (values.hasNext()) {
			
			Text value = (Text) values.next();
                        t=value.toString();
                       
			frequencyForYear += Integer.parseInt(t);
                        c++;
			// process value
		}
         
                av=frequencyForYear/c;
                ans = Double.toString(av);
		output.collect(key, new Text (ans));
                }
                else
                {
                    output.collect(key, new Text ("Average"));
                }
		
         
	}

   
}
