/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vehicletype;

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


public class VehicleTypeMapper extends MapReduceBase implements
		org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);

	
	@Override
	public void map(LongWritable _key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String TempString = value.toString();
		String[] array = TempString.split(",");
                String key=new String("");
                if(array.length>=2){
                if(array[1].length()!=0&&(array[1].equals("-1")!=true))
                {
                    //FOR DAY:
                    if(array[2].length()!=0&&(array[2].equals("-1")!=true))
                    {
                        
                        if(array[2].equals("1")||array[2].equals("2")||array[2].equals("3")||array[2].equals("4")||array[2].equals("5")||array[2].equals("103")||array[2].equals("104")||array[2].equals("105")||array[2].equals("106")||array[2].equals("22")||array[2].equals("23")||array[2].equals("97"))
                        {
                            key+="TwoWheelers, ";
                            
                            
                        }
                        else if (array[2].equals("8")||array[2].equals("10")||array[2].equals("11")||array[2].equals("17")||array[2].equals("20")||array[2].equals("21")||array[2].equals("98")||array[2].equals("110")||array[2].equals("108")||array[2].equals("113"))
                        {
                            key+="FourWheelers-Commercial, ";
                            
                        }
                        else if(array[2].equals("9")||array[2].equals("109")||array[2].equals("19"))
                        {
                            key+="FourWheelers-NonCommercial, ";
                        }
                        else
                        {
                            key+="Others, ";
                        }
                        if(array[1].equals("1"))
                            key+="fatal ";
                        else if(array[1].equals("2"))
                            key+="serious ";
                        else
                            key+="slight ";
                        output.collect(new Text(key), one);    
                    }
                    
                    
                }
                }
                
                
	}
}