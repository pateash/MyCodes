package com.orzota.bookx;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class BookXDriver {
	public static void main(String[] args) {
		JobClient client = new JobClient();
		// Configurations for Job set in this variable
		JobConf conf = new JobConf(com.orzota.bookx.BookXDriver.class);
		
		// Name of the Job
		conf.setJobName("BookCrossing1.0");
		
		// Data type of Output Key and Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		// Setting the Mapper and Reducer Class
		conf.setMapperClass(com.orzota.bookx.BookXMapper.class);
		conf.setReducerClass(com.orzota.bookx.BookXReducer.class);

		// Formats of the Data Type of Input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		// Specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path(args[2]));
		
		client.setConf(conf);
		try {
			// Running the job with Configurations set in the conf.
			JobClient.runJob(conf);
		} catch (IOException e) {
		}
	}
}