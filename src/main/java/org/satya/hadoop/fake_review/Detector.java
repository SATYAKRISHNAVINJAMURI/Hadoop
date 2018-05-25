package org.satya.hadoop.fake_review;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Detector { 
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job job=new Job();
		job.setJarByClass(Detector.class);
		
		//conf.set("mapred.job.tracker", "myserver.com:8021");
		//conf.set("fs.default.name", "hdfs://myserver.com:8020");
				
		

		job.setMapperClass(Detector_Mapper.class);
		job.setReducerClass(Detector_Reducer.class);
		
		job.setMapOutputKeyClass(Key.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		job.setInputFormatClass(Review_InputFormat.class);  // take input from the file in text format
		job.setOutputFormatClass(TextOutputFormat.class);
				
		FileInputFormat.addInputPath(job,new Path( args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		job.waitForCompletion(true);
		
		
	}
}  	