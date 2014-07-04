package org.nchc.bigdata.mapreduce.frineds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindFriends {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        
	        if (otherArgs.length != 2) {
	                System.err.println("Usage: org.nchc.bigdata.mapreduce.frineds.FindFriends");
	                System.exit(2);
	        }
		
		
		Job job = Job.getInstance(conf, "FindFrineds");
		job.setJarByClass(FindFriends.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);
        
        job.setMapOutputKeyClass(Relational.class);
        job.setMapOutputValueClass(TextArraryWritable.class);
        
        job.setOutputKeyClass(Relational.class);
        job.setOutputValueClass(TextArraryWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
