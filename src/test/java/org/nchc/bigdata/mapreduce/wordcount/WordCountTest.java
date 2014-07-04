package org.nchc.bigdata.mapreduce.wordcount;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


public class WordCountTest {
	private Mapper<Object, Text, Text, IntWritable> mapper;
	private MapDriver<Object, Text, Text, IntWritable> mapdriver;
	private Reducer<Text,IntWritable,Text,IntWritable> reducer;
	private ReduceDriver<Text,IntWritable,Text,IntWritable> reducerdriver;
	private MapReduceDriver<Object, Text, Text, IntWritable, Text,IntWritable> MRdriver;
	
	 @Before
	 public void init(){
	    mapper = new WordCountMapper(); 
	    mapdriver = new MapDriver<Object, Text, Text, IntWritable>(mapper);
	    reducer = new WordCountReducer();
	    reducerdriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);
	    MRdriver = new MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable>(mapper, reducer);
	 }

	   @Test
	   public void testMapper() throws IOException{	
		   String line = "Taobao Taobao is a great website";
		   mapdriver
		   	.withInput(new Text(""),new Text(line))
	     	.withOutput(new Text("Taobao"),new IntWritable(1))
	     	.withOutput(new Text("Taobao"),new IntWritable(1))
	     	.withOutput(new Text("is"), new IntWritable(1))
	     	.withOutput(new Text("a"), new IntWritable(1))
	     	.withOutput(new Text("great"), new IntWritable(1))
	     	.withOutput(new Text("website"), new IntWritable(1))
	     	.runTest();
		}
	   
	   @Test
	   public void testReducer() throws IOException{
		   String key = "taobao";
		   ArrayList<IntWritable> values = new ArrayList<IntWritable>();
		   values.add(new IntWritable(2));
		   values.add(new IntWritable(3));
		   reducerdriver
		   	.withInput(new Text(key), values)
		   	.withOutput(new Text(key), new IntWritable(5))
		   	.runTest();
	   }
	   
	   @Test
	   public void testMR() throws IOException{
		   String line = "Taobao is a great website is it not";
		   MRdriver
		    .withInput(new Text(""),new Text(line))
		    .withInput(new Text(""),new Text(line))
			.withOutput(new Text("Taobao"),new IntWritable(2))
			.withOutput(new Text("a"),new IntWritable(2))
			.withOutput(new Text("great"),new IntWritable(2))
			.withOutput(new Text("is"),new IntWritable(4))
			.withOutput(new Text("it"),new IntWritable(2))
			.withOutput(new Text("not"),new IntWritable(2))
			.withOutput(new Text("website"),new IntWritable(2))
			.runTest();
	   }
}
