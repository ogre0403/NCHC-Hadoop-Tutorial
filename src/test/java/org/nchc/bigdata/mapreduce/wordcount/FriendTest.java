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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.nchc.bigdata.mapreduce.frineds.FriendMapper;
import org.nchc.bigdata.mapreduce.frineds.FriendReducer;
import org.nchc.bigdata.mapreduce.frineds.Relational;
import org.nchc.bigdata.mapreduce.frineds.TextArraryWritable;

public class FriendTest {
	private Mapper<Object, Text, Relational, TextArraryWritable> mapper;
	private MapDriver<Object, Text, Relational, TextArraryWritable> mapdriver;
	private Reducer<Relational,TextArraryWritable,Relational,TextArraryWritable> reducer;
	private ReduceDriver<Relational,TextArraryWritable,Relational,TextArraryWritable> reducerdriver;
	private MapReduceDriver<Object, Text, Relational, TextArraryWritable, Relational,TextArraryWritable> MRdriver;
	
	@Before
	public void init() {
		mapper = new FriendMapper();
		mapdriver = new MapDriver<Object, Text, Relational, TextArraryWritable>(mapper);
		
		reducer = new FriendReducer();
		reducerdriver = new ReduceDriver<Relational,TextArraryWritable,Relational,TextArraryWritable>(reducer);
		
		MRdriver = new MapReduceDriver<Object, Text, Relational, TextArraryWritable, Relational,TextArraryWritable>(mapper, reducer);
	}

	@Test
	public void testType() {
		Relational A = new Relational("a", "b");
		Relational B = new Relational("a", "b");

		
		Assert.assertTrue(A.equals(B));

		Assert.assertTrue(A.compareTo(B) == 0);
		
		Text[] t1 = { new Text("a") };
		Text[] t2 = { new Text("a") };
		TextArraryWritable aw = new TextArraryWritable(t1);
		TextArraryWritable bw = new TextArraryWritable(t2);
		Assert.assertTrue(aw.equals(bw));
	}

	@Ignore
	public void testMapper() throws IOException {
		String line1 = "A	B,C,D";
		String line2 = "B	A,C,D,E";
		
		Text[] r1 = { new Text("B"), new Text("C"), new Text("D") };
		Text[] r2 = { new Text("A"), new Text("C"), new Text("D"), new Text("E") };

		TextArraryWritable res1 = new TextArraryWritable(r1);
		TextArraryWritable res2 = new TextArraryWritable(r2);
		
		mapdriver.withInput(new Text(""), new Text(line1))
				 .withInput(new Text(""), new Text(line2))
				 .withOutput(new Relational("A", "B"), res1)
				 .withOutput(new Relational("A", "C"), res1)
				 .withOutput(new Relational("A", "D"), res1)
				 .withOutput(new Relational("A", "B"), res2)
				 .withOutput(new Relational("B", "C"), res2)
				 .withOutput(new Relational("B", "D"), res2)
				 .withOutput(new Relational("B", "E"), res2)
				 .runTest();
	}

	@Ignore
	public void testReducer() throws IOException {
		ArrayList<TextArraryWritable> values = new ArrayList<TextArraryWritable>();
		ArrayList<TextArraryWritable> values2 = new ArrayList<TextArraryWritable>();
		Text[] v1 = { new Text("A"), new Text("C"), new Text("D"),	new Text("E") };
		Text[] v2 = { new Text("B"), new Text("C"), new Text("D") };
		
		Text[] v3 = { new Text("B"), new Text("C"), new Text("D") };
		
		Text[] res = { new Text("C"), new Text("D") };	
		Text[] res2 = { new Text("B"), new Text("C"), new Text("D") };	
		TextArraryWritable result = new TextArraryWritable(res);
		TextArraryWritable result2 = new TextArraryWritable(res2);
		values.add(new TextArraryWritable(v1));
		values.add(new TextArraryWritable(v2));
		
		values2.add(new TextArraryWritable(v3));
		reducerdriver.withInput(new Relational("A", "B"), values)
					 .withInput(new Relational("A", "C"), values2)
					 .withOutput(new Relational("A", "B"), result)
					 .withOutput(new Relational("A", "C"), result2)
					 .runTest();
	}

	@Ignore
	public void testMR() throws IOException{
		
		String line1 = "A	B,C,D";
		String line2 = "B	A,C,D,E";
		String line3 = "C	A,B,D,E";
		Text[] res1 = { new Text("C"), new Text("D") };
		Text[] res2 = { new Text("B"), new Text("D") };
		Text[] res3 = { new Text("A"), new Text("D"), new Text("E") };
		
		TextArraryWritable result1 = new TextArraryWritable(res1);
		TextArraryWritable result2 = new TextArraryWritable(res2);
		TextArraryWritable result3 = new TextArraryWritable(res3);
		
		 MRdriver
		    .withInput(new Text(""),new Text(line1))
		    .withInput(new Text(""),new Text(line2))
		    .withInput(new Text(""),new Text(line3))
			.withOutput(new Relational("A", "B"),result1)
			.withOutput(new Relational("A", "C"),result2)
			.withOutput(new Relational("B", "C"),result3)
			.runTest();
	}
}
